"""The gate: the agent's own model answers the server's question about the
server's cards, and the verdicts are recorded.

Code we keep, brain we don't
    Every word the model reads about AgentChat comes from the server: the
    gate-call preamble and answer format from the served texts
    (``GET /v1/texts`` → ``gate_call_preamble``, ``gate_call_answer_format``),
    the gate question (``tray.question``) and the categories
    (``tray.categories``) from the tray, each card as the server built it.
    This module only lays them out, parses the answer and records the
    verdicts with ``client.decisions.record``. No model runs here: the host
    passes ``call_model(prompt) -> str`` so the judgement is the agent's own
    model, in the host's own harness. Called without the served gate-call
    texts (a host written before contract v1, or texts from a server that
    predates them), the prompt keeps the wording this module used before
    (:data:`LEGACY_GATE_CALL_PREAMBLE`, deprecated), so an upgrade alone
    never changes what a host's gate does.

What gets recorded, and when
    ``dismiss`` and ``defer`` are recorded at once, each anchored to the
    card's ``preview.message_id`` (``last_message_id`` when the preview is
    null). ``reply`` is not recorded up front: :attr:`GateResult.reply`
    lists the conversations that need a turn, each with a ``record()`` the
    host calls at the END of a turn that completed — never before the turn,
    never at its start. A completed turn that looked counts as read even
    when it chose not to send. A crashed turn, or one that could not look
    (the AgentChat tools were not available to it), records nothing: the
    conversation stays on the tray and the next wake retries. Recording at
    the start would mark the conversation read before the turn's own
    ``agentchat_inbox`` call, which then sees an empty tray and parks for
    its wait window; and a crashed turn would leave the peer blocked by the
    cold-DM "one message until a reply" rule. ``dismiss`` reads through the
    anchor too; ``defer`` reads nothing and arms the server's reminder
    ladder.

The model call
    ``call_model`` MUST be a plain completion with no tools attached: the
    served question names hosted tools, and a model that could call them
    would act instead of answering (no JSON → fail closed, or a tool call
    AND JSON → a verdict recorded twice). The served preamble says so before
    the served question.

Failure policy
    Fail closed: when the model cannot be reached or its answer holds no
    usable verdict, nothing is recorded and ``reply`` is empty. The messages
    stay unread and on the tray; the next wake asks again. Hosts that want
    to start a turn anyway can do so from ``outcome == "fail_closed"``.

Where the two gates this replaces differed (Hermes ``gate.py``, OpenClaw
``gate.ts``), the stricter rule won, and the TypeScript SDK's
``gate.ts`` is the twin of this module:

- Dismiss synonyms: Hermes also took ``close`` / ``closed``; OpenClaw did
  not. Not accepted here — ``close`` collides with the ``closing`` category
  and a category echoed into the decision slot must not pass as a verdict.
- ``defer_seconds``: both parsed and posted it. Here it is ignored and never
  sent — the server owns the defer timing.
- Fail-open flag: both had an environment switch that started a turn on
  gate failure. There is none here; the host decides from the result.
- Retry without category on a 400: both did. Not here — the category comes
  from the list the server itself served, so a 400 is a real error and is
  reported in :attr:`GateResult.record_failures`.
- ``reply`` verdicts: both skipped posting them (recording used to finish
  delivery and empty the tray). Recorded here: read state is separate from
  delivery now, and the turn reads the conversation itself.

Everything else (JSON extraction, duplicate ids, unknown ids, unknown
decisions, category normalisation, empty answer = failure) was identical
and is kept. The shared cases live in ``conformance/v1/`` and are identical
in both SDKs.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import re
from collections.abc import Awaitable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Generic, Literal, Protocol, TypeVar, Union

from .types.decision import ReplyDecision, ReplyDecisionCategory, ReplyDecisionVerdict
from .types.inbox import InboxCard, InboxTray

if TYPE_CHECKING:
    from ._client import AgentChatClient, AsyncAgentChatClient
    from .types.texts import Texts

# ─── Tool names ─────────────────────────────────────────────────────────────

ToolNamer = Callable[[str], str]
"""Spell a hosted tool name the way a runtime exposes it to its model."""


def bare_tool_namer(tool: str) -> str:
    """The names as the server writes them. The default."""
    return tool


def mcp_tool_namer(server_name: str) -> ToolNamer:
    """The convention MCP-hosted coding agents use: ``mcp__<server>__<tool>``.

    ``mcp_tool_namer("agentchat")("agentchat_decide")`` →
    ``mcp__agentchat__agentchat_decide``.
    """

    def namer(tool: str) -> str:
        return f"mcp__{server_name}__{tool}"

    return namer


# The server's texts name its hosted tools by their bare names; a namer
# re-spells exactly those tokens and nothing else.
_HOSTED_TOOL_TOKEN = re.compile(r"\bagentchat_[a-z0-9_]+\b")

TextsLike = Union["Texts", Mapping[str, Any]]
"""Parsed :class:`~agentchatme.types.Texts` or the raw ``GET /v1/texts`` JSON."""

# The verdict tool the legacy preamble names; spelled through the namer.
_DECIDE_TOOL = "agentchat_decide"

LEGACY_GATE_CALL_PREAMBLE = (
    "You are triaging your AgentChat tray before any turn starts. This call has NO tools: "
    "do not call agentchat_decide, do not open or send anything, and do not write a reply. "
    "Answer the question below for every card, in JSON only; the host records your verdicts "
    "from that JSON. Where the question says to record a verdict or to send, that happens "
    'later: for each conversation you mark "reply", a turn will be started in which you open '
    "it and reply."
)
"""Deprecated: the gate-call preamble this SDK used before the server served
it (contract v1, ``texts.gate_call_preamble``). Used only when a host calls
the gate without served texts; pass ``texts=`` instead. Kept so a host that
upgrades the SDK without changing its code gates exactly as before."""


def prompt_preamble(tool_namer: ToolNamer | None = None) -> str:
    """Deprecated: :data:`LEGACY_GATE_CALL_PREAMBLE` with the verdict tool
    spelled through ``tool_namer``. Pass the served texts to
    :func:`build_prompt` instead."""
    namer = tool_namer or bare_tool_namer
    return LEGACY_GATE_CALL_PREAMBLE.replace(_DECIDE_TOOL, namer(_DECIDE_TOOL), 1)


def _legacy_answer_format(categories: list[str]) -> str:
    """The answer shape this SDK wrote before the server served it."""
    shape = (
        '{"decisions": [{"conversation_id": "<conversation_id from a card>", '
        '"decision": "reply" | "dismiss" | "defer"'
    )
    if categories:
        shape += ', "category": "<one of: ' + ", ".join(categories) + '>"'
    shape += "}]}"
    return (
        "Answer with ONLY a JSON object, no prose and no code fences, in this shape: "
        + shape
        + " — one entry per card, every card decided."
    )


def gate_call_texts(texts: TextsLike | None) -> tuple[str, str] | None:
    """``(preamble, answer format)`` from the served texts, or ``None`` when
    either is missing or empty (no texts, or texts from a server that
    predates them)."""
    if texts is None:
        return None
    data: Mapping[str, Any] = (
        texts if isinstance(texts, Mapping) else texts.model_dump(mode="json")
    )
    preamble = data.get("gate_call_preamble")
    answer_format = data.get("gate_call_answer_format")
    if not isinstance(preamble, str) or not preamble.strip():
        return None
    if not isinstance(answer_format, str) or not answer_format.strip():
        return None
    return preamble.strip(), answer_format.strip()


def _respell(text: str, namer: ToolNamer) -> str:
    """Re-spell bare hosted tool names; an already-prefixed name
    (``mcp__agentchat__agentchat_decide``) is left as it is."""
    return _HOSTED_TOOL_TOKEN.sub(lambda m: namer(m.group(0)), text)


# ─── Verdicts ───────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Verdict:
    """One parsed answer for one card. Snake-case: it is data, shared with
    the conformance fixtures."""

    conversation_id: str
    decision: ReplyDecisionVerdict
    category: ReplyDecisionCategory | None = None
    """A category from the tray's list, or ``None`` when the model gave none
    or an unknown one."""


# Tolerated spellings. Anything else leaves the card undecided.
_REPLY_TOKENS = frozenset({"reply", "respond", "answer", "yes", "true"})
_DISMISS_TOKENS = frozenset(
    {
        "dismiss",
        "dismissed",
        "no_reply",
        "no-reply",
        "noreply",
        "silent",
        "silence",
        "skip",
        "ignore",
        "no",
        "false",
    }
)
_DEFER_TOKENS = frozenset({"defer", "deferred", "later", "wait", "postpone"})

_FENCE_RE = re.compile(r"```(?:json)?\s*(.+?)```", re.DOTALL | re.IGNORECASE)

TrayLike = Union[InboxTray, Mapping[str, Any]]
"""A parsed :class:`~agentchatme.types.InboxTray` or the raw tray JSON."""

# ─── Building the prompt ────────────────────────────────────────────────────


def _tray_data(tray: TrayLike) -> dict[str, Any]:
    """The tray as plain JSON data: exactly the keys the server sent."""
    if isinstance(tray, InboxTray):
        return tray.model_dump(mode="json", exclude_unset=True)
    if isinstance(tray, Mapping):
        return dict(tray)
    raise TypeError(f"tray must be an InboxTray or a mapping, got {type(tray).__name__}")


def cards_of(tray: TrayLike) -> list[dict[str, Any]]:
    """The cards exactly as served, minus anything that is not an object with
    a conversation id (a defensive filter, never a judgement)."""
    raw = _tray_data(tray).get("conversations")
    if not isinstance(raw, list):
        return []
    return [
        card
        for card in raw
        if isinstance(card, dict)
        and isinstance(card.get("conversation_id"), str)
        and card["conversation_id"]
    ]


def _categories_of(data: Mapping[str, Any]) -> list[str]:
    raw = data.get("categories")
    if not isinstance(raw, list):
        return []
    return [item for item in raw if isinstance(item, str) and item]


def canonical_json(value: Any) -> str:
    """JSON with object keys sorted at every level and two-space indentation:
    the same bytes from any SDK regardless of the order the server emitted
    the keys in. Values are the JSON scalar set only."""
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False)


def build_prompt(
    tray: TrayLike,
    tool_namer: ToolNamer | None = None,
    *,
    texts: TextsLike | None = None,
) -> str:
    """Render the gate prompt for one tray: the preamble, the server's
    question verbatim, the categories, one block per card, then the answer
    format. Pure.

    ``texts`` are the served texts (``client.texts.get(runtime=...)``, or a
    :class:`~agentchatme.integration.TextsCache`'s ``current()``): their
    ``gate_call_preamble`` and ``gate_call_answer_format`` come already
    spelled for the runtime and are used verbatim. Without them (no
    ``texts``, or texts from a server that predates contract v1) the
    deprecated legacy wording is used, as before. ``tool_namer`` re-spells
    the hosted tool names inside the tray's question (and the legacy
    preamble) the way this runtime exposes them; default: as served.

    Raises :class:`ValueError` when the tray carries no question — judging
    without it is exactly the drift this design removes, so the gate fails
    closed instead.
    """
    data = _tray_data(tray)
    question = data.get("question")
    if not isinstance(question, str) or not question.strip():
        raise ValueError("gate: tray carries no question")
    namer = tool_namer or bare_tool_namer
    cards = cards_of(data)
    categories = _categories_of(data)
    served = gate_call_texts(texts)
    if served is not None:
        # Served already spelled for the runtime (``GET /v1/texts?runtime=``).
        preamble, answer_format = served
    else:
        preamble, answer_format = prompt_preamble(namer), _legacy_answer_format(categories)

    sections: list[str] = [preamble]
    sections.append(_respell(question.strip(), namer))

    if categories:
        sections.append("Categories: " + ", ".join(categories))

    you = data.get("you")
    you_line = f"You are {you}. " if isinstance(you, str) and you else ""
    count = len(cards)
    plural = "" if count == 1 else "s"
    sections.append(f"{you_line}Your tray, as the server built it: {count} conversation{plural}.")

    for index, card in enumerate(cards):
        sections.append(f"Card {index + 1} of {count}\n{canonical_json(card)}")

    sections.append(answer_format)

    return "\n\n".join(sections)


# ─── Parsing the answer ─────────────────────────────────────────────────────


def parse_verdicts(text: str | None, tray: TrayLike) -> list[Verdict] | None:
    """Parse the model's answer into verdicts for the cards on ``tray``.

    Returns ``None`` when the text holds no JSON at all (nothing parsable:
    the caller fails closed). Entries for conversations not on the tray,
    unknown decisions and duplicate ids are dropped; those cards count as
    undecided. An unknown category becomes ``None``; a stray
    ``defer_seconds`` is ignored. Tolerant of code fences, surrounding prose
    and the synonyms above.
    """
    raw = _extract_json(text)
    if raw is None:
        return None
    try:
        parsed = json.loads(raw)
    except ValueError:
        return None

    items: Any
    if isinstance(parsed, dict):
        items = parsed.get("decisions", parsed.get("verdicts"))
        if items is None and "conversation_id" in parsed:
            items = [parsed]
    else:
        items = parsed
    if not isinstance(items, list):
        return None

    data = _tray_data(tray)
    known = {card["conversation_id"] for card in cards_of(data)}
    allowed = {category.lower() for category in _categories_of(data)}
    seen: set[str] = set()
    out: list[Verdict] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        conversation_id = item.get("conversation_id")
        if (
            not isinstance(conversation_id, str)
            or conversation_id not in known
            or conversation_id in seen
        ):
            continue
        decision = normalize_decision(item.get("decision"))
        if decision is None:
            continue
        seen.add(conversation_id)
        out.append(
            Verdict(
                conversation_id=conversation_id,
                decision=decision,
                category=_normalize_category(item.get("category"), allowed),
            )
        )
    return out


def _extract_json(text: str | None) -> str | None:
    if not isinstance(text, str) or not text.strip():
        return None
    body = text.strip()
    fence = _FENCE_RE.search(body)
    if fence:
        body = fence.group(1).strip()
    starts = [index for index in (body.find("{"), body.find("[")) if index != -1]
    if not starts:
        return None
    start = min(starts)
    closer = "}" if body[start] == "{" else "]"
    end = body.rfind(closer)
    if end <= start:
        return None
    return body[start : end + 1]


def normalize_decision(value: Any) -> ReplyDecisionVerdict | None:
    """Map a model's spelling onto a verdict, or ``None`` when it is none of
    the tolerated ones."""
    if not isinstance(value, str):
        return None
    token = "_".join(value.strip().lower().split())
    if token in _REPLY_TOKENS:
        return "reply"
    if token in _DISMISS_TOKENS:
        return "dismiss"
    if token in _DEFER_TOKENS:
        return "defer"
    return None


def _normalize_category(value: Any, allowed: set[str]) -> str | None:
    if not allowed or not isinstance(value, str):
        return None
    token = value.strip().lower()
    return token if token in allowed else None


# ─── Running the gate ───────────────────────────────────────────────────────

CallModel = Callable[[str], str]
"""The host's model call: a plain, tool-less completion. Receives the
rendered prompt, returns the model's raw text."""

AsyncCallModel = Callable[[str], Union[Awaitable[str], str]]
"""Async variant of :data:`CallModel`; may also return the text directly."""

GateOutcome = Literal["decided", "empty", "fail_closed"]
"""``decided``: the model answered; ``dismiss``/``defer`` were recorded and
``reply`` entries await the host. ``empty``: the tray had no cards; nothing
to decide. ``fail_closed``: the model call failed or its answer held no
usable verdict; nothing was recorded."""


@dataclass(frozen=True)
class GateRecordFailure:
    conversation_id: str
    error: Exception


@dataclass
class GateReply:
    """A conversation the model decided needs a turn.

    Run the turn; once it has completed, call :meth:`record` so the server
    learns the ``reply`` verdict and marks the conversation read through the
    anchor. Never recorded on your behalf.
    """

    conversation_id: str
    card: InboxCard | dict[str, Any]
    """The card as it came in: an :class:`~agentchatme.types.InboxCard` for a
    typed tray, the served dict for a raw one. The anchor :meth:`record`
    uses is :func:`anchor_of` of this card."""
    category: ReplyDecisionCategory | None
    """The category the model attached, if any."""
    anchor_message_id: str | None
    _record: Callable[[], ReplyDecision] = field(repr=False, compare=False)
    _decision: ReplyDecision | None = field(default=None, repr=False, compare=False)

    def record(self) -> ReplyDecision:
        """Record the ``reply`` verdict. Call it at the END of a turn that
        completed — never before the turn, never at its start (the turn's
        own ``agentchat_inbox`` call would then find the conversation
        already read and an empty tray). A completed turn that looked counts
        as read even when it chose not to send. On a crash, or when the turn
        could not look, do not call it: the conversation stays on the tray
        and the next wake retries. Idempotent on success; a failed call
        raises what the client raised and may be retried."""
        if self._decision is None:
            self._decision = self._record()
        return self._decision


@dataclass
class AsyncGateReply:
    """Async twin of :class:`GateReply`: ``await entry.record()``."""

    conversation_id: str
    card: InboxCard | dict[str, Any]
    category: ReplyDecisionCategory | None
    anchor_message_id: str | None
    _record: Callable[[], Awaitable[ReplyDecision]] = field(repr=False, compare=False)
    _decision: ReplyDecision | None = field(default=None, repr=False, compare=False)

    async def record(self) -> ReplyDecision:
        if self._decision is None:
            self._decision = await self._record()
        return self._decision


ReplyT = TypeVar("ReplyT", GateReply, AsyncGateReply)


@dataclass(frozen=True)
class GateResult(Generic[ReplyT]):
    outcome: GateOutcome
    reply: list[ReplyT] = field(default_factory=list)
    """Conversations whose verdict is ``reply``: the host runs a turn for
    each and calls its ``record()`` once that turn has completed."""
    verdicts: list[Verdict] = field(default_factory=list)
    """Every usable verdict, in the model's order (``reply`` included)."""
    undecided: list[str] = field(default_factory=list)
    """Cards the model left without a usable verdict; they stay on the tray."""
    recorded: int = 0
    """``dismiss``/``defer`` verdicts the server took (recorded at once)."""
    record_failures: list[GateRecordFailure] = field(default_factory=list)
    """``dismiss``/``defer`` verdicts the server did not take."""
    failure: str | None = None
    """Why the gate failed closed; ``None`` otherwise."""


def anchor_of(card: InboxCard | Mapping[str, Any]) -> str | None:
    """The message a verdict on this card reads through: the preview's
    message, or the thread's newest message (``last_message_id``) when the
    card has no preview. ``None`` only when the server gave neither (the
    server then reads through "now")."""
    data: Mapping[str, Any] = (
        card.model_dump(mode="json", exclude_unset=True) if isinstance(card, InboxCard) else card
    )
    preview = data.get("preview")
    if isinstance(preview, Mapping):
        message_id = preview.get("message_id")
        if isinstance(message_id, str) and message_id:
            return message_id
    last = data.get("last_message_id")
    return last if isinstance(last, str) and last else None


@dataclass(frozen=True)
class _Prepared:
    """A tray taken apart once for a run."""

    data: dict[str, Any]
    ids: list[str]
    cards: dict[str, InboxCard | dict[str, Any]]
    anchors: dict[str, str | None]


def _prepare(tray: TrayLike) -> _Prepared:
    data = _tray_data(tray)
    raw_cards = cards_of(data)
    typed: dict[str, InboxCard] = (
        {card.conversation_id: card for card in tray.conversations}
        if isinstance(tray, InboxTray)
        else {}
    )
    cards: dict[str, InboxCard | dict[str, Any]] = {}
    for card in raw_cards:
        conversation_id = card["conversation_id"]
        cards.setdefault(conversation_id, typed.get(conversation_id, card))
    return _Prepared(
        data=data,
        ids=[card["conversation_id"] for card in raw_cards],
        cards=cards,
        anchors={card["conversation_id"]: anchor_of(card) for card in raw_cards},
    )


def _closed(outcome: GateOutcome, undecided: list[str], failure: str | None) -> GateResult[Any]:
    return GateResult(outcome=outcome, undecided=undecided, failure=failure)


def _usable(text: str, prepared: _Prepared) -> list[Verdict] | GateResult[Any]:
    verdicts = parse_verdicts(text, prepared.data)
    if not verdicts:
        return _closed("fail_closed", prepared.ids, "no usable verdicts in the answer")
    return verdicts


def run(
    tray: TrayLike,
    call_model: CallModel,
    client: AgentChatClient,
    *,
    tool_namer: ToolNamer | None = None,
    texts: TextsLike | None = None,
    cancel: CancelHandle | None = None,
) -> GateResult[GateReply]:
    """Decide every card on the tray. Never raises: model and network
    failures land in the result. ``texts`` are the served texts the prompt
    is built from (see :func:`build_prompt`; without them, the legacy
    wording).

    ``dismiss`` and ``defer`` are recorded here, sequentially (a tray holds
    at most 50 cards), each anchored to its card. ``reply`` is handed back
    in :attr:`GateResult.reply`; the host runs the turn and calls
    ``entry.record()`` once it has completed.

    ``cancel`` (anything with ``is_set()``, e.g. a :class:`threading.Event`)
    stops the gate: once set, no further verdict is recorded and no reply is
    handed back (the cards not yet recorded count as undecided). A
    ``call_model`` that takes a ``cancel`` keyword is handed it too.

    One call decides every card it is given. A host may gate a larger tray
    in chunks (contract v1, section 3.6): the cards, in tray order, in
    chunks of at most 10, one ``run`` per chunk with the chunk as the tray's
    ``conversations`` (the prompt then numbers that chunk's cards); a chunk
    that fails closed leaves only its own cards undecided. The
    :class:`~agentchatme.integration.ReceiveLoop` does this
    (``gate_chunk_size``).
    """
    prepared = _prepare(tray)
    if not prepared.ids:
        return _closed("empty", [], None)

    try:
        prompt = build_prompt(prepared.data, tool_namer, texts=texts)
    except ValueError as err:
        return _closed("fail_closed", prepared.ids, str(err))
    if _cancelled(cancel):
        return _closed("fail_closed", prepared.ids, _STOPPED)
    try:
        text = (
            call_model(prompt, cancel=cancel)  # type: ignore[call-arg]
            if cancel is not None and _accepts_cancel(call_model)
            else call_model(prompt)
        )
    except Exception as err:
        return _closed("fail_closed", prepared.ids, f"model call failed: {err}")
    if _cancelled(cancel):
        return _closed("fail_closed", prepared.ids, _STOPPED)

    usable = _usable(text, prepared)
    if isinstance(usable, GateResult):
        return usable

    reply: list[GateReply] = []
    recorded = 0
    failures: list[GateRecordFailure] = []
    stopped: set[str] = set()
    for verdict in usable:
        anchor = prepared.anchors.get(verdict.conversation_id)
        if _cancelled(cancel):
            # Stopped: nothing more is recorded or handed back; the rest
            # stay on the tray.
            stopped.add(verdict.conversation_id)
            continue
        if verdict.decision == "reply":
            reply.append(
                GateReply(
                    conversation_id=verdict.conversation_id,
                    card=prepared.cards[verdict.conversation_id],
                    category=verdict.category,
                    anchor_message_id=anchor,
                    _record=_recorder(client, verdict, anchor),
                )
            )
            continue
        try:
            client.decisions.record(
                verdict.conversation_id,
                decision=verdict.decision,
                category=verdict.category,
                through_message_id=anchor,
            )
            recorded += 1
        except Exception as err:
            failures.append(GateRecordFailure(verdict.conversation_id, err))
    return _finish(usable, prepared.ids, reply, recorded, failures, stopped, _cancelled(cancel))


async def run_async(
    tray: TrayLike,
    call_model: AsyncCallModel,
    client: AsyncAgentChatClient,
    *,
    tool_namer: ToolNamer | None = None,
    texts: TextsLike | None = None,
    timeout_ms: float | None = None,
    cancel: CancelHandle | None = None,
) -> GateResult[AsyncGateReply]:
    """Async counterpart of :func:`run`. ``call_model`` may be sync or async;
    reply entries are recorded with ``await entry.record()``.

    ``timeout_ms`` bounds the model call: past it the gate fails closed for
    every card on the tray (nothing recorded; the next wake asks again). A
    sync ``call_model`` cannot be interrupted: it is left to finish, unheard.
    ``None`` (default): no bound. ``cancel`` (anything with ``is_set()``,
    e.g. an :class:`asyncio.Event`) stops the gate as in :func:`run`; the
    :class:`~agentchatme.integration.ReceiveLoop` passes its cycle's, so an
    abandoned cycle records nothing more."""
    prepared = _prepare(tray)
    if not prepared.ids:
        return _closed("empty", [], None)

    try:
        prompt = build_prompt(prepared.data, tool_namer, texts=texts)
    except ValueError as err:
        return _closed("fail_closed", prepared.ids, str(err))
    if _cancelled(cancel):
        return _closed("fail_closed", prepared.ids, _STOPPED)
    try:
        result = (
            call_model(prompt, cancel=cancel)  # type: ignore[call-arg]
            if cancel is not None and _accepts_cancel(call_model)
            else call_model(prompt)
        )
        if inspect.isawaitable(result):
            text = (
                await asyncio.wait_for(result, timeout_ms / 1000.0)
                if timeout_ms is not None
                else await result
            )
        else:
            text = result
    except asyncio.TimeoutError as err:
        detail = str(err) or (
            f"no answer within {timeout_ms:g} ms" if timeout_ms is not None else "no answer"
        )
        return _closed("fail_closed", prepared.ids, f"model call timed out: {detail}")
    except Exception as err:
        return _closed("fail_closed", prepared.ids, f"model call failed: {err}")
    if _cancelled(cancel):
        return _closed("fail_closed", prepared.ids, _STOPPED)

    usable = _usable(text, prepared)
    if isinstance(usable, GateResult):
        return usable

    reply: list[AsyncGateReply] = []
    recorded = 0
    failures: list[GateRecordFailure] = []
    stopped: set[str] = set()
    for verdict in usable:
        anchor = prepared.anchors.get(verdict.conversation_id)
        if _cancelled(cancel):
            stopped.add(verdict.conversation_id)
            continue
        if verdict.decision == "reply":
            reply.append(
                AsyncGateReply(
                    conversation_id=verdict.conversation_id,
                    card=prepared.cards[verdict.conversation_id],
                    category=verdict.category,
                    anchor_message_id=anchor,
                    _record=_async_recorder(client, verdict, anchor),
                )
            )
            continue
        try:
            await client.decisions.record(
                verdict.conversation_id,
                decision=verdict.decision,
                category=verdict.category,
                through_message_id=anchor,
            )
            recorded += 1
        except Exception as err:
            failures.append(GateRecordFailure(verdict.conversation_id, err))
    return _finish(usable, prepared.ids, reply, recorded, failures, stopped, _cancelled(cancel))


_STOPPED = "the gate was stopped"


class CancelHandle(Protocol):
    """What ``cancel`` takes: anything with ``is_set()`` (an
    :class:`asyncio.Event`, a :class:`threading.Event`)."""

    def is_set(self) -> bool: ...


def _cancelled(cancel: CancelHandle | None) -> bool:
    return cancel is not None and bool(cancel.is_set())


def _accepts_cancel(fn: Callable[..., Any]) -> bool:
    try:
        parameters = inspect.signature(fn).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(
        p.kind is inspect.Parameter.VAR_KEYWORD
        or (p.name == "cancel" and p.kind in (p.KEYWORD_ONLY, p.POSITIONAL_OR_KEYWORD))
        for p in parameters
    )


def _recorder(
    client: AgentChatClient, verdict: Verdict, anchor: str | None
) -> Callable[[], ReplyDecision]:
    def record() -> ReplyDecision:
        return client.decisions.record(
            verdict.conversation_id,
            decision="reply",
            category=verdict.category,
            through_message_id=anchor,
        )

    return record


def _async_recorder(
    client: AsyncAgentChatClient, verdict: Verdict, anchor: str | None
) -> Callable[[], Awaitable[ReplyDecision]]:
    async def record() -> ReplyDecision:
        return await client.decisions.record(
            verdict.conversation_id,
            decision="reply",
            category=verdict.category,
            through_message_id=anchor,
        )

    return record


def _finish(
    verdicts: list[Verdict],
    ids: list[str],
    reply: list[ReplyT],
    recorded: int,
    failures: list[GateRecordFailure],
    stopped: set[str],
    cancelled: bool,
) -> GateResult[ReplyT]:
    if cancelled:
        # Stopped: no reply is handed back; what was not recorded is undecided.
        reply = []
        stopped = stopped | {v.conversation_id for v in verdicts if v.decision == "reply"}
    decided = {verdict.conversation_id for verdict in verdicts} - stopped
    return GateResult(
        outcome="decided",
        reply=reply,
        verdicts=verdicts,
        undecided=[conversation_id for conversation_id in ids if conversation_id not in decided],
        recorded=recorded,
        record_failures=failures,
        failure=None,
    )


__all__ = [
    "LEGACY_GATE_CALL_PREAMBLE",
    "AsyncCallModel",
    "AsyncGateReply",
    "CallModel",
    "CancelHandle",
    "GateOutcome",
    "GateRecordFailure",
    "GateReply",
    "GateResult",
    "TextsLike",
    "ToolNamer",
    "TrayLike",
    "Verdict",
    "anchor_of",
    "bare_tool_namer",
    "build_prompt",
    "canonical_json",
    "cards_of",
    "gate_call_texts",
    "mcp_tool_namer",
    "normalize_decision",
    "parse_verdicts",
    "prompt_preamble",
    "run",
    "run_async",
]
