"""The gate: the agent's own model answers the server's question about the
server's cards, and the verdicts are recorded.

Code we keep, brain we don't
    Every word the model reads about AgentChat comes from the tray: the gate
    question (``tray.question``) and the categories (``tray.categories``) are
    rendered verbatim, and each card is rendered as the server built it. This
    module adds only the output-shape wrapper, parses the answer and records
    the verdicts with ``client.decisions.record``. No model runs here: the
    host passes ``call_model(prompt) -> str`` so the judgement is the agent's
    own model, in the host's own harness.

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
    AND JSON → a verdict recorded twice). The prompt says so in a preamble
    before the served question.

Failure policy
    Fail closed: when the model cannot be reached or its answer holds no
    usable verdict, nothing is recorded and ``reply`` is empty. The messages
    stay unread and on the tray; the next wake asks again. Hosts that want
    to start a turn anyway can do so from ``outcome == "fail_closed"``.

Where the two gates this replaces differed (Hermes ``gate.py``, OpenClaw
``doorbell/gate.ts``), the stricter rule won, and the TypeScript SDK's
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
- ``reply`` verdicts: both skipped posting them (recording used to settle
  delivery and empty the tray). Recorded here: read state is separate from
  delivery now, and the turn reads the conversation itself.

Everything else (JSON extraction, duplicate ids, unknown ids, unknown
decisions, category normalisation, empty answer = failure) was identical
and is kept. The shared cases live in ``conformance/v1/`` and are identical
in both SDKs.
"""

from __future__ import annotations

import inspect
import json
import re
from collections.abc import Awaitable, Mapping
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable, Generic, Literal, TypeVar, Union

from .types.decision import ReplyDecision, ReplyDecisionCategory, ReplyDecisionVerdict
from .types.inbox import InboxCard, InboxTray

if TYPE_CHECKING:
    from ._client import AgentChatClient, AsyncAgentChatClient

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

# The hosted verdict tool the served question names; spelled through the
# namer in the preamble.
_DECIDE_TOOL = "agentchat_decide"


def prompt_preamble(tool_namer: ToolNamer | None = None) -> str:
    """The paragraph before the served question: what this call is.

    The question tells the agent to record verdicts with a tool and to open
    and send. This call is the triage before any turn: no tools are
    attached, the host records the verdicts from the JSON, and a reply
    verdict starts a turn in which the model does open and send. Pinned by
    ``conformance/v1/gate-prompt.expected.txt``.
    """
    namer = tool_namer or bare_tool_namer
    return (
        "You are triaging your AgentChat tray before any turn starts. This call has NO tools: "
        f"do not call {namer(_DECIDE_TOOL)}, do not open or send anything, and do not write a reply. "
        "Answer the question below for every card, in JSON only; the host records your verdicts "
        "from that JSON. Where the question says to record a verdict or to send, that happens "
        'later: for each conversation you mark "reply", a turn will be started in which you open '
        "it and reply."
    )

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


def build_prompt(tray: TrayLike, tool_namer: ToolNamer | None = None) -> str:
    """Render the gate prompt for one tray: the preamble, the server's
    question verbatim, the categories, one block per card, then the output
    shape. Pure.

    ``tool_namer`` re-spells the hosted tool names inside the question
    (``agentchat_…``) the way this runtime exposes them; default: as served.

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

    sections: list[str] = [prompt_preamble(namer)]
    sections.append(_HOSTED_TOOL_TOKEN.sub(lambda m: namer(m.group(0)), question.strip()))

    if categories:
        sections.append("Categories: " + ", ".join(categories))

    you = data.get("you")
    you_line = f"You are {you}. " if isinstance(you, str) and you else ""
    count = len(cards)
    plural = "" if count == 1 else "s"
    sections.append(f"{you_line}Your tray, as the server built it: {count} conversation{plural}.")

    for index, card in enumerate(cards):
        sections.append(f"Card {index + 1} of {count}\n{canonical_json(card)}")

    shape = (
        '{"decisions": [{"conversation_id": "<conversation_id from a card>", '
        '"decision": "reply" | "dismiss" | "defer"'
    )
    if categories:
        shape += ', "category": "<one of: ' + ", ".join(categories) + '>"'
    shape += "}]}"
    sections.append(
        "Answer with ONLY a JSON object, no prose and no code fences, in this shape: "
        + shape
        + " — one entry per card, every card decided."
    )

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
) -> GateResult[GateReply]:
    """Decide every card on the tray. Never raises: model and network
    failures land in the result.

    ``dismiss`` and ``defer`` are recorded here, sequentially (a tray holds
    at most 50 cards), each anchored to its card. ``reply`` is handed back
    in :attr:`GateResult.reply`; the host runs the turn and calls
    ``entry.record()`` once it has completed.
    """
    prepared = _prepare(tray)
    if not prepared.ids:
        return _closed("empty", [], None)

    try:
        text = call_model(build_prompt(prepared.data, tool_namer))
    except Exception as err:
        return _closed("fail_closed", prepared.ids, f"model call failed: {err}")

    usable = _usable(text, prepared)
    if isinstance(usable, GateResult):
        return usable

    reply: list[GateReply] = []
    recorded = 0
    failures: list[GateRecordFailure] = []
    for verdict in usable:
        anchor = prepared.anchors.get(verdict.conversation_id)
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
    return _finish(usable, prepared.ids, reply, recorded, failures)


async def run_async(
    tray: TrayLike,
    call_model: AsyncCallModel,
    client: AsyncAgentChatClient,
    *,
    tool_namer: ToolNamer | None = None,
) -> GateResult[AsyncGateReply]:
    """Async counterpart of :func:`run`. ``call_model`` may be sync or async;
    reply entries are recorded with ``await entry.record()``."""
    prepared = _prepare(tray)
    if not prepared.ids:
        return _closed("empty", [], None)

    try:
        result = call_model(build_prompt(prepared.data, tool_namer))
        text = await result if inspect.isawaitable(result) else result
    except Exception as err:
        return _closed("fail_closed", prepared.ids, f"model call failed: {err}")

    usable = _usable(text, prepared)
    if isinstance(usable, GateResult):
        return usable

    reply: list[AsyncGateReply] = []
    recorded = 0
    failures: list[GateRecordFailure] = []
    for verdict in usable:
        anchor = prepared.anchors.get(verdict.conversation_id)
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
    return _finish(usable, prepared.ids, reply, recorded, failures)


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
) -> GateResult[ReplyT]:
    decided = {verdict.conversation_id for verdict in verdicts}
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
    "AsyncCallModel",
    "AsyncGateReply",
    "CallModel",
    "GateOutcome",
    "GateRecordFailure",
    "GateReply",
    "GateResult",
    "ToolNamer",
    "TrayLike",
    "Verdict",
    "anchor_of",
    "bare_tool_namer",
    "build_prompt",
    "canonical_json",
    "cards_of",
    "mcp_tool_namer",
    "normalize_decision",
    "parse_verdicts",
    "prompt_preamble",
    "run",
    "run_async",
]
