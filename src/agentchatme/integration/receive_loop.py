"""The receive loop: the socket as a wake, one cycle per burst, the tray, the
gate call or the run, and what is recorded afterwards. Optional — every part
it uses is exported on its own. The TypeScript kit's ``ReceiveLoop`` is the
twin.

Two modes (contract v1, D5):

- ``gate_call`` (Hermes, OpenClaw): read the tray, ask the agent's own model
  the served gate question in tool-less calls (``call_model``) of at most
  ``gate_chunk_size`` cards each, record ``dismiss`` / ``defer`` at once,
  then start one turn per ``reply`` (``start_turn``), prompted with the
  served ``reply_turn_template`` for its one conversation, and when a turn
  has ended record from what actually happened
  (:func:`~agentchatme.integration.turn_outcome`). Each pending invite on the
  tray gets one invite turn (``start_invite_turn``, default ``start_turn``)
  and the same rule.
- ``single_run`` (Claude Code, Codex, routines, a listener): when anything is
  owed, start one run (``start_run``) with the served wake text; the run
  reads the tray itself and records its own verdicts. The loop records
  nothing for cards. An invite still pending and undismissed after two
  completed runs is dismissed (seen, left pending, not shown again), so an
  ignored invite does not cost a run on every wake; the count is in memory
  only (``dismiss_ignored_invites=False`` turns this off).

Wakes: a ``message.new`` frame that is not the echo of the agent's own send,
a ``wake`` frame (a reminder, or unanswered cards), every accepted HELLO (a
reconnect after a heartbeat gap included), a ``group.invite.received``
frame, and :meth:`ReceiveLoop.wake` for whatever the integration hears
itself (a wake webhook). The socket is the SDK's
:class:`~agentchatme.RealtimeClient` with ``ack_on_receipt`` and the
``ack``, ``heartbeat`` and ``rewake`` capabilities: delivered means this
machine received the message; only a verdict reads it.

There is no retry ladder here. A cycle that could not finish (the tray could
not be read, the model failed, a turn was refused or failed) leaves the
cards on the tray and does nothing more: the server re-wakes unanswered
cards on its own durable ladder (D3).

Pacing (D5): after a silent turn (it looked and did nothing) the same
conversation at the same anchor is not gated again for ``pace_ms`` (60 s)
unless a newer message moves its anchor or an ``unanswered`` wake names it;
after an invite turn that left the invite pending, no second invite turn for
it within ``pace_ms`` unless an ``unanswered`` wake arrives. In single-run
mode the run that would dismiss an ignored invite counts only when it ended
at least ``pace_ms`` after the first counted one. In memory.

Chunks (contract v1, section 3.6): a tray holds up to 50 cards, and one
answer for all of them can outgrow what a runtime lets its model write; a
cut-off answer does not parse, so nothing would be recorded and the tray
would never shrink. The cards are split, in tray order, into chunks of at
most ``gate_chunk_size`` (10), and each chunk gets its own gate call, one
after another, its prompt assembled exactly as for a whole tray with N =
the cards in that chunk (a tray that fits one chunk gets exactly that
tray's prompt). A chunk with nothing owed in it (muted cards only) costs no
call: its cards are left as in a ``nothing_owed`` cycle. Each chunk's
``dismiss`` / ``defer`` are recorded as soon as it answers. A chunk that
fails (the call failed or ran past its deadline, or its answer held no
usable verdict) leaves only its own cards undecided; the other chunks'
verdicts are recorded. Reply turns start once every chunk has answered, so
a turn never finds on its tray a card the gate has yet to decide. The cycle
is ``decided`` when any chunk answered and ``fail_closed`` when none did;
``chunks_ok`` / ``chunks_failed``, the ``failure`` and one ``gate_chunk``
event per chunk say which.

Deadlines: each gate call (one per chunk) may take ``gate_call_timeout_ms``
(60 s); past it that chunk fails closed for its cards (nothing recorded for
them; the next wake asks again), its cancel handle is set, and the next
chunk gets a deadline of its own. A whole cycle may take
``cycle_timeout_ms`` (10 min), not counting time spent starting turns,
running a run, or inside gate calls while they have a deadline of their own
(a tray gated in k chunks may take k deadlines; with ``gate_call_timeout_ms``
off, gate calls count: the watchdog is then their only bound). Past it the
cycle is abandoned (logged, summarised as ``abandoned``) and wakes are taken
again, so a cycle that never finishes cannot leave the loop looking healthy
while it processes nothing; an abandoned cycle records nothing more, asks no
further chunk and starts no turn (a verdict already on its way is left to
finish, and its card is held out of the next gate until it answers).
``status().recent_cycles`` holds the last outcomes for a host to alarm on.
Either deadline: ``None`` is the default, ``0`` turns it off. Both run on
the loop's timer (``set_timer``) and clock (``now`` when given, else a
monotonic one; a wall-clock ``now`` makes the watchdog wall-clock), so a
test can move them by hand. A gate call whose deadline
is longer than ``cycle_timeout_ms`` is not cut by the watchdog: time inside
a deadline-bound gate call is not counted.

Turns: at most ``max_concurrent_turns`` (default 1) reply and invite turns
run at once; the rest wait for a slot, held out of the gate meanwhile (their
cards stay on the tray; nothing is recorded for them). A turn that ends
after :meth:`ReceiveLoop.stop` still has its outcome recorded.

The runtime's callbacks may be coroutine functions or plain functions. A
plain ``call_model``, ``start_turn``, ``start_run`` or ``readiness`` runs in
a worker thread, so a blocking runtime never holds up the socket: a plain
``start_turn`` runs the whole turn and returns its
:class:`~agentchatme.integration.TurnEnd`; a coroutine ``start_turn``
returns a :class:`TurnAdmission` whose ``done`` resolves when the turn ends.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import inspect
import logging
import threading
import time
from collections import OrderedDict, deque
from collections.abc import Awaitable, Iterator, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Callable, Literal

from .. import gate as _gate
from .._client_identity import AgentChatClientIdentity
from .._realtime import RealtimeClient
from ..types.texts import Texts
from .anchor import normalize_handle
from .coalescer import SetTimer, TimerHandle, WakeCoalescer, _loop_timer, _monotonic_ms
from .outcome import (
    InviteOutcome,
    TurnEnd,
    apply_invite_outcome_async,
    apply_outcome_async,
    invite_outcome,
    turn_outcome,
)
from .turns import RunningTurn, TrackedTurn, TurnHandle, TurnTracker

_log = logging.getLogger("agentchat.integration")

TRAY_LIMIT = 50
"""Largest tray the server builds: the loop always reads all of it."""
GATE_CHUNK_SIZE = 10
"""Default for ``gate_chunk_size``: the most cards one gate call is asked
about (contract v1, section 3.6: at most 10). Valid: 1 to ``TRAY_LIMIT``."""
TURN_OVERDUE_MS = 30 * 60_000
"""A running turn older than this is shown as overdue in ``status()``."""
_NO_PROGRESS_BACKOFF_MS = 1_000
_NO_PROGRESS_BACKOFF_MAX_MS = 60_000
GATE_CALL_TIMEOUT_MS = 60_000
"""Default bound on one gate call (the model answering the gate question)."""
CYCLE_TIMEOUT_MS = 10 * 60_000
"""Default bound on one cycle, not counting time spent starting turns or
runs: past it the cycle is abandoned and wakes are taken again."""
RECENT_CYCLES = 20
"""How many cycle summaries ``status()`` keeps."""
_WATCHDOG_RECHECK_MIN_MS = 5.0
PACE_MS = 60_000
"""D5 pacing: how long a silent turn's card (or an invite after its turn) is
not gated again at the same anchor."""
_SILENT_MEMORY = 1_000
"""Silent-turn memory (D5 "second time for the same anchor"), bounded."""

ReceiveLoopMode = Literal["gate_call", "single_run"]
WakeSource = Literal["message", "wake_frame", "connect", "invite", "external", "more"]
CycleOutcome = Literal[
    "empty",
    "nothing_owed",
    "tray_failed",
    "texts_missing",
    "tools_unavailable",
    "decided",
    "fail_closed",
    "invites_only",
    "run_completed",
    "run_failed",
    "run_not_started",
    "abandoned",
]


@dataclass(frozen=True)
class TurnRequest:
    """One turn the loop asks the runtime to start (gate-call mode)."""

    kind: Literal["reply", "invite"]
    prompt: str
    """``reply``: the served reply-turn template with the conversation id
    filled in (from a server that predates it: the served wake instruction
    and the conversation id; see :func:`reply_prompt`); ``invite``: the
    served invite prompt and the invite id."""
    texts: Texts
    stopping: asyncio.Event = field(repr=False)
    """Set when the loop stops."""
    conversation_id: str | None = None
    card: dict[str, Any] | None = None
    """``reply``: the card the gate decided on, as served."""
    anchor: str | None = None
    """``reply``: the message the gate's verdict reads through."""
    category: str | None = None
    invite: dict[str, Any] | None = None
    """``invite``: the tray invite, as served."""


@dataclass(frozen=True)
class TurnAdmission:
    """Whether the runtime admitted the turn. ``done`` resolves with what the
    runtime reports when it ends — ``still_running`` (with ``next`` when
    known) if the integration stopped waiting before the end."""

    started: bool
    done: Awaitable[TurnEnd] | None = None
    error: str | None = None


@dataclass(frozen=True)
class RunRequest:
    """The one run the loop asks for (single-run mode)."""

    prompt: str
    """The served wake instruction (and the invite prompt when invites
    wait), or the invite prompt alone."""
    texts: Texts
    cards: int | None
    """Owed cards on the tray that started it; ``None`` when the tray could
    not be read."""
    invites: int | None
    stopping: asyncio.Event = field(repr=False)


@dataclass(frozen=True)
class RunEnd:
    status: Literal["completed", "failed", "not_started"]
    detail: str | None = None


@dataclass(frozen=True)
class Readiness:
    """Whether a turn started now could reach AgentChat (e.g. from
    :func:`~agentchatme.integration.probe_hosted_tools`)."""

    ready: bool
    reason: str | None = None


@dataclass(frozen=True)
class ReceiveLoopSocketOptions:
    """Options to build the loop's socket."""

    api_key: str
    base_url: str | None = None
    """``wss://…``; default: the client's base URL with ``http`` → ``ws``."""
    client_identity: AgentChatClientIdentity | None = None
    websocket_connect: Callable[..., Awaitable[Any]] | None = None
    reconnect_interval_ms: int | None = None
    max_reconnect_interval_ms: int | None = None


@dataclass(frozen=True)
class CycleSummary:
    at: str
    outcome: CycleOutcome
    cards: int = 0
    invites: int = 0
    gated: int = 0
    reply: int = 0
    dismiss: int = 0
    defer: int = 0
    undecided: int = 0
    record_failures: int = 0
    turns_started: int = 0
    turns_refused: int = 0
    turns_queued: int = 0
    """Reply or invite turns waiting for a slot (``max_concurrent_turns``)."""
    held_by_turns: int = 0
    """Cards left out because a turn for their conversation is running."""
    paced: int = 0
    """Cards and invites left out by D5 pacing (a recent silent turn)."""
    truncated: bool = False
    failure: str | None = None
    """Why the cycle could not finish, or, when some gate chunks failed, how
    many and the first one's reason (``2 of 5 gate chunks failed; chunk 3:
    …``; a cycle that made one gate call gives that call's reason as it is,
    also when other chunks of its plan were skipped)."""
    chunks_ok: int = 0
    """Gate-call mode: gate calls (one per chunk of at most
    ``gate_chunk_size`` cards) whose answer gave verdicts."""
    chunks_failed: int = 0
    """Gate calls that failed closed (the call failed or ran past its
    deadline, or its answer held no usable verdict); only their own cards
    stay undecided. A call the cycle's end cut short (:meth:`ReceiveLoop.stop`,
    the watchdog) is neither: the cycle is ``abandoned``."""


@dataclass(frozen=True)
class ReceiveLoopStatus:
    mode: ReceiveLoopMode
    started: bool
    stopped: bool
    handle: str | None
    socket: dict[str, Any] | None
    coalescer: dict[str, Any]
    texts: dict[str, Any]
    last_cycle: CycleSummary | None
    recent_cycles: list[CycleSummary]
    """The last cycles, oldest first (at most ``RECENT_CYCLES``): alarm on a
    run of ``fail_closed`` / ``tools_unavailable`` / ``texts_missing`` /
    ``abandoned``."""
    turns: list[RunningTurn]
    counters: dict[str, Any]
    last_error: str | None


ReceiveLoopEvent = dict[str, Any]
"""``{"type": "wake" | "echo" | "cycle" | "gate_chunk" | "turn_started" |
"turn_refused" | "turn_outcome" | "invite_outcome" | "run" | "error", ...}``.

``gate_chunk`` (gate-call mode, one per gate call, in order, once its
verdicts are recorded): ``cycle`` (the cycle's identifier: the chunks of one
cycle share it), ``index`` (1-based, in tray order), ``of`` (how many chunks
the cycle's cards were split into), ``cards`` (the N of its prompt),
``outcome`` (``decided`` or ``fail_closed``), ``undecided`` (its cards left
without a usable verdict) and ``failure``; a partial failure shows here
chunk by chunk. A cycle the watchdog abandoned emits no further one."""

TurnStarter = Callable[[TurnRequest], Any]
RunStarter = Callable[[RunRequest], Any]
ReadinessCheck = Callable[[], Any]


class _StaticTexts:
    """A fixed text set as a texts source."""

    def __init__(self, texts: Texts) -> None:
        self._texts = texts
        self.source = "static"
        self.fetched_at: str | None = None
        self.last_error: str | None = None

    def current(self) -> Texts | None:
        return self._texts

    async def ensure(self) -> Texts | None:
        return self._texts


_CONVERSATION_PLACEHOLDER = "<conversation_id>"


def reply_prompt(texts: Any, conversation_id: str) -> str:
    """The prompt a reply turn starts with: the served
    ``reply_turn_template`` with every ``<conversation_id>`` replaced by the
    conversation's id, which sends the turn to that one conversation and no
    other. A served template without the placeholder is used as it is, with
    a final ``conversation_id:`` line. Texts without the field (an older
    server, a copy cached before it; or a value that is empty, blank or not
    a string) fall back to the served wake instruction and a
    ``conversation_id:`` line."""
    if isinstance(texts, Mapping):
        template = texts.get("reply_turn_template")
        instruction = texts.get("wake_instruction")
    else:
        template = getattr(texts, "reply_turn_template", None)
        instruction = texts.wake_instruction
    if isinstance(template, str) and template.strip():
        body = template.strip()
        if _CONVERSATION_PLACEHOLDER in body:
            return body.replace(_CONVERSATION_PLACEHOLDER, conversation_id)
        # Served, but it does not name the conversation: the template still
        # leads, and the conversation is named after it.
        return f"{body}\n\nconversation_id: {conversation_id}"
    return f"{str(instruction).strip()}\n\nconversation_id: {conversation_id}"


def _iso(epoch_ms: float) -> str:
    moment = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def _ws_base(http_base: str | None) -> str:
    base = (http_base or "https://api.agentchat.me").rstrip("/")
    if base.startswith("https://"):
        return "wss://" + base[len("https://") :]
    if base.startswith("http://"):
        return "ws://" + base[len("http://") :]
    return base


def _consume_outcome(task: asyncio.Future[Any]) -> None:
    """A shielded record whose caller went away: its failure is not lost
    in the log (the gate saw it, or the cycle was abandoned)."""
    if not task.cancelled():
        task.exception()


def _number_text(value: float) -> str:
    """A duration in a failure text: an integral value as an integer
    (``1200000``, never ``1.2e+06``), any other as it is (``1.5``), as the
    TypeScript kit prints it."""
    number = float(value)
    return str(int(number)) if number.is_integer() else repr(number)


def _timeout_option(value: float | None, default: float, name: str) -> float:
    """A timeout option in ms: ``None`` → the default, ``0`` → off (returned
    as 0), a positive number → itself; anything else raises."""
    if value is None:
        return float(default)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value != value or value < 0:
        raise ValueError(f"ReceiveLoop: {name} must be 0 (off) or a positive number")
    if value == float("inf"):
        return 0.0
    return float(value)


class _StaleFlag:
    """``is_set()`` once a cycle has gone stale (abandoned or stopped): the
    gate's cancel handle."""

    def __init__(self, stale: Callable[[], bool]) -> None:
        self._stale = stale

    def is_set(self) -> bool:
        return self._stale()


class _CallCancel:
    """Cancel handles for the runtime's model call: an
    :class:`asyncio.Event` for a coroutine ``call_model``, a
    :class:`threading.Event` for a plain one (it runs in a worker thread).

    Each cycle holds one; each gate call (one per chunk) takes a
    :meth:`child`, set by that call's own deadline or with the cycle's (the
    cycle abandoned, the loop stopped). A chunk past its deadline tells only
    its own call: the next chunk's call starts with a fresh handle. Used on
    the loop's event loop only."""

    def __init__(self) -> None:
        self.on_loop = asyncio.Event()
        self.in_thread = threading.Event()
        self._children: set[_CallCancel] = set()

    def is_set(self) -> bool:
        return self.in_thread.is_set()

    def set(self) -> None:
        self.on_loop.set()
        self.in_thread.set()
        for child in list(self._children):
            child.set()

    def child(self) -> _CallCancel:
        handles = _CallCancel()
        if self.is_set():
            handles.set()
        else:
            self._children.add(handles)
        return handles

    def release(self, child: _CallCancel) -> None:
        self._children.discard(child)


def _chunked(cards: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    """The cards to gate, in tray order, in chunks of at most ``size``
    (contract v1, section 3.6). A tray that fits one chunk is that one chunk,
    exactly as served, so its prompt is exactly the whole tray's."""
    return [cards[start : start + size] for start in range(0, len(cards), size)]


@dataclass
class _ChunkedGate:
    """What the gate's chunks of one cycle came to, together."""

    gated: int = 0
    """Cards sent to the model in a chunk whose call was not cut short; each
    counts once, so it is ``reply`` + ``dismiss`` + ``defer`` +
    ``undecided``."""
    held: int = 0
    """Cards left out of their chunk: a turn took them while earlier chunks
    were gated."""
    reply: list[_gate.AsyncGateReply] = field(default_factory=list)
    """The replies, chunk by chunk, each chunk's in the model's order."""
    verdicts: dict[str, int] = field(default_factory=lambda: {"reply": 0, "dismiss": 0, "defer": 0})
    undecided: int = 0
    recorded: int = 0
    record_failures: int = 0
    chunks: list[dict[str, Any]] = field(default_factory=list)
    """The ``gate_chunk`` reports, in order."""


def _chunk_failure(chunks: list[dict[str, Any]]) -> str | None:
    """The cycle's failure from the gate calls it made (``chunks``: their
    reports): none when every call decided; the reason as it is when one
    call was made (also when the plan had more chunks, skipped as muted-only
    or held); otherwise how many of the calls made failed, and the first
    failed chunk's position in the plan and reason (each chunk's own is in
    its ``gate_chunk`` event)."""
    failed = [chunk for chunk in chunks if chunk["outcome"] == "fail_closed"]
    if not failed:
        return None
    first = failed[0]
    reason = first["failure"] or "no usable verdicts in the answer"
    if len(chunks) == 1:
        return str(reason)
    # Counted over the calls made; the chunk named by its planned position,
    # as in its gate_chunk event.
    return f"{len(failed)} of {len(chunks)} gate chunks failed; chunk {first['index']}: {reason}"


def _chunk_size_option(value: object) -> int:
    """``gate_chunk_size``: ``None`` → the default, an int from 1 to
    ``TRAY_LIMIT`` → itself; anything else raises."""
    if value is None:
        return GATE_CHUNK_SIZE
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= TRAY_LIMIT:
        raise ValueError(f"ReceiveLoop: gate_chunk_size must be an int from 1 to {TRAY_LIMIT}")
    return value


def _tray_invites(tray: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = tray.get("invites")
    if not isinstance(raw, list):
        return []
    return [
        invite
        for invite in raw
        if isinstance(invite, dict) and isinstance(invite.get("invite_id"), str) and invite["invite_id"]
    ]


def _api_key_of(client: Any) -> str | None:
    http = getattr(client, "_http", None)
    options = getattr(http, "_options", None)
    key = getattr(options, "api_key", None)
    return key if isinstance(key, str) and key else None


class _Pacer:
    """D5 pacing, in memory: a key (``conversation:<id>`` / ``invite:<id>``)
    held until a time, for one anchor."""

    def __init__(self, pace_ms: float, now: Callable[[], float]) -> None:
        self._pace_ms = pace_ms
        self._now = now
        self._until: dict[str, tuple[str | None, float]] = {}

    def hold(self, key: str, anchor: str | None) -> None:
        self._until[key] = (anchor, self._now() + self._pace_ms)
        if len(self._until) > _SILENT_MEMORY:
            self._until.pop(next(iter(self._until)))

    def held(self, key: str, anchor: str | None) -> bool:
        entry = self._until.get(key)
        if entry is None:
            return False
        held_anchor, until = entry
        if held_anchor != anchor or self._now() >= until:
            del self._until[key]
            return False
        return True

    def release(self, key: str) -> None:
        self._until.pop(key, None)

    def release_invites(self) -> None:
        for key in [k for k in self._until if k.startswith("invite:")]:
            del self._until[key]


class ReceiveLoop:
    """See the module docstring.

    ``client`` is an :class:`~agentchatme.AsyncAgentChatClient`. ``texts`` is
    a :class:`~agentchatme.integration.TextsCache` (anything with
    ``current()`` and ``ensure()``) or a fixed
    :class:`~agentchatme.types.Texts`. ``socket`` is
    :class:`ReceiveLoopSocketOptions`, a :class:`~agentchatme.RealtimeClient`
    you built (build it with ``ack_on_receipt=True`` and
    ``capabilities={"rewake": True}``; the loop attaches its handlers and
    connects and disconnects it), ``"auto"`` (default: built from the
    client's key and base URL) or ``None`` (no socket: wakes only through
    :meth:`wake`). ``turn_tracker`` shares a
    :class:`~agentchatme.integration.TurnTracker` with the host (turns it
    tracks itself are kept out of the gate too). ``dismiss_ignored_invites``
    (single-run mode, default ``True``) dismisses an invite still pending
    after two completed runs that left it; ``False`` leaves every invite to
    the runs (a host that forwards invites elsewhere): no read after the run
    and nothing recorded for them. ``gate_chunk_size`` (gate-call mode;
    ``None``: :data:`GATE_CHUNK_SIZE`, 10; an int from 1 to
    :data:`TRAY_LIMIT`) is the most cards one gate call decides: a larger
    tray is gated in chunks of this size (see the module docstring); 50
    gates the largest tray in one call. ``now`` (epoch ms) and ``set_timer``
    are the loop's clock and timers: pacing, the coalescer, the backoff for
    a truncated tray, the gate-call deadline and the watchdog run on them (a
    test passes fakes). Without ``now`` the watchdog measures on a monotonic
    clock; a ``now`` that is given is the watchdog's clock too, so a
    wall-clock ``now`` makes the watchdog wall-clock (a clock step then
    counts against, or for, the running cycle).
    """

    def __init__(
        self,
        client: Any,
        texts: Any,
        mode: ReceiveLoopMode,
        *,
        call_model: Callable[..., Any] | None = None,
        start_turn: TurnStarter | None = None,
        start_invite_turn: TurnStarter | None = None,
        start_run: RunStarter | None = None,
        socket: ReceiveLoopSocketOptions | RealtimeClient | Literal["auto"] | None = "auto",
        handle: str | None = None,
        tool_namer: _gate.ToolNamer | None = None,
        readiness: ReadinessCheck | None = None,
        quiet_ms: float | None = None,
        max_ms: float | None = None,
        on_event: Callable[[ReceiveLoopEvent], Any] | None = None,
        max_concurrent_turns: int = 1,
        pace_ms: float = PACE_MS,
        gate_call_timeout_ms: float | None = None,
        cycle_timeout_ms: float | None = None,
        turn_tracker: TurnTracker | None = None,
        dismiss_ignored_invites: bool = True,
        gate_chunk_size: int | None = None,
        now: Callable[[], float] | None = None,
        set_timer: SetTimer | None = None,
    ) -> None:
        if mode not in ("gate_call", "single_run"):
            raise ValueError("ReceiveLoop: mode is gate_call or single_run")
        if mode == "gate_call" and (call_model is None or start_turn is None):
            raise ValueError("ReceiveLoop: gate_call mode needs call_model and start_turn")
        if mode == "single_run" and start_run is None:
            raise ValueError("ReceiveLoop: single_run mode needs start_run")
        if not isinstance(max_concurrent_turns, int) or max_concurrent_turns < 1:
            raise ValueError("ReceiveLoop: max_concurrent_turns must be an int >= 1")
        self._gate_chunk_size = _chunk_size_option(gate_chunk_size)
        self._client = client
        self._texts: Any = _StaticTexts(texts) if isinstance(texts, Texts) else texts
        self._mode: ReceiveLoopMode = mode
        self._call_model = call_model
        self._start_turn = start_turn
        self._start_invite_turn = start_invite_turn or start_turn
        self._start_run = start_run
        self._tool_namer = tool_namer
        self._readiness = readiness
        self._on_event = on_event
        self._now = now or (lambda: time.time() * 1000.0)
        # Durations the watchdog measures: the given clock (a test's), else
        # a monotonic one, so a wall-clock step never cuts a cycle short.
        self._clock: Callable[[], float] = now or _monotonic_ms
        self._handle = normalize_handle(handle) if handle else None
        self._tracker = turn_tracker if turn_tracker is not None else TurnTracker(now=self._now)
        self._pace_ms = pace_ms
        self._pacer = _Pacer(pace_ms, self._now)
        self._max_turns = max_concurrent_turns
        self._dismiss_ignored_invites = dismiss_ignored_invites is not False
        # Turns holding a slot right now.
        self._active: set[TurnHandle] = set()
        # Turns waiting for a slot: (their tracker handle, the admission).
        self._waiting: deque[tuple[TurnHandle, Callable[[], Awaitable[bool]]]] = deque()
        # A released turn (here, or by the host through a shared tracker)
        # frees its slot at once; one released while it waits is dropped.
        self._unsubscribe_release = self._tracker.on_release(self._on_release_any_thread)
        coalescer_options: dict[str, Any] = {}
        if quiet_ms is not None:
            coalescer_options["quiet_ms"] = quiet_ms
        if max_ms is not None:
            coalescer_options["max_ms"] = max_ms
        self._set_timer: SetTimer = set_timer or _loop_timer
        self._no_progress_streak = 0
        self._more_timer: TimerHandle | None = None
        self._coalescer = WakeCoalescer(
            self._cycle,
            on_error=self._fail,
            now=self._now,
            set_timer=set_timer,
            **coalescer_options,
        )
        self._stopping = asyncio.Event()
        self._unsubscribe: list[Callable[[], Any]] = []
        self._silent: OrderedDict[str, int] = OrderedDict()
        # When a single-run invite's silent run was counted (D5 pacing in
        # single-run mode).
        self._silent_at: dict[str, float] = {}
        # Conversations whose verdict a gate call is recording right now (an
        # abandoned cycle's included): held out of the next gate.
        self._records_in_flight: set[str] = set()
        # The event loop the loop runs on (set by start()): a release from
        # another thread is handed over to it.
        self._event_loop: asyncio.AbstractEventLoop | None = None
        self._pending_work: set[asyncio.Future[Any]] = set()
        self._turn_tasks: set[asyncio.Future[Any]] = set()
        self._started = False
        self._stopped = False
        self._held_while_running = False
        self._last_cycle: CycleSummary | None = None
        self._recent: deque[CycleSummary] = deque(maxlen=RECENT_CYCLES)
        # The running cycle's identity: an abandoned cycle goes stale and
        # acts no more; its cancel handles tell a pending model call.
        self._cycle_token = 0
        self._cycle_cancel = _CallCancel()
        # None: the default; 0: no deadline; a negative value is an error.
        self._gate_call_timeout_ms = _timeout_option(
            gate_call_timeout_ms, GATE_CALL_TIMEOUT_MS, "gate_call_timeout_ms"
        )
        self._cycle_timeout_s = (
            _timeout_option(cycle_timeout_ms, CYCLE_TIMEOUT_MS, "cycle_timeout_ms") / 1000.0
        )
        # Time the running cycle spends starting its own turns, running its
        # run, or inside a gate call that has a deadline of its own (left out
        # by the cycle watchdog). An admission _pump starts runs outside any
        # cycle and is not counted here.
        self._uncounted_depth = 0
        self._uncounted_since = 0.0
        self._uncounted_clock = 0.0
        self._last_error: str | None = None
        self._counts: dict[str, int] = {
            "frames": 0,
            "echoes": 0,
            "turns_started": 0,
            "turns_refused": 0,
            "turns_queued": 0,
            "invite_turns_started": 0,
            "runs_started": 0,
            "runs_completed": 0,
            "runs_failed": 0,
            "runs_not_started": 0,
            "outcome_record_failures": 0,
            "chunks_ok": 0,
            "chunks_failed": 0,
        }
        self._outcomes: dict[str, int] = {}
        self._realtime = self._build_socket(socket)

    def _build_socket(self, socket: Any) -> RealtimeClient | None:
        if socket is None:
            return None
        if isinstance(socket, RealtimeClient):
            return socket
        options = socket if isinstance(socket, ReceiveLoopSocketOptions) else None
        api_key = options.api_key if options is not None else _api_key_of(self._client)
        if not api_key:
            raise ValueError("ReceiveLoop: the socket needs an API key (ReceiveLoopSocketOptions)")
        kwargs: dict[str, Any] = {
            "api_key": api_key,
            "base_url": (options.base_url if options and options.base_url else None)
            or _ws_base(getattr(self._client, "base_url", None)),
            "client": self._client,
            "ack_on_receipt": True,
            "capabilities": {"heartbeat": True, "rewake": True},
        }
        if options is not None:
            if options.client_identity is not None:
                kwargs["client_identity"] = options.client_identity
            if options.websocket_connect is not None:
                kwargs["websocket_connect"] = options.websocket_connect
            if options.reconnect_interval_ms is not None:
                kwargs["reconnect_interval_ms"] = options.reconnect_interval_ms
            if options.max_reconnect_interval_ms is not None:
                kwargs["max_reconnect_interval_ms"] = options.max_reconnect_interval_ms
        return RealtimeClient(**kwargs)

    # ─── lifecycle ───────────────────────────────────────────────────────

    async def start(self) -> None:
        """Attach to the socket and connect it; with no socket, wait for
        :meth:`wake`. Returns once the connect loop has started."""
        if self._started or self._stopped:
            return
        self._started = True
        self._event_loop = asyncio.get_running_loop()
        rt = self._realtime
        if rt is None:
            return
        self._unsubscribe.extend(
            [
                rt.on("message.new", self._on_message),
                rt.on("wake", self._on_wake_frame),
                rt.on("group.invite.received", self._on_invite_frame),
                rt.on_connect(lambda: self._wake_from("connect")),
                rt.on_error(lambda error: self._emit({"type": "error", "error": error})),
            ]
        )
        try:
            await rt.connect()
        except Exception as err:
            # The socket retries on its own; a missing websockets package is
            # the one failure worth reporting here.
            self._fail(err)

    def wake(self) -> None:
        """A wake the integration heard itself (a wake webhook, a timer of
        its own). Safe to call from another thread."""
        self._wake_from("external")

    async def stop(self) -> None:
        """Stop: no new cycles or turns, the socket closed, running turns and
        runs told through ``stopping``, turns waiting for a slot dropped
        (nothing recorded; their cards stay on the tray). Returns when the
        running cycle and any recording already under way have finished; it
        does not wait for turns the runtime has not ended — when one ends
        later, its outcome is still recorded (the client must still be
        open)."""
        if self._stopped:
            return
        self._stopped = True
        # The running cycle stops acting: a gate mid-way records no further
        # verdict and asks no further chunk (their cards stay on the tray for
        # the next start), and a pending model call is told through its
        # cancel handle. A verdict already on its way is left to finish.
        self._cycle_cancel.set()
        self._coalescer.stop()
        self._clear_more_timer()
        self._unsubscribe_release()
        self._stopping.set()
        # Turns that never got a slot never start; their cards stay on the tray.
        while self._waiting:
            handle, _admit = self._waiting.popleft()
            self._tracker.end(handle)
        for off in self._unsubscribe:
            with contextlib.suppress(Exception):
                off()
        self._unsubscribe.clear()
        if self._realtime is not None:
            with contextlib.suppress(Exception):
                await self._realtime.disconnect()
        await self._coalescer.idle()
        if self._pending_work:
            await asyncio.gather(*list(self._pending_work), return_exceptions=True)

    async def idle(self) -> None:
        """Return when the running cycle has finished (tests, graceful stops)."""
        await self._coalescer.idle()

    def release_turn(self, key: str) -> bool:
        """Let go of a turn whose end the runtime will never confirm (an
        operator's call). ``key`` is ``conversation:<id>`` or
        ``invite:<id>``. Nothing is recorded for it, its slot is freed at
        once (a waiting turn starts), and its card may be gated again. Safe
        to call from another thread: the work runs on the loop's event
        loop."""
        released = self._tracker.release(key)
        if released:
            self._wake_from("more")
        return released

    def status(self) -> ReceiveLoopStatus:
        texts = self._texts.current()
        counters = self._coalescer.counters()
        rt = self._realtime
        return ReceiveLoopStatus(
            mode=self._mode,
            started=self._started,
            stopped=self._stopped,
            handle=self._handle,
            socket=None
            if rt is None
            else {
                "state": rt.state,
                "ack_negotiated": rt.ack_negotiated,
                "capabilities": sorted(rt.capabilities),
                "intake_paused": rt.intake_paused,
            },
            coalescer={
                "state": self._coalescer.state,
                "wakes": counters.wakes,
                "absorbed": counters.absorbed,
                "cycles": counters.cycles,
            },
            texts={
                "version": texts.version if texts is not None else None,
                "manual_version": texts.manual.version if texts is not None else None,
                "source": getattr(self._texts, "source", None),
                "fetched_at": getattr(self._texts, "fetched_at", None),
                "last_error": getattr(self._texts, "last_error", None),
            },
            last_cycle=self._last_cycle,
            recent_cycles=list(self._recent),
            turns=[
                replace(turn, overdue=self._now() - turn.started_at > TURN_OVERDUE_MS)
                for turn in self._tracker.running()
            ],
            counters={
                **self._counts,
                "turns_active": len(self._active),
                "turns_waiting": len(self._waiting),
                "outcomes": dict(self._outcomes),
            },
            last_error=self._last_error,
        )

    # ─── wakes ───────────────────────────────────────────────────────────

    def _on_message(self, message: dict[str, Any]) -> None:
        self._counts["frames"] += 1
        payload = message.get("payload")
        data = payload if isinstance(payload, dict) else {}
        conversation_id = data.get("conversation_id") if isinstance(data.get("conversation_id"), str) else None
        raw = data.get("sender") or data.get("from") or data.get("sender_handle")
        sender = normalize_handle(raw) if isinstance(raw, str) else ""
        if self._handle is not None and sender == self._handle:
            # The server echoes this agent's own sends back; its own words
            # never wake it.
            self._counts["echoes"] += 1
            self._emit({"type": "echo", "conversation_id": conversation_id})
            return
        self._wake_from("message", conversation_id)

    def _on_wake_frame(self, frame: dict[str, Any]) -> None:
        self._counts["frames"] += 1
        payload = frame.get("payload")
        data = payload if isinstance(payload, dict) else {}
        cid = data.get("conversation_id")
        reason = data.get("reason") if isinstance(data.get("reason"), str) else None
        ids = data.get("conversation_ids")
        named = [i for i in ids if isinstance(i, str)] if isinstance(ids, list) else []
        if reason == "unanswered":
            # The server says these are still unanswered: pacing yields.
            for conversation_id in named:
                self._pacer.release(f"conversation:{conversation_id}")
            self._pacer.release_invites()
        extra: dict[str, Any] = {"reason": reason}
        if named:
            extra["conversation_ids"] = named
        self._wake_from("wake_frame", cid if isinstance(cid, str) else None, extra)

    def _on_invite_frame(self, _frame: dict[str, Any]) -> None:
        self._counts["frames"] += 1
        self._wake_from("invite")

    def _wake_from(
        self,
        source: WakeSource,
        conversation_id: str | None = None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        if self._stopped:
            return
        hop = self._hop()
        if hop is not None:
            # Called from another thread (release_turn, wake): the wake runs
            # on the loop's own event loop.
            with contextlib.suppress(RuntimeError):  # the event loop has closed
                hop.call_soon_threadsafe(self._wake_from, source, conversation_id, extra)
            return
        event: ReceiveLoopEvent = {"type": "wake", "source": source}
        if conversation_id:
            event["conversation_id"] = conversation_id
        if extra:
            event.update(extra)
        self._emit(event)
        self._coalescer.wake()

    # ─── one cycle ───────────────────────────────────────────────────────

    async def _cycle(self) -> None:
        if self._stopped:
            return
        self._cycle_token += 1
        token = self._cycle_token
        cancel = self._cycle_cancel = _CallCancel()
        body: asyncio.Future[CycleSummary] = asyncio.ensure_future(
            self._gate_call_cycle(token, cancel) if self._mode == "gate_call" else self._single_run_cycle(token)
        )
        summary = await self._watch(body)
        self._last_cycle = summary
        self._recent.append(summary)
        if summary.failure is not None:
            self._last_error = summary.failure
        self._emit({"type": "cycle", "summary": summary})

    async def _watch(self, body: asyncio.Future[CycleSummary]) -> CycleSummary:
        """The cycle watchdog: a cycle running longer than
        ``cycle_timeout_ms`` — not counting time spent starting turns, runs,
        or gate calls that have a deadline of their own — is abandoned
        (cancelled, logged, summarised as ``abandoned``) so the coalescer
        takes wakes again and the next wake retries. It runs on the loop's
        timer (``set_timer``) and clock (``now`` when given, else a
        monotonic one)."""
        timeout_ms = self._cycle_timeout_s * 1000.0
        if timeout_ms <= 0:
            return await body
        started = self._clock()
        uncounted_at_start = self._uncounted_ms()
        overdue: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        timer: TimerHandle | None = None

        def check() -> None:
            nonlocal timer
            timer = None
            if overdue.done() or body.done():
                return
            active = (self._clock() - started) - (self._uncounted_ms() - uncounted_at_start)
            if active >= timeout_ms and self._uncounted_depth == 0:
                overdue.set_result(None)
                return
            timer = self._set_timer(max(_WATCHDOG_RECHECK_MIN_MS, timeout_ms - active), check)

        timer = self._set_timer(timeout_ms, check)
        either: set[asyncio.Future[Any]] = {body, overdue}
        try:
            await asyncio.wait(either, return_when=asyncio.FIRST_COMPLETED)
        except asyncio.CancelledError:
            body.cancel()
            raise
        finally:
            if timer is not None:
                timer.cancel()
        if body.done():
            return body.result()
        # Abandoned: the cycle goes stale (every step checks it), its pending
        # model call is told, and the coalescer takes wakes again.
        self._cycle_token += 1
        self._cycle_cancel.set()
        body.cancel()
        with contextlib.suppress(BaseException):
            await body
        failure = (
            f"cycle abandoned after {_number_text(self._cycle_timeout_s)}s (a tray read, the texts,"
            " the readiness check, a gate call or a verdict never finished); the next"
            " wake retries"
        )
        _log.error("receive loop: %s", failure)
        return self._summary("abandoned", failure=failure)

    def _stale(self, token: int) -> bool:
        return token != self._cycle_token or self._stopped

    def _uncounted_ms(self) -> float:
        extra = self._clock() - self._uncounted_since if self._uncounted_depth > 0 else 0.0
        return self._uncounted_clock + extra

    @contextlib.contextmanager
    def _uncounted(self) -> Iterator[None]:
        """Time the running cycle spends starting a turn, running a run, or
        inside a gate call that has a deadline of its own (a tray gated in k
        chunks may take k deadlines; a cycle making progress is not
        abandoned for it): not counted by the watchdog. Only the cycle's
        own: :meth:`_pump` admits waiting turns as its own task, and an
        admission hanging there must never keep the watchdog from
        abandoning a cycle that hangs."""
        if self._uncounted_depth == 0:
            self._uncounted_since = self._clock()
        self._uncounted_depth += 1
        try:
            yield
        finally:
            self._uncounted_depth -= 1
            if self._uncounted_depth == 0:
                self._uncounted_clock += self._clock() - self._uncounted_since

    async def _read_tray(self) -> dict[str, Any] | None:
        try:
            # The loop shows the tray's invites (invite turns, the single-run
            # invite rule), so it says so to the server.
            tray = await self._client.inbox.tray(wait_ms=0, limit=TRAY_LIMIT, include_invites=True)
        except Exception as err:
            if not self._stopped:
                self._fail(err)
            return None
        data: dict[str, Any] = (
            tray.model_dump(mode="json", exclude_unset=True) if hasattr(tray, "model_dump") else dict(tray)
        )
        you = data.get("you")
        if self._handle is None and isinstance(you, str) and you:
            self._handle = normalize_handle(you)
        return data

    async def _check_ready(self) -> str | None:
        if self._readiness is None:
            return None
        try:
            answer = await self._call(self._readiness)
        except Exception as err:
            return str(err) or type(err).__name__
        if isinstance(answer, Readiness):
            return None if answer.ready else (answer.reason or "not ready")
        if isinstance(answer, bool):
            return None if answer else "not ready"
        ready = getattr(answer, "ready", None)
        if ready is None and isinstance(answer, Mapping):
            ready = answer.get("ready")
        if ready:
            return None
        reason = getattr(answer, "reason", None) or (
            answer.get("reason") if isinstance(answer, Mapping) else None
        )
        return str(reason or "not ready")

    def _held(self, card: Mapping[str, Any]) -> bool:
        """Held out of the gate: a running or waiting turn holds the card's
        conversation, or a verdict for it is still being recorded (an
        abandoned cycle's last call). Gating it again could record the same
        verdict twice (a second defer climbs the ladder)."""
        conversation_id = card["conversation_id"]
        return (
            self._tracker.conversation_running(conversation_id)
            or conversation_id in self._records_in_flight
        )

    async def _gate_call_cycle(self, token: int, cancel: _CallCancel) -> CycleSummary:
        tray = await self._read_tray()
        if self._stale(token):
            return self._summary("abandoned")
        if tray is None:
            return self._summary("tray_failed", failure=self._last_error)
        cards = _gate.cards_of(tray)
        invites = _tray_invites(tray)
        unheld = [card for card in cards if not self._held(card)]
        held = len(cards) - len(unheld)
        if held > 0:
            self._held_while_running = True
        gate_cards = [
            card
            for card in unheld
            if not self._pacer.held(f"conversation:{card['conversation_id']}", _gate.anchor_of(card))
        ]
        open_invites = [i for i in invites if not self._tracker.invite_running(i["invite_id"])]
        owed_invites = [i for i in open_invites if not self._pacer.held(f"invite:{i['invite_id']}", None)]
        paced = (len(unheld) - len(gate_cards)) + (len(open_invites) - len(owed_invites))
        owed_cards = [card for card in gate_cards if card.get("muted") is not True]
        base: dict[str, Any] = {
            "cards": len(cards),
            "invites": len(invites),
            "held_by_turns": held,
            "paced": paced,
            "truncated": tray.get("truncated") is True,
        }
        if not cards and not invites:
            return self._summary("empty", **base)
        if not owed_cards and not owed_invites:
            return self._summary("nothing_owed", **base)

        texts = await self._texts.ensure()
        if self._stale(token):
            return self._summary("abandoned", **base)
        if texts is None:
            return self._summary("texts_missing", failure="the served texts are not available", **base)
        unready = await self._check_ready()
        if self._stale(token):
            return self._summary("abandoned", **base)
        if unready is not None:
            return self._summary("tools_unavailable", failure=unready, **base)

        tally = {
            "gated": 0,
            "reply": 0,
            "dismiss": 0,
            "defer": 0,
            "undecided": 0,
            "record_failures": 0,
            "turns_started": 0,
            "turns_refused": 0,
            "turns_queued": 0,
            "chunks_ok": 0,
            "chunks_failed": 0,
        }
        held_during_gate = 0
        outcome: CycleOutcome = "invites_only"
        failure: str | None = None
        recorded = 0
        if owed_cards:
            gate = await self._gate_in_chunks(token, cancel, tray, gate_cards, texts)
            held_during_gate = gate.held
            recorded = gate.recorded
            tally["gated"] = gate.gated
            for decision, count in gate.verdicts.items():
                tally[decision] = count
            tally["undecided"] = gate.undecided
            tally["record_failures"] = gate.record_failures
            tally["chunks_ok"] = sum(1 for chunk in gate.chunks if chunk["outcome"] == "decided")
            tally["chunks_failed"] = len(gate.chunks) - tally["chunks_ok"]
            failure = _chunk_failure(gate.chunks)
            if tally["chunks_ok"] > 0:
                outcome = "decided"
                # Every chunk has answered and its dismiss / defer verdicts
                # are recorded: only then do the turns start, so a turn never
                # finds on its tray a card the gate has yet to decide.
                for reply in gate.reply:
                    if self._stale(token):
                        break
                    tally[f"turns_{await self._start_reply_turn(reply, texts)}"] += 1
            elif tally["chunks_failed"] > 0:
                outcome = "fail_closed"
            elif not owed_invites and not self._stale(token):
                # Every card was taken by a turn while the gate ran: no call
                # was made.
                return self._summary("nothing_owed", **{**base, "held_by_turns": held + held_during_gate})
        for invite in owed_invites:
            if self._stale(token):
                break
            tally[f"turns_{await self._start_invite_turn_for(invite, texts)}"] += 1
        parts = {**base, "held_by_turns": held + held_during_gate, **tally}
        # Stopped (or abandoned) part-way: what was done is still reported;
        # the reason is the stop itself, not a failure.
        if self._stale(token):
            return self._summary("abandoned", **parts)
        # More conversations wait than one tray shows: read again for the
        # rest — at once after a cycle that made progress, else after a
        # backoff, so a cycle that cannot progress never spins.
        if tray.get("truncated") is True and outcome == "decided":
            self._read_the_rest(recorded > 0 or tally["turns_started"] > 0)
        return self._summary(outcome, failure=failure, **parts)

    async def _gate_in_chunks(
        self,
        token: int,
        cancel: _CallCancel,
        tray: dict[str, Any],
        cards: list[dict[str, Any]],
        texts: Texts,
    ) -> _ChunkedGate:
        """The gate in chunks (contract v1, section 3.6): the cards, in tray
        order, split into chunks of at most ``gate_chunk_size``, one gate
        call per chunk, one after another. Each chunk's prompt is the gate
        prompt for its own cards (N = the cards in that chunk), each call has
        its own deadline and the cycle's cancellation, and each chunk's
        ``dismiss`` / ``defer`` verdicts are recorded as soon as it answers.
        A chunk that fails closed leaves only its own cards undecided.
        Before each call the chunk is checked again for cards a turn has
        taken since the tray was read (a shared tracker), so a longer gate
        never decides a card a turn holds; a chunk left with muted cards
        only is owed nothing and costs no call. Stops once the cycle is
        stale: a call :meth:`stop` cut short is not counted as a failed
        chunk, and a cycle the watchdog abandoned counts and reports no
        further chunk."""
        chunks = _chunked(cards, self._gate_chunk_size)
        out = _ChunkedGate()
        call_model = self._model_call(cancel)
        recording = self._recording_client(token)
        stale = _StaleFlag(lambda: self._stale(token))
        for index, planned in enumerate(chunks, start=1):
            if self._stale(token):
                break
            chunk = [card for card in planned if not self._held(card)]
            if len(chunk) < len(planned):
                out.held += len(planned) - len(chunk)
                self._held_while_running = True
            if not any(card.get("muted") is not True for card in chunk):
                # Nothing owed in it (empty, or muted cards only): no model
                # call, the cycle's own rule applied per chunk. Its muted
                # cards are left as in a nothing_owed cycle.
                continue
            result = await _gate.run_async(
                {**tray, "conversations": chunk},
                call_model,
                recording,
                tool_namer=self._tool_namer,
                texts=texts,
                cancel=stale,
            )
            if token != self._cycle_token:
                break  # abandoned by the watchdog: this cycle reports nothing more
            if self._stopped and result.outcome != "decided":
                break  # cut short by stop(): not this chunk's failure
            out.gated += len(chunk)
            # A stop part-way through the recording leaves the rest undecided
            # (the gate moves them there): each card counts once, as a
            # verdict or as undecided.
            left = set(result.undecided)
            for verdict in result.verdicts:
                if verdict.conversation_id not in left:
                    out.verdicts[verdict.decision] += 1
            out.reply.extend(result.reply)
            out.undecided += len(result.undecided)
            out.recorded += result.recorded
            out.record_failures += len(result.record_failures)
            decided = result.outcome == "decided"
            report: dict[str, Any] = {
                "cycle": token,
                "index": index,
                "of": len(chunks),
                "cards": len(chunk),
                "outcome": "decided" if decided else "fail_closed",
                "undecided": len(result.undecided),
                "failure": result.failure,
            }
            out.chunks.append(report)
            self._counts["chunks_ok" if decided else "chunks_failed"] += 1
            self._emit({"type": "gate_chunk", **report})
        return out

    async def _single_run_cycle(self, token: int) -> CycleSummary:
        tray = await self._read_tray()
        if self._stale(token):
            return self._summary("abandoned")
        cards: int | None = None
        invites: int | None = None
        base: dict[str, Any] = {"cards": 0, "invites": 0, "held_by_turns": 0, "truncated": False}
        if tray is not None:
            all_cards = _gate.cards_of(tray)
            cards = len([card for card in all_cards if card.get("muted") is not True])
            invites = len(_tray_invites(tray))
            base = {
                "cards": len(all_cards),
                "invites": invites,
                "held_by_turns": 0,
                "truncated": tray.get("truncated") is True,
            }
            if not all_cards and invites == 0:
                return self._summary("empty", **base)
            if cards == 0 and invites == 0:
                return self._summary("nothing_owed", **base)
        # A tray that could not be read is not an empty tray, and the wake was
        # real: the run starts anyway.
        texts = await self._texts.ensure()
        if self._stale(token):
            return self._summary("abandoned", **base)
        if texts is None:
            return self._summary("texts_missing", failure="the served texts are not available", **base)
        unready = await self._check_ready()
        if self._stale(token):
            return self._summary("abandoned", **base)
        if unready is not None:
            return self._summary("tools_unavailable", failure=unready, **base)

        wake = texts.wake_instruction.strip()
        invite = texts.invite_prompt.strip()
        if cards == 0 and (invites or 0) > 0:
            prompt = invite
        elif (invites or 0) > 0:
            prompt = f"{wake}\n\n{invite}"
        else:
            prompt = wake
        self._counts["runs_started"] += 1
        request = RunRequest(prompt=prompt, texts=texts, cards=cards, invites=invites, stopping=self._stopping)
        try:
            assert self._start_run is not None
            with self._uncounted():
                raw = await self._call(self._start_run, request)
            end = raw if isinstance(raw, RunEnd) else RunEnd(status="failed", detail=f"start_run returned {type(raw).__name__}")
        except Exception as err:
            end = RunEnd(status="not_started", detail=str(err) or type(err).__name__)
        key = {"completed": "runs_completed", "failed": "runs_failed"}.get(end.status, "runs_not_started")
        self._counts[key] += 1
        self._emit({"type": "run", "end": end})
        # Cards: the run recorded its own verdicts; nothing is recorded for
        # it. Invites sit on the tray until answered, so one the run ignored
        # would cost a run on every wake: the D5 rule of an invite turn —
        # pending after the second completed run → dismissed.
        started_with = (
            _tray_invites(tray) if tray is not None and self._dismiss_ignored_invites else []
        )
        if started_with:
            # Recorded also after stop(): what the run did is a fact.
            await self._track(self._after_run_invites(started_with, end))
        outcome: CycleOutcome = (
            "run_completed"
            if end.status == "completed"
            else "run_failed"
            if end.status == "failed"
            else "run_not_started"
        )
        return self._summary(outcome, failure=None if end.status == "completed" else end.detail, **base)

    # ─── turns (gate-call mode) ──────────────────────────────────────────

    async def _start_reply_turn(
        self, reply: Any, texts: Texts
    ) -> Literal["started", "refused", "queued"]:
        card = reply.card if isinstance(reply.card, dict) else reply.card.model_dump(mode="json", exclude_unset=True)
        anchor = _gate.anchor_of(card)
        handle = self._tracker.begin(TrackedTurn.reply(reply.conversation_id, anchor))
        if handle is None:
            return "refused"
        request = TurnRequest(
            kind="reply",
            prompt=reply_prompt(texts, reply.conversation_id),
            texts=texts,
            stopping=self._stopping,
            conversation_id=reply.conversation_id,
            card=card,
            anchor=anchor,
            category=reply.category,
        )

        async def admit() -> bool:
            assert self._start_turn is not None
            admission = await self._admit(self._start_turn, request)
            if not admission.started or admission.done is None:
                self._tracker.end(handle)
                self._counts["turns_refused"] += 1
                self._emit(
                    {"type": "turn_refused", "kind": "reply", "id": reply.conversation_id, "error": admission.error}
                )
                return False
            self._counts["turns_started"] += 1
            self._emit({"type": "turn_started", "request": request})
            self._spawn_turn(self._after_reply_turn(handle, card, reply.category, admission.done))
            return True

        return await self._launch(handle, admit)

    async def _launch(
        self, handle: TurnHandle, admit: Callable[[], Awaitable[bool]]
    ) -> Literal["started", "refused", "queued"]:
        """Start a turn now when a slot is free, else let it wait for one
        (held out of the gate by its tracker entry meanwhile).

        Waiting turns go first, in order: a freed slot is handed on by
        :meth:`_pump`, which runs as its own task, so a turn gated before it
        runs queues behind them instead of taking the slot. (After a turn
        ends, the TypeScript kit's pump takes the slot synchronously.) A
        refused admission hands its slot on the same way."""
        if self._waiting or len(self._active) >= self._max_turns:
            self._waiting.append((handle, admit))
            self._counts["turns_queued"] += 1
            return "queued"
        self._active.add(handle)
        with self._uncounted():  # the cycle's own admission
            started = await admit()
        if started:
            return "started"
        self._slot_freed(handle)
        return "refused"

    async def _pump(self) -> None:
        """A slot freed: start the turns waiting for one."""
        while self._waiting and len(self._active) < self._max_turns and not self._stopped:
            handle, admit = self._waiting.popleft()
            if not self._tracker.holds(handle):
                continue  # released while it waited: it never starts
            self._active.add(handle)
            if not await admit():
                self._active.discard(handle)

    def _hop(self) -> asyncio.AbstractEventLoop | None:
        """The loop's event loop when called from somewhere else (another
        thread), ``None`` when already on it (or before :meth:`start`)."""
        loop = self._event_loop
        if loop is None:
            return None
        try:
            running: asyncio.AbstractEventLoop | None = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        return None if running is loop else loop

    def _on_release_any_thread(self, handle: TurnHandle) -> None:
        """A tracker release, from whichever thread called it: the slot work
        runs on the loop's own event loop."""
        hop = self._hop()
        if hop is None:
            self._on_turn_released(handle)
            return
        with contextlib.suppress(RuntimeError):  # the event loop has closed
            hop.call_soon_threadsafe(self._on_turn_released, handle)

    def _on_turn_released(self, handle: TurnHandle) -> None:
        waiting = [entry for entry in self._waiting if entry[0] is not handle]
        if len(waiting) != len(self._waiting):
            self._waiting = deque(waiting)
        self._slot_freed(handle)

    def _slot_freed(self, handle: TurnHandle) -> None:
        if handle not in self._active:
            return
        self._active.discard(handle)
        if self._waiting and not self._stopped:
            self._spawn_turn(self._pump())

    async def _after_reply_turn(
        self, handle: TurnHandle, card: dict[str, Any], category: str | None, done: Awaitable[TurnEnd]
    ) -> None:
        try:
            end = await self._tracker.confirmed_end(handle, done)
            if end is None or not self._tracker.holds(handle):
                return  # released: nothing recorded
            # Recorded also when the turn ends after stop(): what it did is
            # a fact either way.
            await self._track(self._record_reply_outcome(handle, card, category, end))
        finally:
            self._tracker.end(handle)
            self._slot_freed(handle)
            self._after_turn_ended()

    async def _record_reply_outcome(
        self, handle: TurnHandle, card: dict[str, Any], category: str | None, end: TurnEnd
    ) -> None:
        fresh = await self._read_tray()
        if not self._tracker.holds(handle):
            return  # released while the tray was read: nothing recorded
        conversation_id = str(card.get("conversation_id"))
        key = f"{conversation_id}|{_gate.anchor_of(card) or ''}"
        silent_before = self._silent.get(key, 0)
        outcome = turn_outcome(
            turn=end,
            turn_started_at=handle.started_at,
            gate_card=card,
            category=category,
            fresh_tray=fresh,
            silent_before=silent_before,
        )
        self._count(outcome.reason)
        if outcome.reason == "silent_first":
            self._remember(key, silent_before + 1)
            # D5 pacing: not gated again at this anchor for a while.
            self._pacer.hold(f"conversation:{conversation_id}", _gate.anchor_of(card))
        elif outcome.reason in ("card_left_tray", "turn_recorded_verdict"):
            self._silent.pop(key, None)
        recorded = False
        if outcome.action != "none":
            try:
                recorded = await apply_outcome_async(self._client, outcome)
                self._silent.pop(key, None)
            except Exception as err:
                self._counts["outcome_record_failures"] += 1
                self._fail(err)
        self._emit({"type": "turn_outcome", "outcome": outcome, "recorded": recorded})

    async def _start_invite_turn_for(
        self, invite: dict[str, Any], texts: Texts
    ) -> Literal["started", "refused", "queued"]:
        handle = self._tracker.begin(TrackedTurn.invite(invite["invite_id"]))
        if handle is None:
            return "refused"
        request = TurnRequest(
            kind="invite",
            prompt=f"{texts.invite_prompt.strip()}\n\ninvite_id: {invite['invite_id']}",
            texts=texts,
            stopping=self._stopping,
            invite=invite,
        )

        async def admit() -> bool:
            assert self._start_invite_turn is not None
            admission = await self._admit(self._start_invite_turn, request)
            if not admission.started or admission.done is None:
                self._tracker.end(handle)
                self._counts["turns_refused"] += 1
                self._emit(
                    {"type": "turn_refused", "kind": "invite", "id": invite["invite_id"], "error": admission.error}
                )
                return False
            self._counts["invite_turns_started"] += 1
            self._emit({"type": "turn_started", "request": request})
            self._spawn_turn(self._after_invite_turn(handle, invite, admission.done))
            return True

        return await self._launch(handle, admit)

    async def _after_invite_turn(
        self, handle: TurnHandle, invite: dict[str, Any], done: Awaitable[TurnEnd]
    ) -> None:
        try:
            end = await self._tracker.confirmed_end(handle, done)
            if end is None or not self._tracker.holds(handle):
                return  # released: nothing recorded
            await self._track(self._record_invite_outcome(invite, end, handle))
            # D5 pacing: no second invite turn for it for a while (an
            # answered invite has left the tray anyway).
            self._pacer.hold(f"invite:{invite['invite_id']}", None)
        finally:
            self._tracker.end(handle)
            self._slot_freed(handle)
            self._after_turn_ended()

    async def _after_run_invites(self, invites: list[dict[str, Any]], end: RunEnd) -> None:
        completed = end.status == "completed"
        fresh = await self._read_tray() if completed else None
        turn = (
            TurnEnd(status="completed", agentchat_calls=None)
            if completed
            else TurnEnd(status="failed", detail=end.detail)
        )
        for invite in invites:
            # A run reports no call counts: the single-run invite rule
            # applies, paced.
            await self._finish_invite(invite, turn, fresh, require_known_calls=False, paced=True)

    async def _record_invite_outcome(
        self, invite: dict[str, Any], end: TurnEnd, handle: TurnHandle
    ) -> None:
        fresh = await self._read_tray()
        if not self._tracker.holds(handle):
            return  # released while the tray was read: nothing recorded
        await self._finish_invite(invite, end, fresh)

    async def _finish_invite(
        self,
        invite: dict[str, Any],
        end: TurnEnd,
        fresh: dict[str, Any] | None,
        *,
        require_known_calls: bool = True,
        paced: bool = False,
    ) -> None:
        """The D5 rule for one invite after a turn or a run, and the one call
        it asks for."""
        key = f"invite|{invite['invite_id']}"
        silent_before = self._silent.get(key, 0)
        outcome = invite_outcome(
            turn=end,
            invite=invite,
            fresh_tray=fresh,
            silent_before=silent_before,
            require_known_calls=require_known_calls,
        )
        # D5 paces both modes: in single-run mode the run that would dismiss
        # counts only when it ended at least pace_ms after the first counted
        # one.
        counted_at = self._silent_at.get(key)
        if (
            paced
            and outcome.reason == "silent_again"
            and counted_at is not None
            and self._now() - counted_at < self._pace_ms
        ):
            outcome = InviteOutcome(action="none", reason="silent_paced", invite_id=outcome.invite_id)
        self._count(f"invite_{outcome.reason}")
        if outcome.reason == "silent_first":
            self._remember(key, silent_before + 1)
            self._silent_at[key] = self._now()
        elif outcome.reason == "invite_answered":
            self._silent.pop(key, None)
            self._silent_at.pop(key, None)
        recorded = False
        if outcome.action != "none":
            try:
                recorded = await apply_invite_outcome_async(self._client, outcome)
                self._silent.pop(key, None)
                self._silent_at.pop(key, None)
            except Exception as err:
                self._counts["outcome_record_failures"] += 1
                self._fail(err)
        self._emit({"type": "invite_outcome", "outcome": outcome, "recorded": recorded})

    async def _admit(self, start: TurnStarter, request: TurnRequest) -> TurnAdmission:
        """Ask the runtime to start a turn. A coroutine starter returns a
        :class:`TurnAdmission` (or a :class:`TurnEnd`); a plain one runs the
        whole turn in a worker thread and returns its :class:`TurnEnd`."""
        try:
            if _is_async(start):
                answer = await start(request)
            else:
                return TurnAdmission(started=True, done=asyncio.ensure_future(self._run_sync_turn(start, request)))
        except Exception as err:
            return TurnAdmission(started=False, error=str(err) or type(err).__name__)
        if isinstance(answer, TurnAdmission):
            return answer
        if isinstance(answer, TurnEnd):
            return TurnAdmission(started=True, done=_resolved(answer))
        return TurnAdmission(started=False, error=f"start_turn returned {type(answer).__name__}")

    async def _run_sync_turn(self, start: TurnStarter, request: TurnRequest) -> TurnEnd:
        try:
            end = await asyncio.to_thread(start, request)
        except Exception as err:
            return TurnEnd(status="failed", detail=str(err) or type(err).__name__)
        if isinstance(end, TurnEnd):
            return end
        if isinstance(end, TurnAdmission):
            if not end.started or end.done is None:
                return TurnEnd(status="failed", detail=end.error or "the turn did not start")
            done = end.done
            return await done if inspect.isawaitable(done) else done
        return TurnEnd(status="failed", detail=f"start_turn returned {type(end).__name__}")

    def _read_the_rest(self, progressed: bool) -> None:
        """A truncated tray: read again at once after progress (a verdict
        recorded, a turn started); otherwise after 1 s, doubling to 60 s."""
        if progressed:
            self._no_progress_streak = 0
            self._clear_more_timer()
            self._wake_from("more")
            return
        self._no_progress_streak += 1
        if self._more_timer is not None:
            return
        delay = min(
            _NO_PROGRESS_BACKOFF_MAX_MS,
            _NO_PROGRESS_BACKOFF_MS * 2 ** min(self._no_progress_streak - 1, 16),
        )

        def fire() -> None:
            self._more_timer = None
            self._wake_from("more")

        self._more_timer = self._set_timer(delay, fire)

    def _clear_more_timer(self) -> None:
        if self._more_timer is not None:
            self._more_timer.cancel()
            self._more_timer = None

    def _after_turn_ended(self) -> None:
        """A turn's end: cards a cycle left out meanwhile are read again."""
        if self._held_while_running and not self._stopped:
            self._held_while_running = False
            self._wake_from("more")

    # ─── small things ────────────────────────────────────────────────────

    def _recording_client(self, token: int) -> Any:
        """The gate's view of the client: a verdict in flight is known until
        it answers (its card is held out of the next gate). It is never
        aborted with an abandoned cycle (shielded): the server may already
        have taken it, and a second one could climb the defer ladder twice."""
        loop = self
        decisions = self._client.decisions

        async def record(conversation_id: str, **kwargs: Any) -> Any:
            loop._records_in_flight.add(conversation_id)
            call = asyncio.ensure_future(decisions.record(conversation_id, **kwargs))

            def done(_task: asyncio.Future[Any]) -> None:
                loop._records_in_flight.discard(conversation_id)
                if loop._stale(token):
                    loop._after_turn_ended()  # a card held for it is read again

            call.add_done_callback(done)
            call.add_done_callback(_consume_outcome)
            return await asyncio.shield(call)

        return SimpleNamespace(decisions=SimpleNamespace(record=record))

    def _model_call(self, cycle_cancel: _CallCancel) -> Callable[[str], Any]:
        """The runtime's model call for one gate call (one chunk), bounded by
        ``gate_call_timeout_ms`` (and by the cycle's abandonment or the
        loop's stop). A ``call_model`` that takes a ``cancel`` keyword is
        handed this call's own cancel handle, set when its answer is no
        longer wanted (its deadline passed, the cycle was abandoned, the
        loop stopped): an :class:`asyncio.Event` for a coroutine function, a
        :class:`threading.Event` for a plain function (which runs in a
        worker thread; the cycle simply stops waiting for it). A chunk past
        its deadline tells only its own call.

        While a call has a deadline, its time does not count against the
        cycle's (a tray gated in k chunks may take k deadlines, and a cycle
        that is making progress is not abandoned for it); with the deadline
        off the watchdog is the calls' only bound, so they count."""
        assert self._call_model is not None
        call: Callable[..., Any] = self._call_model
        is_async = _is_async(call)
        takes_cancel = _accepts_keyword(call, "cancel")
        timeout_ms = self._gate_call_timeout_ms

        async def bounded(prompt: str) -> Any:
            handles = cycle_cancel.child()
            uncounted: contextlib.AbstractContextManager[None] = (
                self._uncounted() if timeout_ms > 0 else contextlib.nullcontext()
            )
            try:
                with uncounted:
                    if is_async:
                        pending: Any = call(prompt, cancel=handles.on_loop) if takes_cancel else call(prompt)
                    elif takes_cancel:
                        pending = asyncio.to_thread(functools.partial(call, prompt, cancel=handles.in_thread))
                    else:
                        pending = asyncio.to_thread(call, prompt)
                    if timeout_ms <= 0:
                        return await pending
                    return await self._within(pending, timeout_ms, handles)
            finally:
                cycle_cancel.release(handles)

        return bounded

    async def _within(self, pending: Awaitable[Any], timeout_ms: float, handles: _CallCancel) -> Any:
        """One gate call bounded by its deadline, on the loop's timer
        (``set_timer``). Past it the call's cancel handle is set, the call is
        cancelled and no longer waited for (a plain function in a worker
        thread is left to finish, unheard), and :class:`asyncio.TimeoutError`
        is raised. Cancelled from outside (the loop stopped, the cycle
        abandoned), the call is cancelled too."""
        task = asyncio.ensure_future(pending)
        deadline: asyncio.Future[None] = asyncio.get_running_loop().create_future()

        def expire() -> None:
            if not deadline.done():
                deadline.set_result(None)

        timer = self._set_timer(timeout_ms, expire)
        either: set[asyncio.Future[Any]] = {task, deadline}
        try:
            await asyncio.wait(either, return_when=asyncio.FIRST_COMPLETED)
        except BaseException:
            task.cancel()
            task.add_done_callback(_consume_outcome)
            raise
        finally:
            timer.cancel()
        if task.done():
            return task.result()
        handles.set()
        task.cancel()
        task.add_done_callback(_consume_outcome)
        raise asyncio.TimeoutError(f"no answer within {_number_text(timeout_ms)} ms")

    async def _call(self, fn: Callable[..., Any], *args: Any) -> Any:
        if _is_async(fn):
            return await fn(*args)
        result = await asyncio.to_thread(fn, *args)
        return await result if inspect.isawaitable(result) else result

    def _spawn_turn(self, coro: Any) -> None:
        task = asyncio.ensure_future(coro)
        self._turn_tasks.add(task)
        task.add_done_callback(self._turn_tasks.discard)
        task.add_done_callback(self._log_task_failure)

    async def _track(self, coro: Any) -> None:
        """Recording under way after a turn's confirmed end: :meth:`stop`
        waits for it."""
        task = asyncio.ensure_future(coro)
        self._pending_work.add(task)
        try:
            await task
        finally:
            self._pending_work.discard(task)

    def _log_task_failure(self, task: asyncio.Future[Any]) -> None:
        if not task.cancelled() and task.exception() is not None:
            self._fail(task.exception())  # type: ignore[arg-type]

    def _remember(self, key: str, count: int) -> None:
        self._silent.pop(key, None)
        self._silent[key] = count
        while len(self._silent) > _SILENT_MEMORY:
            oldest, _ = self._silent.popitem(last=False)
            self._silent_at.pop(oldest, None)

    def _count(self, reason: str) -> None:
        self._outcomes[reason] = self._outcomes.get(reason, 0) + 1

    def _summary(self, outcome: CycleOutcome, **parts: Any) -> CycleSummary:
        return CycleSummary(at=_iso(self._now()), outcome=outcome, **parts)

    def _fail(self, err: BaseException) -> None:
        self._last_error = str(err) or type(err).__name__
        self._emit({"type": "error", "error": err})

    def _emit(self, event: ReceiveLoopEvent) -> None:
        if self._on_event is None:
            if event.get("type") == "error":
                _log.warning("receive loop: %s", event.get("error"))
            return
        try:
            self._on_event(event)
        except Exception:
            _log.debug("receive loop: on_event raised", exc_info=True)


def _accepts_keyword(fn: Any, name: str) -> bool:
    """Whether ``fn`` takes the keyword ``name`` (or ``**kwargs``)."""
    try:
        params = inspect.signature(fn).parameters.values()
    except (TypeError, ValueError):
        return False
    return any(
        p.kind is inspect.Parameter.VAR_KEYWORD
        or (p.name == name and p.kind is not inspect.Parameter.POSITIONAL_ONLY)
        for p in params
    )


def _is_async(fn: Any) -> bool:
    """A coroutine function, or an object whose ``__call__`` is one."""
    if inspect.iscoroutinefunction(fn):
        return True
    return callable(fn) and inspect.iscoroutinefunction(type(fn).__call__)


async def _resolved_value(value: TurnEnd) -> TurnEnd:
    return value


def _resolved(value: TurnEnd) -> Awaitable[TurnEnd]:
    return _resolved_value(value)


__all__ = [
    "GATE_CHUNK_SIZE",
    "TRAY_LIMIT",
    "CycleOutcome",
    "CycleSummary",
    "Readiness",
    "ReceiveLoop",
    "ReceiveLoopEvent",
    "ReceiveLoopMode",
    "ReceiveLoopSocketOptions",
    "ReceiveLoopStatus",
    "RunEnd",
    "RunRequest",
    "RunStarter",
    "TurnAdmission",
    "TurnRequest",
    "TurnStarter",
    "WakeSource",
    "reply_prompt",
]
