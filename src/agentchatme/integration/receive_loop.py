"""The receive loop: the socket as a wake, one cycle per burst, the tray, the
gate call or the run, and what is recorded afterwards. Optional — every part
it uses is exported on its own. The TypeScript kit's ``ReceiveLoop`` is the
twin.

Two modes (contract v1, D5):

- ``gate_call`` (Hermes, OpenClaw): read the tray, ask the agent's own model
  the served gate question in a tool-less call (``call_model``), record
  ``dismiss`` / ``defer`` at once, start one turn per ``reply``
  (``start_turn``), and when a turn has ended record from what actually
  happened (:func:`~agentchatme.integration.turn_outcome`). Each pending
  invite on the tray gets one invite turn (``start_invite_turn``, default
  ``start_turn``) and the same rule.
- ``single_run`` (Claude Code, Codex, routines, a listener): when anything is
  owed, start one run (``start_run``) with the served wake text; the run
  reads the tray itself and records its own verdicts. The loop records
  nothing for cards. An invite still pending and undismissed after two
  completed runs is dismissed (seen, left pending, not shown again), so an
  ignored invite does not cost a run on every wake; the count is in memory
  only.

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
it within ``pace_ms`` unless an ``unanswered`` wake arrives. In memory.

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
import inspect
import logging
import time
from collections import OrderedDict, deque
from collections.abc import Awaitable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Literal

from .. import gate as _gate
from .._client_identity import AgentChatClientIdentity
from .._realtime import RealtimeClient
from ..types.texts import Texts
from .anchor import normalize_handle
from .coalescer import SetTimer, WakeCoalescer
from .outcome import (
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
]


@dataclass(frozen=True)
class TurnRequest:
    """One turn the loop asks the runtime to start (gate-call mode)."""

    kind: Literal["reply", "invite"]
    prompt: str
    """``reply``: the served wake instruction and the conversation id;
    ``invite``: the served invite prompt and the invite id."""
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
    turns: list[RunningTurn]
    counters: dict[str, Any]
    last_error: str | None


ReceiveLoopEvent = dict[str, Any]
"""``{"type": "wake" | "echo" | "cycle" | "turn_started" | "turn_refused" |
"turn_outcome" | "invite_outcome" | "run" | "error", ...}``."""

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


def reply_prompt(texts: Any, conversation_id: str) -> str:
    """The prompt a reply turn starts with: the served wake instruction, then
    the conversation it is for."""
    instruction = texts.get("wake_instruction") if isinstance(texts, Mapping) else texts.wake_instruction
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
    tracks itself are kept out of the gate too).
    """

    def __init__(
        self,
        client: Any,
        texts: Any,
        mode: ReceiveLoopMode,
        *,
        call_model: Callable[[str], Any] | None = None,
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
        turn_tracker: TurnTracker | None = None,
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
        self._handle = normalize_handle(handle) if handle else None
        self._tracker = turn_tracker if turn_tracker is not None else TurnTracker(now=self._now)
        self._pacer = _Pacer(pace_ms, self._now)
        self._max_turns = max_concurrent_turns
        self._active_turns = 0
        # Turns waiting for a slot: (their tracker handle, the admission).
        self._waiting: deque[tuple[TurnHandle, Callable[[], Awaitable[bool]]]] = deque()
        coalescer_options: dict[str, Any] = {}
        if quiet_ms is not None:
            coalescer_options["quiet_ms"] = quiet_ms
        if max_ms is not None:
            coalescer_options["max_ms"] = max_ms
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
        self._pending_work: set[asyncio.Future[Any]] = set()
        self._turn_tasks: set[asyncio.Future[Any]] = set()
        self._started = False
        self._stopped = False
        self._held_while_running = False
        self._last_cycle: CycleSummary | None = None
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
        its own). Call it on the loop's thread."""
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
        self._coalescer.stop()
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
        await self._coalescer.settled()
        if self._pending_work:
            await asyncio.gather(*list(self._pending_work), return_exceptions=True)

    async def settled(self) -> None:
        """Return when the running cycle has finished (tests, graceful stops)."""
        await self._coalescer.settled()

    def release_turn(self, key: str) -> bool:
        """Let go of a turn whose end the runtime will never confirm (an
        operator's call). ``key`` is ``conversation:<id>`` or
        ``invite:<id>``."""
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
            turns=self._tracker.running(),
            counters={
                **self._counts,
                "turns_active": self._active_turns,
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
        summary = (
            await self._gate_call_cycle() if self._mode == "gate_call" else await self._single_run_cycle()
        )
        self._last_cycle = summary
        if summary.failure is not None:
            self._last_error = summary.failure
        self._emit({"type": "cycle", "summary": summary})

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

    async def _gate_call_cycle(self) -> CycleSummary:
        tray = await self._read_tray()
        if tray is None:
            return self._summary("tray_failed", failure=self._last_error)
        cards = _gate.cards_of(tray)
        invites = _tray_invites(tray)
        unheld = self._tracker.without_running(cards)
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
        if texts is None:
            return self._summary("texts_missing", failure="the served texts are not available", **base)
        unready = await self._check_ready()
        if unready is not None:
            return self._summary("tools_unavailable", failure=unready, **base)

        gated = 0
        tally = {
            "reply": 0,
            "dismiss": 0,
            "defer": 0,
            "undecided": 0,
            "record_failures": 0,
            "turns_started": 0,
            "turns_refused": 0,
            "turns_queued": 0,
        }
        outcome: CycleOutcome = "invites_only"
        failure: str | None = None
        if owed_cards:
            gated = len(gate_cards)
            result = await _gate.run_async(
                {**tray, "conversations": gate_cards},
                self._model_call(),
                self._client,
                tool_namer=self._tool_namer,
                texts=texts,
            )
            for verdict in result.verdicts:
                tally[verdict.decision] += 1
            tally["undecided"] = len(result.undecided)
            tally["record_failures"] = len(result.record_failures)
            if result.outcome == "fail_closed":
                outcome = "fail_closed"
                failure = result.failure
            else:
                outcome = "decided"
                for reply in result.reply:
                    if self._stopped:
                        break
                    tally[f"turns_{await self._start_reply_turn(reply, texts)}"] += 1
        for invite in owed_invites:
            if self._stopped:
                break
            tally[f"turns_{await self._start_invite_turn_for(invite, texts)}"] += 1
        # More conversations wait than one tray shows: read again for the rest.
        if tray.get("truncated") is True and outcome == "decided":
            self._wake_from("more")
        return self._summary(outcome, gated=gated, failure=failure, **base, **tally)

    async def _single_run_cycle(self) -> CycleSummary:
        tray = await self._read_tray()
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
        if texts is None:
            return self._summary("texts_missing", failure="the served texts are not available", **base)
        unready = await self._check_ready()
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
        started_with = _tray_invites(tray) if tray is not None else []
        if started_with and not self._stopped:
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
        (held out of the gate by its tracker entry meanwhile)."""
        if self._active_turns >= self._max_turns:
            self._waiting.append((handle, admit))
            self._counts["turns_queued"] += 1
            return "queued"
        self._active_turns += 1
        if await admit():
            return "started"
        self._active_turns -= 1
        return "refused"

    async def _pump(self) -> None:
        """A slot freed: start the turns waiting for one."""
        while self._waiting and self._active_turns < self._max_turns and not self._stopped:
            _handle, admit = self._waiting.popleft()
            self._active_turns += 1
            if not await admit():
                self._active_turns -= 1

    def _slot_freed(self) -> None:
        self._active_turns = max(0, self._active_turns - 1)
        if self._waiting and not self._stopped:
            self._spawn_turn(self._pump())

    async def _after_reply_turn(
        self, handle: TurnHandle, card: dict[str, Any], category: str | None, done: Awaitable[TurnEnd]
    ) -> None:
        try:
            end = await self._tracker.confirmed_end(handle, done)
            if end is None:
                return  # released: nothing recorded
            # Recorded also when the turn ends after stop(): what it did is
            # a fact either way.
            await self._track(self._record_reply_outcome(handle, card, category, end))
        finally:
            self._tracker.end(handle)
            self._slot_freed()
            self._after_turn_ended()

    async def _record_reply_outcome(
        self, handle: TurnHandle, card: dict[str, Any], category: str | None, end: TurnEnd
    ) -> None:
        fresh = await self._read_tray()
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
            if end is None:
                return
            await self._track(self._record_invite_outcome(invite, end))
            # D5 pacing: no second invite turn for it for a while (an
            # answered invite has left the tray anyway).
            self._pacer.hold(f"invite:{invite['invite_id']}", None)
        finally:
            self._tracker.end(handle)
            self._slot_freed()
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
            await self._settle_invite(invite, turn, fresh)

    async def _record_invite_outcome(self, invite: dict[str, Any], end: TurnEnd) -> None:
        await self._settle_invite(invite, end, await self._read_tray())

    async def _settle_invite(
        self, invite: dict[str, Any], end: TurnEnd, fresh: dict[str, Any] | None
    ) -> None:
        """The D5 rule for one invite after a turn or a run, and the one call
        it asks for."""
        key = f"invite|{invite['invite_id']}"
        silent_before = self._silent.get(key, 0)
        outcome = invite_outcome(turn=end, invite=invite, fresh_tray=fresh, silent_before=silent_before)
        self._count(f"invite_{outcome.reason}")
        if outcome.reason == "silent_first":
            self._remember(key, silent_before + 1)
        elif outcome.reason == "invite_answered":
            self._silent.pop(key, None)
        recorded = False
        if outcome.action != "none":
            try:
                recorded = await apply_invite_outcome_async(self._client, outcome)
                self._silent.pop(key, None)
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

    def _after_turn_ended(self) -> None:
        """A turn's end: cards a cycle left out meanwhile are read again."""
        if self._held_while_running and not self._stopped:
            self._held_while_running = False
            self._wake_from("more")

    # ─── small things ────────────────────────────────────────────────────

    def _model_call(self) -> Callable[[str], Any]:
        call = self._call_model
        assert call is not None
        if _is_async(call):
            return call

        async def in_thread(prompt: str) -> Any:
            return await asyncio.to_thread(call, prompt)

        return in_thread

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
            self._silent.popitem(last=False)

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
