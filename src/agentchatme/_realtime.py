"""Async realtime WebSocket client.

Mirrors the TypeScript ``RealtimeClient`` behavior:

- HELLO handshake with a 15 s deadline (authenticates over wire, never URL)
- Capability-negotiated delivery acks (``ack``): at-least-once delivery
  with per-message confirmation after handler dispatch, or on receipt
  with ``ack_on_receipt=True`` (delivered = this machine received it)
- Bounded message-id dedup across the live and offline-drain paths
- Per-conversation seq ordering with out-of-order buffering
- Gap detection with optional ``AsyncAgentChatClient``-backed recovery
- ``wake`` frames (a signal with no message text: read the tray), including
  the server's unanswered re-wake (``rewake`` capability, opt-in)
- ``heartbeat`` frames (capability ``heartbeat``): the server's 45 s timer as
  an inbound frame, so the idle watchdog works on any transport
- Handlers run off the receive loop: a slow handler never holds up frame
  reads, acks of other frames, or the transport's pongs
- A connect budget answer (close 1013 with ``retry_after``) is backoff with
  the server's delay, never ``blocked``; any other 1013, and an upgrade
  answered with an HTTP status, is the ordinary backoff
- Jittered exponential reconnect that never gives up, a ``blocked`` state
  with a slow retry after an auth rejection, handshake + liveness watchdogs,
  disposed flag, offline ``/sync`` drain speaking the bare-array wire

The client is async-only because Python's WebSocket story is asyncio-native.
Pair it with an :class:`~agentchat.AsyncAgentChatClient` for gap recovery and
auto-drain on reconnect.

Connection rules (shared with the TypeScript SDK; they came from the
OpenClaw plugin's socket, which Hermes and OpenClaw now get through here):

1. The client has no dead state except an explicit :meth:`disconnect`.
   There is no reconnect cap: backoff grows exponentially with jitter, and
   after ``_LONG_OUTAGE_AFTER_ATTEMPTS`` consecutive failures the cap widens
   to about five minutes and one error line is logged.
2. A close the client itself initiated (handshake timeout, liveness timeout, its
   own stop) is never evidence about the server: normal backoff.
3. A server close is an auth rejection only when it is ``4401``/``4403``, or
   ``1008`` with the server's auth reason (``Invalid token``), during the
   handshake. Any other ``1008`` (``HELLO frame required`` from the server's
   own 5 s timer, an eviction after key rotation or suspension) is transient.
4. An auth rejection does not end the client: it enters ``blocked`` (exposed
   as :attr:`RealtimeClient.state` with :attr:`RealtimeClient.blocked_reason`)
   and retries a fresh HELLO every 10-15 minutes until a ``hello.ok`` clears it.
5. Liveness: ``hello.ok`` must arrive within ``handshake_timeout_ms`` of the
   socket opening, and a ready socket must show an inbound frame or answer
   a ping within ``liveness_timeout_ms``; a tripped watchdog closes the
   socket as client-initiated (rule 2) and reconnects.

Guest mode: the client keeps no process-wide state. Several instances run
side by side in one loop, and :meth:`RealtimeClient.disconnect` releases
every task, timer and socket the instance owns, so a host process (an agent
runtime embedding the SDK) can start and stop it cleanly.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import logging
import random
import time
import warnings
from collections import OrderedDict, deque
from collections.abc import Awaitable, Coroutine
from dataclasses import dataclass, field, replace
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Literal,
    TypedDict,
    Union,
)

from ._client import last_sync_delivery_id
from ._client_identity import DEFAULT_CLIENT_IDENTITY, AgentChatClientIdentity
from ._connect_budget import connect_budget_delay_ms
from .errors import ConnectionError as _RealtimeConnectionError

if TYPE_CHECKING:
    from ._client import AsyncAgentChatClient, SyncRow


# ───────────────────────── Public type aliases ─────────────────────────

MessageHandler = Callable[[dict[str, Any]], Union[Awaitable[None], None]]
"""Handler signature for WS frames. Receives the decoded JSON dict."""


ErrorHandler = Callable[[BaseException], Union[Awaitable[None], None]]
"""Handler fired on every socket-level or protocol error."""

ConnectHandler = Callable[[], Union[Awaitable[None], None]]
"""Handler fired once per successful ``hello.ok`` (initial + every reconnect)."""

DisconnectHandler = Callable[[dict[str, Any]], Union[Awaitable[None], None]]
"""Handler fired on every close. Receives ``{code, reason, was_clean}``."""

RealtimeState = Literal["idle", "connecting", "ready", "backoff", "blocked", "stopped"]
"""Where :class:`RealtimeClient` is in its connection lifecycle. Same names
as the TypeScript SDK.

``idle``
    No socket and nothing scheduled: before the first
    :meth:`RealtimeClient.connect`, or after a close with ``reconnect=False``.
``connecting``
    The transport is opening, or open with HELLO sent and ``hello.ok``
    awaited under the handshake watchdog.
``ready``
    ``hello.ok`` received; frames flow and the liveness watchdog runs.
``backoff``
    The socket closed or failed to open for a transient reason; a jittered
    exponential backoff timer is pending. There is no attempt cap.
``blocked``
    The server rejected the credential during the handshake
    (:attr:`RealtimeClient.blocked_reason` says how); a fresh HELLO is
    retried every 10-15 minutes until one is accepted.
``stopped``
    :meth:`RealtimeClient.disconnect` ran. The only terminal state.
"""

GapReason = Literal[
    "gap_filled",
    "gap_fill_failed",
    "gap_fill_unavailable",
    "buffer_overflow",
    # ack_on_receipt only: the socket closed while frames sat in the gap
    # buffer. They were already acknowledged, so they were handed to the
    # handlers in seq order instead of being dropped (see the option).
    "connection_closed",
]


@dataclass
class SequenceGapInfo:
    """Describes a detected per-conversation seq gap and its resolution."""

    conversation_id: str
    expected_seq: int
    buffered_seq: int | None
    gap_ms: int
    recovered: bool
    reason: GapReason


SequenceGapHandler = Callable[[SequenceGapInfo], Union[Awaitable[None], None]]


# Default bound on the message-id dedup LRU shared by the live and drain
# paths. At-least-once delivery makes duplicates a design feature; 2048 ids
# comfortably covers a reconnect/drain burst without unbounded growth.
_DEFAULT_DEDUP_CACHE_SIZE = 2048

# Handshake deadline: socket open → ``hello.ok``. The server runs its own
# 5 s HELLO timer and closes a slow handshake with 1008 'HELLO frame
# required'; that close is transient (see _is_auth_rejection_close), so there
# is no need to beat it. This watchdog covers a server that neither answers
# nor closes. A trip closes the socket as client-initiated: normal backoff.
_DEFAULT_HANDSHAKE_TIMEOUT_MS = 15_000

# Liveness deadline for a ready socket: this long without any inbound frame,
# the client probes with a ping; a pong within _LIVENESS_PROBE_TIMEOUT_S
# counts as liveness, anything else trips the watchdog (client-initiated
# close → normal backoff). The ``websockets`` library answers the server's
# pings and runs its own keepalive (every 20 s, see _keepalive_kwargs)
# without exposing either, so inbound frames plus an explicit probe are what
# we can count. 100 s is more than two server heartbeat cycles (the server
# pings every 45 s with a 30 s pong deadline, starting after hello.ok). With
# the real ``websockets`` connection its keepalive trips first on a dead
# path (20 s + 20 s) and surfaces as a close code 1011/1006 that takes the
# ordinary backoff; this watchdog is the backstop for a socket that keeps
# answering pings at the transport yet delivers nothing, and for injected
# transports without a keepalive.
_DEFAULT_LIVENESS_TIMEOUT_MS = 100_000


def _keepalive_kwargs(connect_fn: Any) -> dict[str, float]:
    """Pin the client keepalive instead of inheriting library defaults.

    ``websockets`` defaults to ping every 20s / pong deadline 20s, which
    has served fine — but inheriting it silently means a library upgrade
    could change our wire behaviour without a line of our code changing,
    and nobody reading this file could tell what the contract is. The
    server pings independently at 45s with a 30s pong deadline; the two
    regimes are deliberately separate, each detecting death on its own
    side.

    Skipped entirely for injected/test connect functions, which are plain
    stubs that don't accept these kwargs.
    """
    if connect_fn is not _resolved_ws_connect():
        return {}
    return {"ping_interval": 20.0, "ping_timeout": 20.0}


def _resolved_ws_connect() -> Any:
    """The real ``websockets`` connect callable, or ``None`` if absent."""
    try:
        from websockets.asyncio.client import connect as _ws_connect

        return _ws_connect
    except ImportError:
        pass
    try:
        from websockets import connect as _ws_connect_legacy

        return _ws_connect_legacy
    except ImportError:
        return None


class SocketCapabilityOptions(TypedDict, total=False):
    """Which optional socket capabilities to offer (see
    :attr:`RealtimeOptions.capabilities`)."""

    heartbeat: bool
    rewake: bool


@dataclass
class RealtimeOptions:
    """Configuration for :class:`RealtimeClient`."""

    api_key: str
    base_url: str = "wss://api.agentchat.me"
    reconnect: bool = True
    reconnect_interval_ms: int = 500
    max_reconnect_interval_ms: int = 30_000
    """Cap on the backoff between reconnects. After
    ``_LONG_OUTAGE_AFTER_ATTEMPTS`` consecutive failures the cap widens to at
    least five minutes; the client keeps retrying either way."""
    max_reconnect_attempts: int | None = None
    """Deprecated and ignored: the client never stops reconnecting on its
    own (only :meth:`RealtimeClient.disconnect` ends it). Passing a value
    emits a :class:`DeprecationWarning`. Kept so existing calls still
    construct."""
    client: AsyncAgentChatClient | None = None
    on_sequence_gap: SequenceGapHandler | None = None
    auto_drain_on_connect: bool | None = None  # None → True iff client set
    dedup_cache_size: int = _DEFAULT_DEDUP_CACHE_SIZE
    """Bound on the message-id dedup LRU (live + drain). ``0`` disables dedup."""
    client_identity: AgentChatClientIdentity = DEFAULT_CLIENT_IDENTITY
    handshake_timeout_ms: int = _DEFAULT_HANDSHAKE_TIMEOUT_MS
    """Socket open → ``hello.ok`` deadline. A trip is a client-initiated
    close: normal backoff, never ``blocked``."""
    liveness_timeout_ms: int | None = None
    """How long a ready socket may go without an inbound frame before the
    client probes it with a ping (a pong within 10 s keeps it). ``None``
    (default) means 100 s on a socket that exposes ``ping()`` — the
    ``websockets`` connection does — and no watchdog on one that does not,
    since frames alone cannot tell a quiet conversation from a dead peer.
    ``0`` disables it. An explicit value arms it on any socket."""
    ack_on_receipt: bool = False
    """Acknowledge each ``message.new`` the moment its frame is parsed,
    before any handler runs. This is the integration setting: "delivered"
    then means the agent's machine received the message, and whether the
    agent has looked is tracked separately as read state (a verdict or
    ``client.conversations.mark_read``), so an early ack never empties the
    tray. Covers live frames, the server's reconnect backlog and gap-fill
    rows; the REST offline drain keeps its settle-then-ack cursor. Default
    ``False``: ack after every handler settled, for app clients whose
    handler IS the processing. Only applies when the server negotiated
    ack-mode.

    Trade-off: an acked frame is never re-offered by the server, so a frame
    that is parked on a seq gap when the socket drops is handed to the
    handlers at close time, in seq order but possibly ahead of the seqs it
    was waiting for (which arrive after the reconnect). Ack-on-receipt
    trades strict ordering across a connection drop for no loss; the
    default mode keeps ordering because unacked frames come back in order
    on the next drain. Such a flush fires ``on_sequence_gap`` with
    ``reason="connection_closed"``."""
    in_flight_adopt_ms: int = 15 * 60_000
    """A re-offer of a message whose first copy is still with the handlers
    takes that copy's outcome instead of being dispatched again — on the same
    connection always, on a later connection while the first copy has run
    less than this. Past it a handler that never finishes does not swallow
    every re-offer. Default 15 minutes."""
    capabilities: SocketCapabilityOptions | None = None
    """Socket capabilities to offer in HELLO besides ``ack`` (offered while a
    ``message.new`` handler is attached); the server echoes what it accepted
    (:attr:`RealtimeClient.capabilities`).

    ``heartbeat`` (default ``True``): the server sends a ``heartbeat`` frame
    on its 45 s timer, which keeps the idle watchdog working on sockets that
    cannot see pings.

    ``rewake`` (default ``False``): the server sends ``wake`` frames with
    ``reason: "unanswered"`` while cards on the tray stay unanswered, and
    counts this socket as present, so the wake webhook does not re-wake too.
    Offer it only when an ``on("wake")`` handler acts on those frames (the
    integration kit's :class:`~agentchatme.integration.ReceiveLoop` does); a
    socket that accepts it and ignores wakes swallows them."""


# ───────────────────────── Internal constants ─────────────────────────

# How long the liveness probe waits for its pong.
_LIVENESS_PROBE_TIMEOUT_S = 10.0

# The code on every close this client initiates (watchdogs, a HELLO it could
# not send). Private-use range, so server telemetry can tell our closes from
# the standard codes; the reason says which watchdog.
_CLIENT_CLOSE_CODE = 4000

# Events the client raises itself through ``on()``. A server frame that
# happens to carry one of these types is dropped, never dispatched as if it
# were ours.
_LOCAL_EVENTS = frozenset({"state", "blocked", "stopped"})

# How long a connection must SURVIVE before we call it healthy and clear
# the reconnect backoff.
#
# This exists because "the handshake succeeded" is not the same claim as
# "this connection works". Resetting the attempt counter on ``hello.ok``
# alone means a socket that connects and dies seconds later always retries
# at the floor delay — the exponential backoff below can then never engage,
# because it only ever counts attempts that failed BEFORE the handshake.
# A client in that state reconnects forever at ~2/second-of-session with no
# ramp at all: observed in production as 725 connections in four hours from
# one agent, delivering zero messages, while six sibling agents on the same
# host held single connections the whole time.
#
# 30s is comfortably longer than any handshake-adjacent failure (the
# handshake deadline is 15s, the server's own heartbeat cycle is 45s) and
# short enough that a genuinely healthy client clears its counter well
# before a second blip.
_STABLE_CONNECTION_S = 30.0

# Reconnects within this window are "rapid" for the purposes of the
# instability warning below. Slightly above the backoff floor so a single
# ordinary reconnect never trips it.
_RAPID_RECONNECT_S = 60.0

# Consecutive rapid reconnects before we tell the operator something is
# wrong. The failure this catches is invisible from inside the agent — the
# SDK reconnects successfully every time, so nothing looks broken locally
# while the connection is unusable in practice.
_INSTABILITY_WARN_THRESHOLD = 5

# Time we let a seq gap sit before issuing an explicit gap-fill. Two
# seconds is well below agent-loop perceptual floors and well above the
# drain↔live-fanout interleave window.
_GAP_FILL_WINDOW_S = 2.0

# Hard cap on per-conversation buffer before we force-drain and surface
# the incident. Realistic bursts stay well under this.
_MAX_BUFFERED_PER_CONVERSATION = 500

# Maximum rows requested in a single ``get_messages`` gap-fill.
_GAP_FILL_LIMIT = 200

# Sync drain page size. Passed explicitly as ``limit`` so the short-page
# check compares against the size WE requested — the server-side default
# (200) is larger and may drift independently of this SDK.
_SYNC_PAGE_SIZE = 100

# Capability strings advertised in the HELLO frame. The server echoes the
# subset it accepted on ``hello.ok``; anything not echoed stays disabled
# locally (a ``hello.ok`` without ``capabilities`` means legacy server).
# ``ack`` (always): per-message delivery acks. ``heartbeat`` (default on):
# the server sends a ``heartbeat`` frame on its 45 s timer (no reply
# expected), which the idle watchdog counts like any inbound frame.
# ``rewake`` (opt-in): the server's unanswered re-wake comes to this socket.
# See ``RealtimeOptions.capabilities``.

# A ready socket whose server negotiated ``heartbeat`` gets a frame at least
# every 45 s, so the idle watchdog is armed on it even when the transport
# exposes no ping(); this deadline is two missed heartbeats plus margin.
_HEARTBEAT_LIVENESS_TIMEOUT_MS = 100_000

# Connect budget (per agent on /v1/ws, D8): over budget the server closes
# 1013 with a retry hint (see ``_connect_budget``): backoff with the server's
# delay, never ``blocked``. The client waits up to 10 % longer than that
# delay, never less, so a fleet told the same delay spreads out.
_RATE_LIMIT_JITTER = 0.1

# disconnect(): how long dispatches still running get to finish before they
# are cancelled. Handlers run off the receive loop, so this is the one place
# they are waited for.
_DISPOSE_GRACE_S = 1.0

# After this many consecutive failed attempts (about half an hour at the
# default 30 s cap) the outage is long: one error line is logged for the
# operator and the backoff cap widens to _LONG_OUTAGE_MAX_INTERVAL_MS.
# Retrying continues until the server is back — a deploy, a DNS outage or a
# laptop closed overnight is not a reason to stay dead afterwards.
_LONG_OUTAGE_AFTER_ATTEMPTS = 60
_LONG_OUTAGE_MAX_INTERVAL_MS = 5 * 60_000

# Backoff jitter: up to 25 % BELOW each step, so a fleet that drops at the
# same instant re-enters the connect queue spread across ms * [0.75, 1.0]
# and the caps (30 s, then 5 min) are true ceilings.
_BACKOFF_JITTER = 0.25

# After the server rejects the key, retry a fresh HELLO at this cadence
# (±_AUTH_RETRY_JITTER: 10-15 min). A rejected key is not a dead end: keys
# get rotated back, suspended accounts get reinstated, and a server-side
# lookup failure during HELLO presents as a reject too.
_AUTH_RETRY_INTERVAL_MS = 750_000
_AUTH_RETRY_JITTER = 0.2

# Close codes that mean "the server rejected this session" when the SERVER
# sends them during the handshake. 4401/4403 are unambiguous (the server
# today sends only 1008; the application codes are honoured if they ever
# appear). 1008 (policy
# violation) is not: the AgentChat server closes a rejected HELLO with
# 1008 'Invalid token', but also uses 1008 for a handshake that was merely
# too slow ('HELLO frame required', from its own 5 s timer) — which a slow
# key lookup or one slow network leg produces with a perfectly good key —
# and for evicting a live socket after key rotation, suspension or sign-out.
# So 1008 counts as a rejection only with the server's auth reason; any
# other 1008 takes the normal backoff. A close this client initiated itself
# never counts (see ``_close_initiated_locally``), and these codes after
# ``hello.ok`` are an eviction: the next HELLO is the evidence, so they take
# the normal reconnect path too.
_UNAMBIGUOUS_AUTH_CLOSE_CODES = frozenset({4401, 4403})
_AMBIGUOUS_AUTH_CLOSE_CODE = 1008
_SERVER_AUTH_CLOSE_REASONS = ("invalid token",)


def _is_auth_rejection_close(code: int, reason: str) -> bool:
    """True iff a server-originated close with this code and reason, received
    during the handshake, means the key was rejected. Pure."""
    if code in _UNAMBIGUOUS_AUTH_CLOSE_CODES:
        return True
    if code != _AMBIGUOUS_AUTH_CLOSE_CODE:
        return False
    lower = reason.lower()
    return any(known in lower for known in _SERVER_AUTH_CLOSE_REASONS)


def _jittered_ms(ms: int, ratio: float) -> int:
    """``ms`` scaled by a uniform factor in ``[1 - ratio, 1 + ratio]``: a
    range around ``ms`` (the blocked retry's 10-15 min), not a cap."""
    factor = 1.0 - ratio + random.random() * 2.0 * ratio
    return max(0, int(ms * factor))


def _jittered_below_ms(ms: int, ratio: float) -> int:
    """``ms`` scaled by a uniform factor in ``[1 - ratio, 1]``: ``ms`` stays
    a true ceiling (the reconnect backoff and its caps)."""
    factor = 1.0 - random.random() * ratio
    return max(0, int(ms * factor))


def _jittered_above_ms(ms: int, ratio: float) -> int:
    """``ms`` scaled by a uniform factor in ``[1, 1 + ratio]``: ``ms`` stays
    a true floor (a delay the server asked for)."""
    factor = 1.0 + random.random() * ratio
    return max(0, int(ms * factor))


def _budget_wait_ms(delay_ms: int) -> int:
    """The server's delay, up to 10 % longer to spread a fleet told the same
    delay, never shorter."""
    return _jittered_above_ms(delay_ms, _RATE_LIMIT_JITTER)


async def _default_sleep(delay_s: float) -> None:
    await asyncio.sleep(delay_s)


def _consume_future(future: asyncio.Future[Any]) -> None:
    """Retrieve a future's outcome so an unawaited failure is not logged."""
    if not future.cancelled():
        future.exception()


_Routed = Literal["delivered", "buffered"]
"""Where :meth:`RealtimeClient._process_ordered_message` sent an envelope."""


@dataclass
class _PendingEnvelope:
    """A ``message.new`` frame buffered by the ordering layer, plus its
    delivery-ack eligibility.

    ``ws_ack`` records where the envelope entered the client: ``True`` for
    frames received on the live socket (ack-mode confirms them per-message
    after dispatch), ``False`` for rows injected by the REST drain or a
    gap-fill (those are committed via the ``/sync/ack`` cursor instead).
    The flag travels with the frame through the out-of-order buffer so a
    live frame that sat behind a seq gap still acks once dispatched.
    """

    message: dict[str, Any]
    ws_ack: bool
    # Acknowledged when it was received (ack_on_receipt): the server will
    # never re-offer it, so it must reach a handler, whatever happens.
    acked: bool = False


class _BoundedLruSet:
    """Bounded LRU membership set used for message-id dedup.

    :meth:`hit` refreshes recency on lookup so an id that keeps re-arriving
    (drain ↔ live races, server redelivery) cannot age out while it is
    still hot. Once ``capacity`` is exceeded the least-recently-seen id is
    evicted. A capacity of zero (or less) disables the set entirely.
    """

    __slots__ = ("_capacity", "_entries")

    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._entries: OrderedDict[str, None] = OrderedDict()

    @property
    def enabled(self) -> bool:
        return self._capacity > 0

    def hit(self, key: str) -> bool:
        """Return ``True`` (and refresh recency) if ``key`` was seen before."""
        if key not in self._entries:
            return False
        self._entries.move_to_end(key)
        return True

    def add(self, key: str) -> None:
        if self._capacity <= 0:
            return
        self._entries[key] = None
        self._entries.move_to_end(key)
        while len(self._entries) > self._capacity:
            self._entries.popitem(last=False)


@dataclass
class _InFlight:
    """A ``message.new`` whose handlers are running: the future its outcome
    lands on (``True`` = every handler settled cleanly), the connection
    generation it was dispatched on, and when (monotonic seconds)."""

    outcome: asyncio.Future[bool]
    generation: int
    started_at: float


@dataclass
class _Job:
    """One frame waiting in (or running on) a dispatch lane."""

    message: dict[str, Any]
    handlers: list[Any]
    # message.new only: what its settlement records.
    message_new: bool = False
    message_id: str | None = None
    own: _InFlight | None = None
    ws_ack: bool = False
    settlement: asyncio.Future[bool] | None = None
    # Where the handlers stand: the next one to call, and what an async one
    # called inline returned (the lane's worker awaits it, so a handler runs
    # inside its lane's task).
    next_index: int = 0
    awaiting: Any = None
    ok: bool = True
    # The task finishing this job's handlers once the lane reached it.
    run: asyncio.Future[None] | None = None


@dataclass
class _Lane:
    jobs: deque[_Job] = field(default_factory=deque)
    worker: asyncio.Task[None] | None = None


@dataclass
class _OrderState:
    next_expected_seq: int | None = None
    buffer: dict[int, _PendingEnvelope] = field(default_factory=dict)
    gap_task: asyncio.Task[None] | None = None
    gap_started_at: float | None = None
    gap_started_expected_seq: int | None = None
    gap_fill_in_flight: bool = False
    # Callers handing a resolved gap's frames over right now. The receive
    # loop keeps running meanwhile and must not drop this state as idle:
    # the next frame would anchor afresh and skip a hole unseen.
    resolving: int = 0


# ───────────────────────── Client ─────────────────────────


class RealtimeClient:
    """WebSocket client with HELLO handshake, seq ordering, and reconnect.

    The class can be used in two construction styles:

    .. code-block:: python

        rt = RealtimeClient(api_key="sk_...", client=async_client)
        # — or —
        rt = RealtimeClient(RealtimeOptions(api_key="sk_...", client=...))

    It is also an async context manager::

        async with RealtimeClient(api_key="sk_...") as rt:
            rt.on("message.new", on_msg)
            await asyncio.sleep(3600)  # keep the loop alive
    """

    def __init__(
        self,
        options: RealtimeOptions | None = None,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        reconnect: bool | None = None,
        reconnect_interval_ms: int | None = None,
        max_reconnect_interval_ms: int | None = None,
        max_reconnect_attempts: int | None = None,
        client: AsyncAgentChatClient | None = None,
        on_sequence_gap: SequenceGapHandler | None = None,
        auto_drain_on_connect: bool | None = None,
        dedup_cache_size: int | None = None,
        client_identity: AgentChatClientIdentity | None = None,
        handshake_timeout_ms: int | None = None,
        liveness_timeout_ms: int | None = None,
        ack_on_receipt: bool | None = None,
        capabilities: SocketCapabilityOptions | None = None,
        in_flight_adopt_ms: int | None = None,
        websocket_connect: Callable[..., Awaitable[Any]] | None = None,
    ) -> None:
        if options is None:
            if api_key is None:
                raise TypeError("RealtimeClient requires an api_key")
            options = RealtimeOptions(
                api_key=api_key,
                base_url=base_url if base_url is not None else "wss://api.agentchat.me",
                reconnect=reconnect if reconnect is not None else True,
                reconnect_interval_ms=(
                    reconnect_interval_ms if reconnect_interval_ms is not None else 500
                ),
                max_reconnect_interval_ms=(
                    max_reconnect_interval_ms
                    if max_reconnect_interval_ms is not None
                    else 30_000
                ),
                max_reconnect_attempts=max_reconnect_attempts,
                client=client,
                on_sequence_gap=on_sequence_gap,
                auto_drain_on_connect=auto_drain_on_connect,
                dedup_cache_size=(
                    dedup_cache_size
                    if dedup_cache_size is not None
                    else _DEFAULT_DEDUP_CACHE_SIZE
                ),
                client_identity=client_identity or DEFAULT_CLIENT_IDENTITY,
                handshake_timeout_ms=(
                    handshake_timeout_ms
                    if handshake_timeout_ms is not None
                    else _DEFAULT_HANDSHAKE_TIMEOUT_MS
                ),
                liveness_timeout_ms=liveness_timeout_ms,
                ack_on_receipt=bool(ack_on_receipt),
                capabilities=capabilities,
                in_flight_adopt_ms=(
                    in_flight_adopt_ms if in_flight_adopt_ms is not None else 15 * 60_000
                ),
            )

        # A private copy: the client writes to its options (the auto-drain
        # default here, `reconnect=False` on disconnect) and must never write
        # to the caller's object — several instances may be built from one.
        options = replace(options)
        if options.auto_drain_on_connect is None:
            options.auto_drain_on_connect = options.client is not None
        if options.handshake_timeout_ms <= 0:
            # A zero deadline would open and close sockets in a loop; a
            # non-positive value means the default, as in the TypeScript SDK.
            options.handshake_timeout_ms = _DEFAULT_HANDSHAKE_TIMEOUT_MS
        if options.max_reconnect_attempts is not None:
            warnings.warn(
                "RealtimeClient ignores max_reconnect_attempts: the client never"
                " stops reconnecting on its own; call disconnect() to end it.",
                DeprecationWarning,
                stacklevel=2,
            )

        self._opts = options
        self._websocket_connect = websocket_connect

        self._ws: Any = None
        self._recv_task: asyncio.Task[None] | None = None
        # One slot for both timers: the backoff before a reconnect and the
        # slow retry while blocked. Never both at once.
        self._reconnect_task: asyncio.Task[None] | None = None
        self._handshake_task: asyncio.Task[None] | None = None
        self._liveness_task: asyncio.Task[None] | None = None
        self._stability_task: asyncio.Task[None] | None = None
        self._bg_tasks: set[asyncio.Task[Any]] = set()
        self._state: RealtimeState = "idle"
        self._connecting = False
        self._reconnect_attempts = 0
        # Consecutive connections that failed to reach _STABLE_CONNECTION_S.
        # Drives the operator-facing instability warning only; the backoff
        # itself is governed by _reconnect_attempts.
        self._rapid_reconnects = 0
        self._last_connect_monotonic: float | None = None
        self._last_inbound_monotonic = 0.0
        self._authenticated = False
        self._disposed = False
        self._order_states: dict[str, _OrderState] = {}
        # Delivery-ack mode: on only when THIS connection's hello.ok echoed
        # the `ack` capability. Reset on every connect/close.
        self._ack_negotiated = False
        # Every capability THIS connection's hello.ok echoed of the ones its
        # HELLO offered (empty between connections and against a legacy
        # server).
        self._negotiated: frozenset[str] = frozenset()
        self._offered_this_hello: frozenset[str] = frozenset()
        # HELLO went out on the current socket; and the close in progress is
        # the one quiet reconnect that renegotiates ack (see
        # _maybe_renegotiate_ack).
        self._hello_sent = False
        self._renegotiating = False
        # Set when the server's last close or upgrade answer was the connect
        # budget (D8): the delay it asked for, until the next hello.ok.
        self._rate_limited_ms: int | None = None
        # Set by _close_socket before it closes the current socket, read and
        # cleared by the recv loop's close handling. A close this client
        # asked for says nothing about the server, whatever code travelled
        # with it.
        self._close_initiated_locally = False
        # The last auth rejection, as "<code> <reason>", until a hello.ok
        # clears it. Survives a transport failure during the slow retry.
        self._blocked_reason: str | None = None
        # One error line per long outage / per run of rejected keys, then
        # debug.
        self._long_outage_announced = False
        self._blocked_announced = False
        # The wait used by the reconnect and blocked-retry timers. Tests swap
        # it for a controllable clock; everything else sleeps for real.
        self._sleep: Callable[[float], Awaitable[None]] = _default_sleep
        # Message-id dedup shared by the live and drain paths. Survives
        # reconnects deliberately: redelivery across connections is exactly
        # the duplicate source it exists to absorb.
        self._dedup = _BoundedLruSet(options.dedup_cache_size)
        # Dispatches still running, by message id: resolves to the first
        # copy's outcome. A second copy that arrives meanwhile (the server's
        # reconnect backlog and the REST drain bring the same stored rows; a
        # slow handler is still on the first; a reconnect re-offers a frame
        # whose handler has not finished) is not handed to the handlers
        # again, on this connection or a later one: it takes the first
        # copy's outcome without waiting for it. An entry lives exactly as
        # long as its handlers run, so the map is bounded by the dispatches
        # in flight. Off with the dedup cache (``dedup_cache_size=0``).
        self._inflight: dict[str, _InFlight] = {}
        # Async on_connect handler tasks; disconnect() gives them a short
        # grace, then cancels them.
        self._dispatch_tasks: set[asyncio.Future[Any]] = set()
        # Dispatch lanes: one serial lane per conversation (message.new and
        # every frame of that conversation) and one for frames of no
        # conversation (wake, presence, invites, ...). At most one handler
        # runs per lane; lanes run concurrently; none blocks the receive
        # loop. A lane's worker exists only while it has work.
        self._lanes: dict[str, _Lane] = {}
        # Bumped on every socket close: a repeat adopts a running first copy
        # from an earlier connection only within in_flight_adopt_ms.
        self._generation = 0
        # One REST drain at a time: the server-side ack pointer only moves
        # forward, so a second concurrent drain adds nothing but races.
        self._drain_in_flight = False
        # Per-row dispatch settlement for the page the drain is working on,
        # keyed by the envelope's identity: ``None`` until the envelope is
        # handed to handlers, then a future that resolves ``True`` (every
        # handler settled cleanly, or a dedup hit) or ``False`` (a handler
        # raised, or no ``message.new`` handler was attached). The drain
        # awaits it before moving the ack cursor — see
        # :meth:`drain_offline_envelopes`.
        self._drain_settlements: dict[int, asyncio.Future[bool] | None] = {}
        # Drain rows of the current page that adopted an in-flight first copy
        # instead of being dispatched, by envelope identity: the cursor reads
        # that copy's outcome without ever waiting for it.
        self._drain_adopted: dict[int, asyncio.Future[bool]] = {}

        self._handlers: dict[str, set[MessageHandler]] = {}
        self._error_handlers: set[ErrorHandler] = set()
        self._connect_handlers: set[ConnectHandler] = set()
        self._disconnect_handlers: set[DisconnectHandler] = set()

    # ─── Context manager ─────────────────────────────────────────────

    async def __aenter__(self) -> RealtimeClient:
        await self.connect()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.disconnect()

    # ─── connect ─────────────────────────────────────────────────────

    async def connect(self) -> None:
        """Open the WebSocket and send the HELLO frame.

        Returns once the HELLO has been transmitted — NOT after
        ``hello.ok``. Register :meth:`on_connect` to react to a fully
        authenticated session. Raises :class:`ConnectionError` if the
        socket fails to open or the ``websockets`` package is missing; a
        failed open also schedules the normal reconnect.

        A no-op while an attempt is in flight or a socket is live. A pending
        backoff or blocked-retry timer is cancelled and the attempt made now,
        so a caller that has just rotated its key does not wait out the slow
        retry. Callable from inside ``on_disconnect``, ``on_error`` and
        ``on("state")``: the closed socket is already gone by then, so a
        caller running its own reconnects (``reconnect=False``) may reopen
        from there.
        """
        if self._disposed:
            raise _RealtimeConnectionError(
                "RealtimeClient has been disposed; create a new instance to reconnect."
            )
        recv_task = self._recv_task
        recv_alive = (
            recv_task is not None
            and not recv_task.done()
            # The recv loop's own close handling (handlers run inside its
            # ``finally``) is not a live socket.
            and recv_task is not asyncio.current_task()
        )
        if self._connecting or self._ws is not None or recv_alive:
            _log.debug("connect() ignored: an attempt or a live socket exists")
            return
        pending = self._reconnect_task
        if pending is not None and pending is not asyncio.current_task() and not pending.done():
            pending.cancel()
        self._reconnect_task = None

        connect_fn = self._resolve_connect_fn()

        url = f"{self._opts.base_url}/v1/ws"
        self._connecting = True
        await self._set_state("connecting")
        try:
            ws = await connect_fn(url, **_keepalive_kwargs(connect_fn))
        except Exception as err:
            self._connecting = False
            # Any failed open, an upgrade answered with an HTTP status (429
            # included) too, is the ordinary backoff: the connect budget is
            # answered after the upgrade (close 1013), never before it.
            error = _RealtimeConnectionError(f"WebSocket connection failed: {err}")
            await self._emit_error(error)
            await self._schedule_reconnect()
            raise error from err
        finally:
            self._connecting = False

        if self._disposed:
            # disconnect() ran while the transport was opening.
            with contextlib.suppress(Exception):
                await ws.close(code=1000, reason="client stopped")
            return

        self._ws = ws
        self._authenticated = False
        self._ack_negotiated = False
        self._negotiated = frozenset()
        self._close_initiated_locally = False
        self._hello_sent = False

        try:
            await ws.send(
                json.dumps(
                    {
                        "type": "hello",
                        "api_key": self._opts.api_key,
                        # Advertised capabilities; the server ignores unknown
                        # strings and echoes the accepted subset on hello.ok.
                        "capabilities": self._advertised_capabilities(),
                        "client": self._opts.client_identity.name,
                        **(
                            {
                                "client_version": self._opts.client_identity.version
                            }
                            if self._opts.client_identity.version
                            else {}
                        ),
                    }
                )
            )
            self._hello_sent = True
        except Exception as err:
            # A socket we cannot write HELLO to is useless; drop it as our
            # own close and take the normal backoff rather than sit on it.
            await self._emit_error(
                _RealtimeConnectionError(f"HELLO send failed: {err}")
            )
            self._ws = None
            await self._close_socket(ws, _CLIENT_CLOSE_CODE, "client HELLO send failed")
            await self._schedule_reconnect()
            return

        self._handshake_task = asyncio.create_task(self._handshake_watchdog(ws))
        self._recv_task = asyncio.create_task(self._recv_loop())

    def _maybe_renegotiate_ack(self) -> None:
        """The first ``message.new`` handler was attached after this
        socket's HELLO went out without ``ack``: reconnect once, quietly (no
        error, no backoff step), so the next HELLO negotiates delivery acks.
        Frames parked on a seq gap are handed to the handlers first."""
        if self._disposed or self._ws is None or not self._hello_sent:
            return
        if "ack" in self._offered_this_hello:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        ws = self._ws

        def renegotiate() -> None:
            if self._ws is not ws or self._disposed or not self._has_message_handler():
                return
            self._renegotiating = True
            self._spawn(self._close_socket(ws, 1000, "client reconnects to negotiate delivery acks"))

        loop.call_soon(renegotiate)

    def _advertised_capabilities(self) -> list[str]:
        """What this HELLO offers. ``ack`` only while a ``message.new``
        handler is attached: a handler-less client is marked on send, as a
        handler-less 1.2 client effectively was, and never builds a stored
        backlog on the server."""
        offered = self._opts.capabilities or {}
        caps: list[str] = []
        if self._has_message_handler():
            caps.append("ack")
        if offered.get("heartbeat", True) is not False:
            caps.append("heartbeat")
        if offered.get("rewake", False) is True:
            caps.append("rewake")
        self._offered_this_hello = frozenset(caps)
        return caps

    def _resolve_connect_fn(self) -> Callable[..., Awaitable[Any]]:
        if self._websocket_connect is not None:
            return self._websocket_connect
        try:
            from websockets.asyncio.client import connect as _ws_connect

            return _ws_connect
        except ImportError:
            pass
        try:
            from websockets import connect as _ws_connect_legacy

            return _ws_connect_legacy
        except ImportError as err:
            raise _RealtimeConnectionError(
                "The `websockets` package is required for the realtime client. "
                "Install it with `pip install websockets>=12`."
            ) from err

    def _start_stability_timer(self) -> None:
        """Clear the reconnect backoff once this connection proves itself.

        Scheduled on ``hello.ok``, cancelled on close. If it fires, the
        socket has been up for _STABLE_CONNECTION_S and the next failure
        deserves to start its backoff from the floor again. If it's
        cancelled, the connection died young and the counter carries
        forward, so the delay keeps growing toward the cap.
        """
        self._cancel_stability_timer()
        self._last_connect_monotonic = time.monotonic()

        async def _mark_stable() -> None:
            try:
                await asyncio.sleep(_STABLE_CONNECTION_S)
            except asyncio.CancelledError:
                return
            if self._disposed or not self._authenticated:
                return
            self._reconnect_attempts = 0
            self._rapid_reconnects = 0
            self._long_outage_announced = False
            self._stability_task = None

        try:
            self._stability_task = asyncio.create_task(_mark_stable())
        except RuntimeError:  # pragma: no cover - no running loop
            # No loop to schedule on: fall back to the old semantics rather
            # than leaving the counter pinned forever.
            self._reconnect_attempts = 0

    def _cancel_stability_timer(self) -> None:
        if self._stability_task is not None:
            self._stability_task.cancel()
            self._stability_task = None

    def _note_connection_ended(self) -> None:
        """Track short-lived connections and warn once they form a pattern.

        A flapping client looks healthy from the inside — every reconnect
        succeeds — so without this the operator has no local signal at all
        that the connection is unusable.
        """
        started = self._last_connect_monotonic
        self._last_connect_monotonic = None
        if started is None:
            return
        if time.monotonic() - started >= _RAPID_RECONNECT_S:
            self._rapid_reconnects = 0
            return

        self._rapid_reconnects += 1
        if self._rapid_reconnects == _INSTABILITY_WARN_THRESHOLD:
            _log.warning(
                "AgentChat realtime connection is unstable: %d reconnects in "
                "under %.0fs each. Backing off (next retry in up to %.0fs). "
                "This usually means the network path or a local supervisor "
                "is dropping the socket, not an AgentChat outage.",
                self._rapid_reconnects,
                _RAPID_RECONNECT_S,
                self._opts.max_reconnect_interval_ms / 1000.0,
            )

    # ─── Watchdogs ───────────────────────────────────────────────────

    async def _close_socket(self, ws: Any, code: int, reason: str) -> None:
        """Close ``ws`` as a client-initiated close.

        The flag is set before the close so the recv loop, which may see
        the close before this coroutine resumes, never reads our own code
        as a verdict from the server (rule 2).
        """
        self._close_initiated_locally = True
        with contextlib.suppress(Exception):
            await ws.close(code=code, reason=reason)

    async def _handshake_watchdog(self, ws: Any) -> None:
        """Bound socket-open → ``hello.ok``. A trip is our close: backoff."""
        timeout_ms = self._opts.handshake_timeout_ms
        try:
            await asyncio.sleep(timeout_ms / 1000.0)
        except asyncio.CancelledError:
            return
        if self._authenticated or self._disposed or self._ws is not ws:
            return
        await self._emit_error(
            _RealtimeConnectionError(f"handshake timeout: no hello.ok within {timeout_ms}ms")
        )
        # Spawned, not awaited: the recv loop cancels this task as part of
        # the close, and a cancelled close could leave the socket half shut.
        self._close_initiated_locally = True
        self._spawn(self._close_socket(ws, _CLIENT_CLOSE_CODE, "client handshake timeout"))

    def _liveness_timeout_ms(self, ws: Any) -> int:
        """The liveness deadline for ``ws``; ``0`` means no watchdog."""
        configured = self._opts.liveness_timeout_ms
        if configured is not None:
            return max(0, configured)
        if callable(getattr(ws, "ping", None)):
            return _DEFAULT_LIVENESS_TIMEOUT_MS
        if "heartbeat" in self._negotiated:
            # The server sends a frame at least every 45 s: frames alone are
            # the evidence, ping or not.
            return _HEARTBEAT_LIVENESS_TIMEOUT_MS
        # Nothing to probe with: inbound frames alone cannot separate a quiet
        # conversation from a dead peer, so do not guess.
        _log.debug("liveness watchdog not armed: the socket exposes no ping()")
        return 0

    def _start_liveness_watchdog(self, ws: Any) -> None:
        self._cancel_liveness_watchdog()
        timeout_ms = self._liveness_timeout_ms(ws)
        if timeout_ms <= 0:
            return
        self._last_inbound_monotonic = time.monotonic()
        self._liveness_task = asyncio.create_task(self._liveness_watchdog(ws, timeout_ms))

    def _cancel_liveness_watchdog(self) -> None:
        if self._liveness_task is not None:
            self._liveness_task.cancel()
            self._liveness_task = None

    async def _liveness_watchdog(self, ws: Any, timeout_ms: int) -> None:
        """Trip when ``timeout_ms`` passes with no inbound frame and the peer
        does not answer a ping either. A trip is our close: backoff."""
        timeout_s = timeout_ms / 1000.0
        try:
            while not self._disposed and self._ws is ws:
                remaining = timeout_s - (time.monotonic() - self._last_inbound_monotonic)
                if remaining > 0:
                    await asyncio.sleep(remaining)
                    continue
                if await self._probe_liveness(ws):
                    self._last_inbound_monotonic = time.monotonic()
                    continue
                if self._disposed or self._ws is not ws:
                    return
                await self._emit_error(
                    _RealtimeConnectionError(
                        f"liveness timeout: nothing received for {timeout_ms}ms"
                    )
                )
                self._close_initiated_locally = True
                self._spawn(self._close_socket(ws, _CLIENT_CLOSE_CODE, "client liveness timeout"))
                return
        except asyncio.CancelledError:
            return

    async def _probe_liveness(self, ws: Any) -> bool:
        """Send a ping; ``True`` iff a pong arrives in ``_LIVENESS_PROBE_TIMEOUT_S``.

        ``websockets`` (both implementations) returns an awaitable that
        resolves on the matching pong. Its future is never cancelled from
        here — the library settles it on pong or on close — only left
        behind with its outcome consumed. A socket without ``ping()`` (an
        explicit ``liveness_timeout_ms`` armed the watchdog anyway) counts
        inbound frames alone.
        """
        ping = getattr(ws, "ping", None)
        if not callable(ping):
            return False
        try:
            waiter = await ping()
        except Exception:
            return False
        if not inspect.isawaitable(waiter):
            # A transport whose ping() gives no pong signal cannot be probed;
            # treat the successful send as the best evidence available.
            return True
        future = asyncio.ensure_future(waiter)
        done, _ = await asyncio.wait({future}, timeout=_LIVENESS_PROBE_TIMEOUT_S)
        if future in done:
            return not future.cancelled() and future.exception() is None
        future.add_done_callback(_consume_future)
        return False

    async def _set_state(self, state: RealtimeState) -> None:
        """Record a transition and raise ``on("state")`` for it."""
        previous = self._state
        if state == previous:
            return
        _log.debug("realtime state %s -> %s", previous, state)
        self._state = state
        if self._handlers.get("state"):
            await self._dispatch_local({"type": "state", "state": state, "previous": previous})

    async def _recv_loop(self) -> None:
        ws = self._ws
        if ws is None:
            return
        try:
            async for raw in ws:
                self._last_inbound_monotonic = time.monotonic()
                try:
                    message = json.loads(raw)
                except (ValueError, TypeError):
                    continue
                if not isinstance(message, dict):
                    continue

                if not self._authenticated:
                    if message.get("type") == "hello.ok":
                        self._authenticated = True
                        # NOT reset here. A successful handshake proves the
                        # credential, not the connection — see
                        # _STABLE_CONNECTION_S. The counter clears from the
                        # stability timer below, only if we're still up in
                        # 30 seconds.
                        self._start_stability_timer()
                        # Delivery-ack capability negotiation: enable only
                        # if the server echoed `ack`. A hello.ok without a
                        # capabilities list is a legacy server → stay in
                        # mark-on-send semantics and send no ack frames.
                        # A capability holds only when this HELLO offered it
                        # and the server echoed it.
                        caps = message.get("capabilities")
                        self._negotiated = frozenset(
                            cap
                            for cap in caps
                            if isinstance(cap, str) and cap in self._offered_this_hello
                        ) if isinstance(caps, list) else frozenset()
                        self._ack_negotiated = "ack" in self._negotiated
                        self._rate_limited_ms = None
                        if self._handshake_task is not None:
                            self._handshake_task.cancel()
                            self._handshake_task = None
                        # A hello.ok is the proof that the key is accepted:
                        # whatever rejection came before is over.
                        if self._blocked_reason is not None:
                            _log.info(
                                "AgentChat realtime: key accepted again, blocked cleared (was %s)",
                                self._blocked_reason,
                            )
                        self._blocked_reason = None
                        self._blocked_announced = False
                        await self._set_state("ready")
                        self._start_liveness_watchdog(ws)
                        self._emit_connect()
                        if (
                            self._opts.auto_drain_on_connect
                            and self._opts.client is not None
                        ):
                            self._spawn(self.drain_offline_envelopes())
                    continue

                if self._disposed:
                    # disconnect() has begun: take nothing new in. A frame
                    # is neither acked nor dispatched, so the server keeps it
                    # for the next connection, and the shutdown flush works
                    # on a fixed set of parked frames instead of chasing an
                    # open socket.
                    continue

                frame_type = message.get("type")
                if frame_type == "message.new":
                    if self._opts.ack_on_receipt and self._has_message_handler():
                        # Delivered = this machine received it. Ack now, then
                        # route through ordering/dedup with the ack already
                        # spent so dispatch never acks a second time. With
                        # no message.new handler attached nothing here will
                        # ever see the frame, so it is not acked at all: it
                        # takes the default path below, which acks only
                        # after a handler took it.
                        message_id = _extract_message_id(message)
                        if message_id is not None:
                            await self._send_delivery_ack(message_id)
                        await self._process_ordered_message(
                            message, ws_ack=False, ack_spent=True
                        )
                    else:
                        # Live socket frames are ack-eligible after dispatch;
                        # drain/gap-fill injections are not (they commit via
                        # the REST cursor).
                        await self._process_ordered_message(message, ws_ack=True)
                    continue

                if frame_type == "wake":
                    # A signal with no message text: read the tray. Nothing
                    # to ack — no message travelled.
                    await self._dispatch_wake(message)
                    continue

                if frame_type in _LOCAL_EVENTS:
                    _log.debug("dropping server frame typed like a local event: %s", frame_type)
                    continue

                # Everything else, ``heartbeat`` included (its arrival already
                # fed the idle watchdog above), goes to its handlers without
                # holding up the next read.
                self._dispatch_frame(message)
        except asyncio.CancelledError:
            raise
        except Exception as err:
            await self._emit_error(
                _RealtimeConnectionError(f"WebSocket error: {err}")
            )
        finally:
            close_code = 0
            close_reason = ""
            if ws is not None:
                code_attr = getattr(ws, "close_code", None)
                reason_attr = getattr(ws, "close_reason", None)
                if isinstance(code_attr, int):
                    close_code = code_attr
                if isinstance(reason_attr, str):
                    close_reason = reason_attr
            # Read both BEFORE anything below resets them: only a close the
            # server sent, before hello.ok, can be a rejection.
            ours = self._close_initiated_locally
            self._close_initiated_locally = False
            handshake_done = self._authenticated
            renegotiating = self._renegotiating
            self._renegotiating = False
            self._hello_sent = False

            if self._handshake_task is not None:
                self._handshake_task.cancel()
                self._handshake_task = None
            self._cancel_liveness_watchdog()
            # Cancel BEFORE clearing _authenticated: a connection that dies
            # young must not clear the backoff counter, which is the whole
            # point of the stability window.
            self._cancel_stability_timer()
            if not renegotiating:
                self._note_connection_ended()
            self._authenticated = False
            self._ack_negotiated = False
            self._negotiated = frozenset()
            if self._ws is ws:
                self._ws = None

            # The connect budget (D8): the server closed with 1013 and,
            # usually, the delay it wants. Backoff with that delay; never an
            # auth verdict, whenever it arrives.
            rate_limited_ms: int | None = None
            if not ours:
                rate_limited_ms = connect_budget_delay_ms(code=close_code, reason=close_reason)
                if rate_limited_ms is not None:
                    self._rate_limited_ms = rate_limited_ms

            info: dict[str, Any] = {
                "code": close_code,
                "reason": close_reason,
                "was_clean": close_code == 1000,
                # Who ended the socket. "client" is this client's own
                # watchdog or stop: such a close says nothing about the
                # server. "server" arrived on the wire: the server, a
                # proxy, or the network.
                "initiator": "client" if ours else "server",
            }
            if rate_limited_ms is not None:
                info["retry_after_ms"] = rate_limited_ms
            await self._emit_disconnect(info)
            if rate_limited_ms is not None and not self._disposed:
                await self._emit_error(
                    _RealtimeConnectionError(
                        f"the server refused the connection: connect budget exceeded"
                        f" (close {close_code}); retrying in {round(rate_limited_ms / 1000)}s"
                    )
                )

            # Ack-on-receipt: every frame parked on a seq gap was acknowledged
            # at parse, so the server has it `delivered` and will never
            # re-offer it. Dropping the buffer here would lose it for good —
            # the exact loss the ack protocol exists to prevent. Hand the
            # parked frames to the handlers now, in seq order, before the
            # state goes. Default mode skips this: its parked frames are
            # unacked and come back in order on the next drain.
            # The quiet reconnect that renegotiates ack: this socket was
            # marked on send, so nothing would re-offer a parked frame either.
            if self._opts.ack_on_receipt or renegotiating:
                await self._flush_parked_frames("connection_closed")
            self._reset_order_states()
            self._generation += 1
            if renegotiating and not self._disposed:
                # At once, no backoff step, no error: the socket was fine.
                with contextlib.suppress(Exception):
                    await self.connect()
            elif not self._disposed:
                _log.debug(
                    "realtime socket closed (code=%s reason=%r ours=%s handshake_done=%s)",
                    close_code,
                    close_reason,
                    ours,
                    handshake_done,
                )
                if (
                    not ours
                    and not handshake_done
                    and rate_limited_ms is None
                    and _is_auth_rejection_close(close_code, close_reason)
                ):
                    # The server rejected the key during the handshake. Not
                    # the end: keys get rotated back and accounts reinstated,
                    # so retry slowly rather than storm or die (rule 4).
                    await self._enter_blocked(close_code, close_reason)
                else:
                    # Our own close, a transport drop, a slow handshake the
                    # server timed out, an eviction after hello.ok, or the
                    # connect budget: the next HELLO is the evidence (rules 2
                    # and 3), after the server's delay when it gave one.
                    await self._schedule_reconnect(server_delay_ms=rate_limited_ms)

    # ─── Offline drain ───────────────────────────────────────────────

    async def drain_offline_envelopes(self) -> None:
        """Drain undelivered messages accumulated while the socket was down.

        Speaks the real ``/v1/messages/sync`` wire: a **bare array** of
        message rows whose ``delivery_id`` is an opaque, nullable string
        cursor — never an integer, never compared numerically. Pages with
        the ``after`` cursor until a short page.

        Per page: rows are routed through the same ``message.new``
        ordering pipeline as live frames, **then** the batch is committed
        via ``POST /v1/messages/sync/ack`` with a positional cursor — the
        last non-empty ``delivery_id`` of the rows that were actually handed
        to the application. The cursor commits everything at-or-before it,
        so it may only cover rows whose fate is known, in page order:

        - A row every handler settled cleanly (or a dedup hit: the earlier
          dispatch is the proof) is covered.
        - A row whose handler raised is not, nor is anything after it: the
          server re-offers them; the dedup cache absorbs the replays of its
          acked predecessors.
        - A row parked in the out-of-order buffer (waiting on a seq gap) is
          not covered until it is dispatched: the cursor **freezes** at the
          last settled row for the rest of the drain. Without this, a
          disconnect that clears the ordering buffers would drop an
          already-acked message, and an acked message is never re-offered.
          Reading continues so the in-session gap-fill still resolves; the
          frozen tail comes back on the next drain and dedups.
        - A row below this connection's seq anchor (a live frame anchored the
          conversation first) is handed to the handlers late, out of order,
          unless this session already handled it (dedup hit): never lost
          beats strictly ordered. It is covered like any other row.
        - With no ``message.new`` handler attached, nothing is covered: no
          handler saw the rows, so the server keeps them.
        - A row whose message another copy is still handling on this
          connection (the server's reconnect backlog brings the same rows) is
          not dispatched again, and the drain never waits for that copy: the
          row is covered only if the copy already finished cleanly; otherwise
          the cursor stops there and the next drain covers it as a dedup hit.
        - A row that fails minimal validation stops the drain: the clean
          prefix before it is processed; the cursor never crosses it.

        Concurrent calls are coalesced. Safe to call again at any time —
        the server's ack pointer only advances.
        """
        client = self._opts.client
        if client is None or self._drain_in_flight:
            return
        self._drain_in_flight = True
        try:
            await self._run_drain(client)
        finally:
            self._drain_in_flight = False
            self._drain_settlements = {}
            self._drain_adopted = {}

    async def _run_drain(self, client: AsyncAgentChatClient) -> None:
        after: str | None = None
        acks_frozen = False
        while not self._disposed:
            try:
                rows = await client.sync(limit=_SYNC_PAGE_SIZE, after=after)
            except Exception as err:
                await self._emit_error(
                    _RealtimeConnectionError(f"sync drain failed: {err}")
                )
                return

            # disconnect() may have run during the request: the handler map
            # is cleared, so dispatching (and then acking) would mark
            # messages delivered that no handler ever saw.
            if self._disposed:
                return
            if not isinstance(rows, list) or not rows:
                return

            # Clean-prefix rule: stop at the FIRST row that fails minimal
            # validation. The ack cursor commits everything at-or-before
            # it, so a skipped-but-acked row would be silently lost.
            prefix: list[SyncRow] = []
            invalid_index: int | None = None
            for index, row in enumerate(rows):
                if not _is_valid_sync_row(row):
                    invalid_index = index
                    break
                prefix.append(row)

            envelopes: list[dict[str, Any]] = [
                {"type": "message.new", "payload": row} for row in prefix
            ]
            self._drain_settlements = {id(envelope): None for envelope in envelopes}
            self._drain_adopted = {}
            routed: list[_Routed] = []
            for envelope in envelopes:
                if self._disposed:
                    break
                routed.append(await self._process_ordered_message(envelope, ws_ack=False))

            if not acks_frozen and not self._disposed:
                cursor: str | None = None
                for row, envelope, outcome in zip(prefix, envelopes, routed):
                    if not await self._drain_row_ackable(envelope):
                        acks_frozen = True
                        _log.debug(
                            "sync drain: ack cursor frozen before %s (%s, not handled cleanly)",
                            row.get("id"),
                            outcome,
                        )
                        break
                    if self._disposed:
                        # disconnect() began while a handler settled: the
                        # handler map is gone, commit nothing more.
                        acks_frozen = True
                        break
                    # Positional cursor: batch order is authoritative; ids
                    # are opaque. A null delivery_id is covered by the next
                    # non-null one at-or-after it.
                    delivery_id = row.get("delivery_id")
                    if isinstance(delivery_id, str) and delivery_id:
                        cursor = delivery_id
                if cursor is not None:
                    try:
                        await client.sync_ack(cursor)
                    except Exception as err:
                        await self._emit_error(
                            _RealtimeConnectionError(f"sync ack failed: {err}")
                        )
                        return
            self._drain_settlements = {}
            self._drain_adopted = {}

            if invalid_index is not None:
                await self._emit_error(
                    _RealtimeConnectionError(
                        f"sync drain: row {invalid_index} failed validation;"
                        f" processed the {len(prefix)}-row clean prefix and"
                        " stopped without acking past it"
                    )
                )
                return

            if self._disposed or len(rows) < _SYNC_PAGE_SIZE:
                return

            # Page forward with the read cursor (non-committing). A full
            # page that cannot advance it (e.g. every delivery_id was null)
            # would re-read the same rows forever: stop and surface it; the
            # next drain retries from the server's committed pointer.
            next_after = last_sync_delivery_id(prefix)
            if next_after is None or next_after == after:
                await self._emit_error(
                    _RealtimeConnectionError(
                        "sync drain: full page without an advancing"
                        " delivery_id cursor; stopping to avoid a re-read loop"
                    )
                )
                return
            after = next_after

    async def _drain_row_ackable(self, envelope: dict[str, Any]) -> bool:
        """May the sync/ack cursor cover this drain row? See :meth:`drain_offline_envelopes`."""
        adopted = self._drain_adopted.get(id(envelope))
        if adopted is not None:
            # Another copy of this message is (or was) with the handlers.
            # Covered only if it already finished cleanly; never waited for.
            # Otherwise the cursor stops and the next drain re-offers the row,
            # a dedup hit by then.
            return adopted.done() and not adopted.cancelled() and adopted.result()
        settlement = self._drain_settlements.get(id(envelope))
        if settlement is None:
            # Still parked on a seq gap, or cleared from the buffer by a reset
            # without dispatch. No handler saw it: the cursor stops here.
            return False
        # Handed to handlers during this page (possibly after a stint in the
        # buffer): covered iff every handler settled cleanly. The handlers
        # run off the receive loop; the drain is the one that waits for them
        # (shielded: the drain being cancelled must not cancel a handler).
        try:
            return bool(await asyncio.shield(settlement))
        except asyncio.CancelledError:
            raise
        except Exception:
            return False

    # ─── Reconnect ───────────────────────────────────────────────────

    async def _schedule_reconnect(self, *, server_delay_ms: int | None = None) -> None:
        """Arm the normal backoff. No attempt cap: the client never gives up
        on its own (rule 1). ``server_delay_ms`` is the delay the server
        asked for (the connect budget): it replaces the backoff step (the
        client waits up to 10 % longer, never less)."""
        if self._disposed:
            return
        if self._connecting or self._ws is not None:
            # A handler (on_disconnect, on_error) already called connect()
            # for this close; the new attempt owns what happens next.
            return
        if not self._opts.reconnect:
            # The caller runs reconnects itself; a blocked verdict stays
            # visible so its logic can see it.
            if self._state != "blocked":
                await self._set_state("idle")
            return
        if self._reconnect_task is not None and not self._reconnect_task.done():
            return

        self._reconnect_attempts += 1
        attempt = self._reconnect_attempts
        if attempt > _LONG_OUTAGE_AFTER_ATTEMPTS and not self._long_outage_announced:
            # Once per outage, at error level: the operator should know the
            # server has been unreachable this long. Retrying goes on.
            self._long_outage_announced = True
            _log.error(
                "AgentChat realtime: still reconnecting after %d consecutive"
                " failures; backing off up to %.0fs between attempts and not"
                " giving up. The server is unreachable from here, or every"
                " connection dies before it is stable.",
                _LONG_OUTAGE_AFTER_ATTEMPTS,
                max(self._opts.max_reconnect_interval_ms, _LONG_OUTAGE_MAX_INTERVAL_MS) / 1000.0,
            )
        delay_ms = (
            _budget_wait_ms(server_delay_ms)
            if server_delay_ms is not None
            else self._compute_reconnect_delay_ms(attempt)
        )
        delay_s = delay_ms / 1000.0
        _log.debug("realtime reconnect %d scheduled in %.1fs", attempt, delay_s)
        await self._set_state("backoff")
        if self._disposed or self._connecting or self._ws is not None:
            # A state handler stopped the client, or reconnected it itself.
            return
        self._reconnect_task = asyncio.create_task(self._reconnect_after(delay_s))

    async def _enter_blocked(self, code: int, reason: str) -> None:
        """The server rejected the key during the handshake.

        Records the verdict, tells the operator and ``on_error`` once per
        blocked run (later rejections of the same run log at debug), fires
        ``on("blocked")`` for every rejection, and arms the slow retry. A
        ``hello.ok`` clears it; :meth:`disconnect` ends it.
        """
        detail = reason or "no reason given"
        self._blocked_reason = f"{code} {reason}".strip()
        retry_ms: int | None = None
        if self._opts.reconnect:
            retry_ms = self._compute_auth_retry_delay_ms()
        first_of_run = not self._blocked_announced
        self._blocked_announced = True
        log = _log.error if first_of_run else _log.debug
        log(
            "AgentChat realtime: the server rejected this API key (close %d %s);"
            " blocked, retrying a fresh HELLO %s until it is accepted again."
            " Check the key and the account state; disconnect() stops the retry.",
            code,
            detail,
            f"in ~{retry_ms // 60_000} min" if retry_ms is not None else "when connect() is called",
        )
        await self._set_state("blocked")
        if first_of_run:
            await self._emit_error(
                _RealtimeConnectionError(
                    f"WebSocket closed with auth rejection {code} ({detail}); blocked —"
                    + (
                        f" retrying a fresh HELLO in ~{retry_ms // 60_000} min"
                        if retry_ms is not None
                        else " reconnect is off, call connect() to retry"
                    )
                    + " until the key is accepted again; disconnect() stops it."
                )
            )
        # The server's reason as sent (empty when it sent none).
        await self._dispatch_local(
            {"type": "blocked", "code": code, "reason": reason, "retry_in_ms": retry_ms}
        )
        if self._disposed or retry_ms is None or self._connecting or self._ws is not None:
            return
        pending = self._reconnect_task
        if pending is not None and pending is not asyncio.current_task() and not pending.done():
            pending.cancel()
        self._reconnect_task = asyncio.create_task(self._reconnect_after(retry_ms / 1000.0))

    async def _reconnect_after(self, delay_s: float) -> None:
        try:
            await self._sleep(delay_s)
        except asyncio.CancelledError:
            return
        self._reconnect_task = None
        if self._disposed:
            return
        try:
            await self.connect()
        except Exception as err:
            # connect() already emitted and rescheduled; a repeat here would
            # double the error for the same failure.
            _log.debug("realtime reconnect attempt failed: %s", err)

    def _compute_reconnect_delay_ms(self, attempt: int) -> int:
        """Exponential from ``reconnect_interval_ms``, capped at
        ``max_reconnect_interval_ms`` — or at least five minutes once the
        outage is long — then jittered up to 25 % below, so the cap holds."""
        exp = self._opts.reconnect_interval_ms * (2 ** min(attempt - 1, 20))
        cap = self._opts.max_reconnect_interval_ms
        if attempt > _LONG_OUTAGE_AFTER_ATTEMPTS:
            cap = max(cap, _LONG_OUTAGE_MAX_INTERVAL_MS)
        return _jittered_below_ms(min(exp, cap), _BACKOFF_JITTER)

    def _compute_auth_retry_delay_ms(self) -> int:
        """The blocked cadence: 12.5 min ±20 % → 10-15 min."""
        return _jittered_ms(_AUTH_RETRY_INTERVAL_MS, _AUTH_RETRY_JITTER)

    # ─── Handler registration ────────────────────────────────────────

    def on(self, event: str, handler: MessageHandler) -> Callable[[], None]:
        """Register a handler for a server event. Returns an unsubscribe fn.

        ``on("wake", handler)`` receives a validated ``wake`` frame
        (``payload`` parses as :class:`~agentchatme.types.WakePayload`):
        a content-free "check AgentChat" signal that is never acknowledged.
        ``on("heartbeat", handler)`` receives the server's heartbeat frames
        when the ``heartbeat`` capability was negotiated (informational: the
        idle watchdog already counts them).

        Server-frame handlers (``message.new``, ``wake``, everything the
        server sends) run off the receive loop: a sync handler runs at once,
        an async one is started at once and runs as its own task, so a slow
        handler never holds up the next frame, another frame's ack, or the
        transport's pongs. Handlers are started in arrival order (per
        conversation: seq order); an async handler that needs the previous
        message's handler finished first serialises itself. A ``message.new``
        is acknowledged once every handler for it settled cleanly (unless
        ``ack_on_receipt``), and never when no ``message.new`` handler is
        attached.

        Three events are raised by the client itself, in the same shape (a
        server frame carrying one of these types is dropped, never dispatched):

        ``on("state", handler)``
            Every transition of :attr:`state`. Receives
            ``{"type": "state", "state": RealtimeState, "previous": RealtimeState}``.
        ``on("blocked", handler)``
            The server rejected the key during the handshake. Receives
            ``{"type": "blocked", "code": int, "reason": str,
            "retry_in_ms": int | None}``; ``retry_in_ms`` is the slow retry
            now armed (``None`` with ``reconnect=False``). Fires once per
            rejection, so once per 10-15 min while the key stays rejected.
        ``on("stopped", handler)``
            :meth:`disconnect` ran; receives ``{"type": "stopped"}``. The
            last event this instance ever raises.
        """
        first_message_handler = event == "message.new" and not self._handlers.get(event)
        handlers = self._handlers.setdefault(event, set())
        handlers.add(handler)
        if first_message_handler:
            self._maybe_renegotiate_ack()

        def off() -> None:
            hs = self._handlers.get(event)
            if hs is None:
                return
            hs.discard(handler)
            if not hs:
                self._handlers.pop(event, None)

        return off

    def on_error(self, handler: ErrorHandler) -> Callable[[], None]:
        self._error_handlers.add(handler)
        return lambda: self._error_handlers.discard(handler)

    def on_connect(self, handler: ConnectHandler) -> Callable[[], None]:
        """Fires each time the HELLO handshake completes (initial + reconnects)."""
        self._connect_handlers.add(handler)
        return lambda: self._connect_handlers.discard(handler)

    def on_disconnect(self, handler: DisconnectHandler) -> Callable[[], None]:
        """Fires on every socket close, regardless of reason."""
        self._disconnect_handlers.add(handler)
        return lambda: self._disconnect_handlers.discard(handler)


    async def send(self, message: dict[str, Any]) -> None:
        """Send a client-initiated frame (e.g. ``typing.start``)."""
        if self._ws is None or not self._authenticated:
            raise _RealtimeConnectionError("WebSocket is not connected")
        try:
            await self._ws.send(json.dumps(message))
        except Exception as err:
            raise _RealtimeConnectionError(f"send failed: {err}") from err

    @property
    def is_connected(self) -> bool:
        return bool(self._authenticated and self._ws is not None)

    @property
    def state(self) -> RealtimeState:
        """Where the client is in its lifecycle; see :data:`RealtimeState`."""
        return self._state

    @property
    def blocked_reason(self) -> str | None:
        """The last auth rejection as ``"<code> <reason>"`` (for example
        ``"1008 Invalid token"``), or ``None``. Set when the client enters
        ``blocked``; it stays set through a transport failure during the
        slow retry and clears on the next ``hello.ok``."""
        return self._blocked_reason

    @property
    def capabilities(self) -> frozenset[str]:
        """The capabilities the current connection's ``hello.ok`` echoed
        (``ack``, ``heartbeat``, ``rewake``, …). Empty between connections
        and against a legacy server."""
        return self._negotiated

    @property
    def rate_limited_ms(self) -> int | None:
        """The delay the server's connect budget asked for on the last
        refused attempt (a close 1013 with ``retry_after``), until the next
        ``hello.ok``; ``None`` otherwise."""
        return self._rate_limited_ms

    @property
    def ack_negotiated(self) -> bool:
        """True while the current connection's ``hello.ok`` echoed the
        ``ack`` capability: the server keeps envelopes ``stored`` until this
        client acknowledges them. False between connections and against a
        legacy server (envelopes marked on push)."""
        return self._ack_negotiated

    async def disconnect(self) -> None:
        """Close the socket, disable reconnect, and release every task,
        timer and handler this instance owns. The only way into ``stopped``.

        Buffered out-of-order envelopes are flushed to handlers first (see
        :meth:`_flush_parked_frames`), then the socket closes, then every
        background task (backoff or blocked-retry timer, handshake and idle
        watchdogs, stability timer, gap timers, drain) is cancelled **and
        awaited**, so nothing of this instance survives in the loop. Fires
        ``on("stopped")`` last. After this, :meth:`connect` raises — create
        a fresh :class:`RealtimeClient` to reopen.
        """
        self._disposed = True
        self._opts.reconnect = False
        await self._set_state("stopped")

        current = asyncio.current_task()
        owned: list[asyncio.Future[Any]] = []
        for task in (
            self._reconnect_task,
            self._handshake_task,
            self._liveness_task,
            self._stability_task,
        ):
            if task is not None:
                owned.append(task)
        self._reconnect_task = None
        self._handshake_task = None
        self._liveness_task = None
        self._stability_task = None
        for state in self._order_states.values():
            if state.gap_task is not None:
                owned.append(state.gap_task)
                state.gap_task = None
        owned.extend(self._bg_tasks)
        self._bg_tasks.clear()

        await self._flush_parked_frames("gap_fill_unavailable")

        # Handlers run off the receive loop; the frames just flushed and any
        # still running get a short grace to finish, then are cancelled.
        # (A handler may be the one calling disconnect(): it is skipped, and
        # so is the settlement waiting on it.)
        await self._release_dispatches(current)

        ws = self._ws
        self._ws = None
        if ws is not None:
            # Ours: on_disconnect reports initiator "client".
            await self._close_socket(ws, 1000, "client disconnect")

        recv_task = self._recv_task
        self._recv_task = None
        if recv_task is not None and recv_task is not current:
            with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError, Exception):
                await asyncio.wait_for(recv_task, timeout=1.0)

        await _release_tasks(owned, current)

        self._authenticated = False
        self._ack_negotiated = False
        self._negotiated = frozenset()
        await self._dispatch_local({"type": "stopped"})
        self._handlers.clear()
        self._error_handlers.clear()
        self._connect_handlers.clear()
        self._disconnect_handlers.clear()

    # ─── Dispatch ────────────────────────────────────────────────────

    async def _release_dispatches(self, current: asyncio.Future[Any] | None) -> None:
        """disconnect(): let running lanes (and on_connect handlers) finish
        within the grace, then cancel the rest; a cancelled lane records its
        frames as not handled, so nothing is acked for them. The lane whose
        handler is calling disconnect() is skipped."""
        workers = [
            lane.worker
            for lane in list(self._lanes.values())
            if lane.worker is not None and lane.worker is not current and not lane.worker.done()
        ]
        connects = [t for t in self._dispatch_tasks if t is not current and not t.done()]
        running: list[asyncio.Future[Any]] = [*workers, *connects]
        if running:
            with contextlib.suppress(Exception):
                await asyncio.wait(running, timeout=_DISPOSE_GRACE_S)
        await _release_tasks(running, current)
        self._dispatch_tasks.clear()

    async def _emit_error(self, error: BaseException) -> None:
        # ``_invoke`` isolates each handler: a raising ``on_error`` must never
        # break the step that follows a notice (arming a watchdog close, the
        # backoff, the blocked retry) — that would be the one way into a
        # parked client.
        for h in list(self._error_handlers):
            await _invoke(h, error)

    def _emit_connect(self) -> None:
        """Start every ``on_connect`` handler; none of them holds up the
        receive loop (the server's reconnect backlog follows hello.ok)."""
        for h in list(self._connect_handlers):
            self._start_handler(h, (), fire_and_forget=True)

    async def _emit_disconnect(self, info: dict[str, Any]) -> None:
        for h in list(self._disconnect_handlers):
            await _invoke(h, info)

    async def _dispatch_wake(self, message: dict[str, Any]) -> None:
        """Validate a ``wake`` frame's payload, then dispatch it to ``on("wake")``.

        A frame the server sent malformed is surfaced through ``on_error``
        rather than handed to handlers as something it is not.
        """
        problem = wake_payload_problem(
            message.get("payload"), rewake="rewake" in self._negotiated
        )
        if problem is not None:
            await self._emit_error(
                _RealtimeConnectionError(f"wake frame failed validation: {problem}")
            )
            return
        self._dispatch_frame(message)

    async def _dispatch_local(self, message: dict[str, Any]) -> None:
        """Run the handlers of an event this client raises itself (``state``,
        ``blocked``, ``stopped``), awaiting each: they belong to the control
        flow that raised them. Handler exceptions are logged, never raised."""
        event = message.get("type")
        if not isinstance(event, str):
            return
        for h in list(self._handlers.get(event, ())):
            await _invoke(h, message)

    def _dispatch_frame(self, message: dict[str, Any]) -> None:
        """Hand a server frame (not ``message.new``) to its handlers on its
        lane, without waiting for them. Failures are logged."""
        event = message.get("type")
        if not isinstance(event, str):
            return
        handlers = list(self._handlers.get(event, ()))
        if not handlers:
            return
        self._submit(_lane_of(message), _Job(message=message, handlers=handlers))

    # ─── Lanes ───────────────────────────────────────────────────────

    def _submit(self, key: str, job: _Job) -> bool:
        """Run ``job`` on lane ``key``. On an idle lane its handlers start at
        once, inline, and a sync handler finishes before this returns;
        ``True`` when every handler finished that way. Otherwise the lane's
        worker carries the job (behind the jobs already waiting there) and
        ``False`` is returned."""
        lane = self._lanes.get(key)
        if lane is None or (lane.worker is None and not lane.jobs):
            if self._run_inline(job):
                return True
            lane = self._lanes.setdefault(key, _Lane())
        lane.jobs.append(job)
        if lane.worker is None:
            lane.worker = asyncio.ensure_future(self._lane_worker(key, lane))
        return False

    def _run_inline(self, job: _Job) -> bool:
        """Call the job's handlers now, in order, until one is async: that
        one is started as a task the lane's worker will wait for."""
        while job.next_index < len(job.handlers):
            handler = job.handlers[job.next_index]
            job.next_index += 1
            try:
                result = handler(job.message)
            except Exception:
                job.ok = False
                _log.warning("realtime handler raised", exc_info=True)
                continue
            if inspect.isawaitable(result):
                job.awaiting = result
                return False
        return True

    async def _run_job(self, job: _Job) -> None:
        """Finish the job's handlers, one at a time."""
        if job.awaiting is not None:
            awaiting, job.awaiting = job.awaiting, None
            try:
                await awaiting
            except asyncio.CancelledError:
                _discard_awaitable(awaiting)
                raise
            except Exception:
                job.ok = False
                _log.warning("realtime handler raised", exc_info=True)
        while job.next_index < len(job.handlers):
            handler = job.handlers[job.next_index]
            job.next_index += 1
            try:
                result = handler(job.message)
                if inspect.isawaitable(result):
                    await result
            except asyncio.CancelledError:
                raise
            except Exception:
                job.ok = False
                _log.warning("realtime handler raised", exc_info=True)

    async def _lane_worker(self, key: str, lane: _Lane) -> None:
        """Run the lane's jobs one at a time. A job holds the lane until its
        handlers finished, or ``in_flight_adopt_ms`` at most: a handler that
        never finishes keeps running, but no longer holds its conversation's
        next message back."""
        bound_s = max(0.0, self._opts.in_flight_adopt_ms / 1000.0)
        try:
            while lane.jobs:
                job = lane.jobs[0]
                run: asyncio.Future[None] = asyncio.ensure_future(self._finish_job(job))
                job.run = run
                self._dispatch_tasks.add(run)
                run.add_done_callback(self._dispatch_tasks.discard)
                run.add_done_callback(_consume_future)
                done, _ = await asyncio.wait({run}, timeout=bound_s)
                lane.jobs.popleft()
                if not done:
                    _log.warning(
                        "realtime: a message.new handler has run past %.0fs; its"
                        " conversation's next message goes on (the handler keeps running)",
                        bound_s,
                    )
        except asyncio.CancelledError:
            # disconnect() released the lane: what never started was not
            # handled (a job already running is its own task's to settle).
            for job in lane.jobs:
                if job.run is not None:
                    continue
                _discard_awaitable(job.awaiting)
                job.awaiting = None
                await self._settle_job(job, ok=False, ack=False)
            lane.jobs.clear()
            raise
        finally:
            lane.worker = None
            if not lane.jobs and self._lanes.get(key) is lane:
                del self._lanes[key]

    async def _finish_job(self, job: _Job) -> None:
        try:
            await self._run_job(job)
        except asyncio.CancelledError:
            await self._settle_job(job, ok=False, ack=False)
            raise
        await self._settle_job(job, ok=job.ok)

    async def _settle_job(self, job: _Job, *, ok: bool, ack: bool = True) -> None:
        if not job.message_new:
            return
        self._finish_dispatch_sync(job.message_id, job.own, ok=ok)
        if ok and ack and job.ws_ack and job.message_id is not None and not self._disposed:
            await self._send_delivery_ack(job.message_id)
        if job.settlement is not None and not job.settlement.done():
            job.settlement.set_result(ok)

    def _has_message_handler(self) -> bool:
        return bool(self._handlers.get("message.new"))

    def _start_handler(
        self, handler: Any, args: tuple[Any, ...], *, fire_and_forget: bool
    ) -> asyncio.Future[Any] | bool:
        """Call one user handler now.

        A sync handler runs to completion here: ``True`` when it returned,
        ``False`` when it raised (logged). An async handler is started as its
        own task, owned by this client, and the task is returned; with
        ``fire_and_forget`` its failure is logged when it ends.
        """
        try:
            result = handler(*args)
        except Exception:
            _log.warning("realtime handler raised", exc_info=True)
            return False
        if not inspect.isawaitable(result):
            return True
        task: asyncio.Future[Any] = asyncio.ensure_future(result)
        self._dispatch_tasks.add(task)
        task.add_done_callback(self._dispatch_tasks.discard)
        if fire_and_forget:
            task.add_done_callback(_log_handler_failure)
        return task

    async def _deliver_message_new(
        self, message: dict[str, Any], *, ws_ack: bool
    ) -> asyncio.Future[bool]:
        """Single delivery choke point for ``message.new`` envelopes.

        Every path that hands an envelope to handlers — live in-order,
        buffered drain, gap resolution, REST drain injection, shutdown
        flush — funnels through here so dedup and delivery acks behave
        identically everywhere. Returns a future for the envelope's
        settlement without waiting for the handlers:

        - **Dedup** (bounded LRU on ``message_id``, live + drain): a hit
          skips dispatch but still acks — the prior successful dispatch is
          the proof of processing. Settles ``True`` at once.
        - **No handler attached**: nothing will ever see the frame, so it is
          neither acked nor remembered. Settles ``False`` at once (the drain
          cursor stops; the server keeps the envelope).
        - **In flight**: a copy whose first copy is still with the handlers
          — on this connection or an earlier one — is not dispatched again;
          it takes the first copy's outcome (acked once that finished
          cleanly), never waiting for it.
        - **Ack-after-dispatch**: with the ``ack`` capability negotiated,
          confirm ``{"type": "ack", "message_id": ...}`` only after every
          handler (async ones run as their own tasks) completed without
          raising. A handler exception ⇒ no ack ⇒ the envelope stays
          ``stored`` server-side and re-arrives via drain/redelivery.

        The future resolves ``True`` when the envelope is safe to
        acknowledge (clean dispatch or dedup hit), ``False`` otherwise. The
        REST drain reads it through :attr:`_drain_settlements` before it
        moves the sync/ack cursor.
        """
        loop = asyncio.get_running_loop()
        message_id = _extract_message_id(message)
        handlers = list(self._handlers.get("message.new", ()))
        first = self._adoptable(message_id)

        if first is None and not handlers:
            # No handler attached: nothing on this machine received the
            # message. Not acked on any path (the drain cursor stops here
            # too) and not remembered, so the server keeps offering it until
            # a handler is there.
            return self._settle_drain_row(message, _resolved(loop, False))

        if message_id is not None and self._dedup.hit(message_id):
            if ws_ack:
                await self._send_delivery_ack(message_id)
            return self._settle_drain_row(message, _resolved(loop, True))

        adopted = self._adopt_in_flight(message, ws_ack=ws_ack)
        if adopted is not None:
            return adopted

        own: _InFlight | None = None
        if message_id is not None and self._dedup.enabled:
            own = _InFlight(loop.create_future(), self._generation, time.monotonic())
            self._inflight[message_id] = own

        job = _Job(
            message=message,
            handlers=handlers,
            message_new=True,
            message_id=message_id,
            own=own,
            ws_ack=ws_ack,
        )
        if self._submit(_lane_of(message), job):
            # Every handler was sync and has run on an idle lane: settle in
            # this step, so the ack goes out in arrival order.
            await self._settle_job(job, ok=job.ok)
            return self._settle_drain_row(message, _resolved(loop, job.ok))
        settlement: asyncio.Future[bool] = loop.create_future()
        job.settlement = settlement
        return self._settle_drain_row(message, settlement)

    def _finish_dispatch_sync(
        self, message_id: str | None, own: _InFlight | None, *, ok: bool
    ) -> None:
        """Record a dispatch's outcome. In one step: the id enters the dedup
        cache (on success) and leaves the in-flight map, so a later copy finds
        one or the other, never neither."""
        if ok and message_id is not None:
            self._dedup.add(message_id)
        if own is not None and message_id is not None:
            if self._inflight.get(message_id) is own:
                del self._inflight[message_id]
            if not own.outcome.done():
                own.outcome.set_result(ok)

    def _adopt_in_flight(
        self, message: dict[str, Any], *, ws_ack: bool
    ) -> asyncio.Future[bool] | None:
        """Take the outcome of a copy of this message whose handlers are still
        running, instead of dispatching it again — whichever connection the
        first copy came in on (a reconnect re-offers an unacked frame while
        its first handler may still run).

        Never waits: progress beats dedup. A live copy is acknowledged once
        the first copy finished cleanly (``ws_ack``), never if it failed; a
        drain row's cursor reads that outcome when it is computed and stops
        if the first copy has not finished. Returns the first copy's outcome
        when adopted, ``None`` otherwise.
        """
        message_id = _extract_message_id(message)
        if message_id is None:
            return None
        first = self._adoptable(message_id)
        if first is None:
            return None
        if id(message) in self._drain_settlements:
            self._drain_adopted[id(message)] = first.outcome
        if ws_ack:

            def ack_if_handled(outcome: asyncio.Future[bool]) -> None:
                if outcome.cancelled() or not outcome.result() or self._disposed:
                    return
                self._spawn(self._send_delivery_ack(message_id))

            first.outcome.add_done_callback(ack_if_handled)
        return first.outcome

    def _adoptable(self, message_id: str | None) -> _InFlight | None:
        """The running first copy a repeat may adopt: one on this connection,
        or one on an earlier connection that has run less than
        ``in_flight_adopt_ms`` (past that, a handler that never finishes must
        not swallow every re-offer: the repeat is dispatched afresh)."""
        if message_id is None:
            return None
        running = self._inflight.get(message_id)
        if running is None:
            return None
        if running.generation == self._generation:
            return running
        age_ms = (time.monotonic() - running.started_at) * 1000.0
        return running if age_ms < self._opts.in_flight_adopt_ms else None

    def _settle_drain_row(
        self, message: dict[str, Any], settlement: asyncio.Future[bool]
    ) -> asyncio.Future[bool]:
        key = id(message)
        if key in self._drain_settlements:
            self._drain_settlements[key] = settlement
        return settlement

    async def _send_delivery_ack(self, message_id: str) -> None:
        """Send a delivery-ack frame if this connection negotiated ack-mode.

        Best-effort by design: a lost ack (socket died mid-send) leaves
        the envelope ``stored`` server-side; it re-arrives on the next
        drain/redelivery, where the dedup cache turns it into
        skip-dispatch + re-ack. Never raises into the dispatch pipeline.
        """
        if not self._ack_negotiated:
            return
        ws = self._ws
        if ws is None or not self._authenticated:
            return
        try:
            await ws.send(json.dumps({"type": "ack", "message_id": message_id}))
        except Exception:
            _log.debug(
                "delivery ack send failed for %s", message_id, exc_info=True
            )

    def _spawn(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        """Start an owned background task. ``disconnect()`` cancels and awaits
        every task still in ``_bg_tasks``; a finished task removes itself."""
        task: asyncio.Task[Any] = asyncio.create_task(coro)
        self._bg_tasks.add(task)
        task.add_done_callback(self._bg_tasks.discard)
        return task

    # ─── Per-conversation seq ordering ───────────────────────────────
    #
    # Invariant: for any conversation_id, handlers see message.new envelopes
    # in strict seq-ascending order, no gaps and no repeats (modulo the
    # gap-fill-failed path, where we surface ``recovered=False`` and
    # advance the cursor past the hole).

    async def _process_ordered_message(
        self, message: dict[str, Any], *, ws_ack: bool = False, ack_spent: bool = False
    ) -> _Routed:
        """Route one ``message.new`` envelope through seq ordering.

        ``ws_ack`` tags the envelope's origin (live socket vs drain/gap
        injection) and follows it through the buffer — see
        :class:`_PendingEnvelope`. ``ack_spent`` says the frame was already
        acknowledged at parse (``ack_on_receipt``): the server has it
        `delivered` and will never re-offer it, so a shutdown or close flush
        must hand it over rather than drop it.

        Returns where the envelope went: ``"delivered"`` (handed to
        :meth:`_deliver_message_new` now — in order, or late when it is
        below this connection's seq anchor) or ``"buffered"`` (parked on a
        seq gap; dispatched later by the gap machinery or the shutdown
        flush). The handlers themselves run off this call.
        """
        payload = message.get("payload")
        if not isinstance(payload, dict):
            await self._deliver_message_new(message, ws_ack=ws_ack)
            return "delivered"
        conversation_id = payload.get("conversation_id")
        if not isinstance(conversation_id, str):
            await self._deliver_message_new(message, ws_ack=ws_ack)
            return "delivered"

        seq = _extract_seq(message)
        if seq is None:
            await self._deliver_message_new(message, ws_ack=ws_ack)
            return "delivered"

        state = self._order_states.get(conversation_id)
        if state is None:
            state = _OrderState()
            self._order_states[conversation_id] = state

        # First arrival for this conversation in this connection — anchor.
        if state.next_expected_seq is None:
            state.next_expected_seq = seq + 1
            await self._deliver_message_new(message, ws_ack=ws_ack)
            return "delivered"

        if seq < state.next_expected_seq:
            # Below this connection's anchor. With a first-arrival anchor that
            # means "older than the first frame this connection saw", NOT
            # "already dispatched": the server registers the socket before it
            # drains the stored backlog, so a live frame can anchor a
            # conversation at seq 5 and the drain then brings stored seqs 3
            # and 4 that no handler has seen. Dropping them would stall them:
            # unacked, they come back on the next drain, behind a fresh live
            # anchor again, and the drain cursor could never pass them; acked
            # at parse, they would be lost. The same holds for a seq a failed
            # gap fill skipped that arrives late. So hand the frame over and
            # let the dedup cache — the only evidence of a prior dispatch —
            # decide: a hit skips the handlers (and acks again), a copy still
            # with the handlers is adopted, a miss is dispatched late, out of
            # order but never lost (the TypeScript SDK's rule).
            await self._deliver_message_new(message, ws_ack=ws_ack)
            return "delivered"

        if seq == state.next_expected_seq:
            # Dispatched, or adopted by a copy of this message whose handler
            # is still running (never waited for). Either way the cursor moves
            # on now, so the next frame is not taken for a gap; that frame's
            # handler may then start while the first copy's still runs.
            await self._deliver_message_new(message, ws_ack=ws_ack)
            # Never backwards: another copy of this message (or a drain of
            # the buffer) may have moved the cursor on during the await.
            _advance_past(state, seq)
            await self._drain_consecutive(conversation_id, state)
            self._maybe_clear_gap_timer(state)
            self._cleanup_if_idle(conversation_id, state)
            return "delivered"

        # seq > next_expected_seq — out of order. Buffer + arm gap timer.
        state.buffer[seq] = _PendingEnvelope(message=message, ws_ack=ws_ack, acked=ack_spent)

        if len(state.buffer) > _MAX_BUFFERED_PER_CONVERSATION:
            await self._resolve_gap(
                conversation_id,
                state,
                recovered=False,
                reason="buffer_overflow",
                buffered_seq=_min_buffered_seq(state),
            )
            return "buffered"

        if state.gap_task is None:
            self._open_gap(conversation_id, state)
        return "buffered"

    def _open_gap(self, conversation_id: str, state: _OrderState) -> None:
        state.gap_started_at = time.monotonic()
        state.gap_started_expected_seq = state.next_expected_seq
        # Owned through _bg_tasks as well: the handler clears
        # `state.gap_task` once the timer has fired, but the same task
        # then runs the gap-fill fetch, and disconnect() must still be
        # able to cancel and await it mid-fetch.
        state.gap_task = self._spawn(self._gap_timer(conversation_id))

    async def _gap_timer(self, conversation_id: str) -> None:
        try:
            await asyncio.sleep(_GAP_FILL_WINDOW_S)
        except asyncio.CancelledError:
            return
        await self._handle_gap_timer(conversation_id)

    async def _handle_gap_timer(self, conversation_id: str) -> None:
        state = self._order_states.get(conversation_id)
        if state is None:
            return
        state.gap_task = None

        # Race: missing seq may have arrived between timer firing and now.
        if not state.buffer:
            self._cleanup_if_idle(conversation_id, state)
            return

        expected_seq = state.next_expected_seq
        if expected_seq is None:
            return  # defensive

        if self._opts.client is None:
            await self._resolve_gap(
                conversation_id,
                state,
                recovered=False,
                reason="gap_fill_unavailable",
                buffered_seq=_min_buffered_seq(state),
            )
            return

        if state.gap_fill_in_flight:
            return
        state.gap_fill_in_flight = True

        fetched: list[dict[str, Any]] = []
        fill_error = False
        try:
            # ``after_seq`` is exclusive, so subtract 1 to include ``expected_seq``.
            fetched = await self._opts.client.get_messages(
                conversation_id,
                after_seq=expected_seq - 1,
                limit=_GAP_FILL_LIMIT,
            )
        except Exception:
            fill_error = True
        finally:
            state.gap_fill_in_flight = False

        # State may have been reset (a close during the await), or
        # disconnect() may have begun: then take nothing in. The fetched rows
        # are neither acked nor handed over (a gap fill marks nothing on the
        # server, which keeps them); acking them now would lose them when the
        # shutdown cancels this task mid-handover.
        state_now = self._order_states.get(conversation_id)
        if self._disposed or state_now is not state:
            return

        if fill_error:
            await self._resolve_gap(
                conversation_id,
                state,
                recovered=False,
                reason="gap_fill_failed",
                buffered_seq=_min_buffered_seq(state),
            )
            return

        for row in fetched or []:
            if not isinstance(row, dict):
                continue
            row_seq = row.get("seq")
            if not isinstance(row_seq, int) or isinstance(row_seq, bool):
                continue
            if row_seq < expected_seq:
                continue
            if row_seq in state.buffer:
                continue
            # Gap-fill rows ride the live ack path: in the default mode they
            # are acked after dispatch like any live frame; with
            # ack_on_receipt the fetch IS this machine's receipt, so ack now
            # and spend it. Either way the server's envelope (if any) is
            # settled here instead of waiting for a later drain.
            envelope: dict[str, Any] = {"type": "message.new", "payload": row}
            # No handler attached: nothing would see the row, so no receipt
            # either; it rides the ack-after-dispatch path instead.
            receipt = self._opts.ack_on_receipt and self._has_message_handler()
            if receipt:
                fetched_id = _extract_message_id(envelope)
                if fetched_id is not None:
                    await self._send_delivery_ack(fetched_id)
            state.buffer[row_seq] = _PendingEnvelope(
                message=envelope,
                ws_ack=not receipt,
                acked=receipt,
            )

        drained = await self._drain_consecutive(conversation_id, state)
        if drained:
            await self._resolve_gap(
                conversation_id,
                state,
                recovered=True,
                reason="gap_filled",
                buffered_seq=None,
            )
        else:
            await self._resolve_gap(
                conversation_id,
                state,
                recovered=False,
                reason="gap_fill_failed",
                buffered_seq=_min_buffered_seq(state),
            )

    async def _drain_consecutive(
        self, conversation_id: str, state: _OrderState
    ) -> bool:
        if state.next_expected_seq is None:
            return False
        drained = False
        while state.next_expected_seq in state.buffer:
            seq = state.next_expected_seq
            pending = state.buffer.pop(seq)
            await self._deliver_message_new(pending.message, ws_ack=pending.ws_ack)
            _advance_past(state, seq)
            drained = True
        if drained:
            self._cleanup_if_idle(conversation_id, state)
        return drained

    async def _resolve_gap(
        self,
        conversation_id: str,
        state: _OrderState,
        *,
        recovered: bool,
        reason: GapReason,
        buffered_seq: int | None,
    ) -> None:
        if state.gap_started_expected_seq is not None:
            expected_seq = state.gap_started_expected_seq
        elif state.next_expected_seq is not None:
            expected_seq = state.next_expected_seq
        else:
            expected_seq = 0
        gap_ms = (
            int((time.monotonic() - state.gap_started_at) * 1000)
            if state.gap_started_at is not None
            else 0
        )

        # Hand over the frames parked when the gap resolved. Each leaves the
        # buffer, and the cursor moves past it, before its handler is
        # awaited: the receive loop keeps running meanwhile, and a frame it
        # gets in order is dispatched there directly, while one behind a new
        # hole parks with a gap window of its own (below). Nothing is
        # cleared: under ack_on_receipt a parked frame is already acked. An
        # entry another caller took meanwhile is skipped.
        entry_gap_task = state.gap_task
        state.resolving += 1
        try:
            for seq in sorted(state.buffer):
                await self._hand_over(state, seq)
            # Frames that parked behind those and are now next in line.
            await self._hand_over_in_order(state)
        finally:
            state.resolving -= 1

        if (
            state.buffer
            and state.gap_task is not None
            and state.gap_task is not entry_gap_task
        ):
            # A newer gap opened while handlers ran; its window, fill and
            # report are its own.
            pass
        else:
            if state.gap_task is not None:
                state.gap_task.cancel()
                state.gap_task = None
            state.gap_started_at = None
            state.gap_started_expected_seq = None
            if state.buffer:
                # Still parked behind a hole that opened meanwhile.
                self._open_gap(conversation_id, state)

        if self._opts.on_sequence_gap is not None:
            info = SequenceGapInfo(
                conversation_id=conversation_id,
                expected_seq=expected_seq,
                buffered_seq=buffered_seq,
                gap_ms=gap_ms,
                recovered=recovered,
                reason=reason,
            )
            await _invoke(self._opts.on_sequence_gap, info)

        self._cleanup_if_idle(conversation_id, state)

    async def _hand_over(self, state: _OrderState, seq: int) -> None:
        """Take the frame parked at ``seq`` (if still there), move the cursor
        past it, then hand it over."""
        pending = state.buffer.pop(seq, None)
        if pending is None:
            return
        _advance_past(state, seq)
        await self._deliver_message_new(pending.message, ws_ack=pending.ws_ack)

    async def _hand_over_in_order(self, state: _OrderState) -> None:
        """Hand over the parked frames that are next in line. The cursor
        moves before each handler is awaited, so a frame arriving in order
        meanwhile goes to the receive loop rather than lengthening this
        loop."""
        while True:
            seq = state.next_expected_seq
            if seq is None or seq not in state.buffer:
                return
            await self._hand_over(state, seq)

    def _maybe_clear_gap_timer(self, state: _OrderState) -> None:
        if state.gap_task is not None and not state.buffer:
            state.gap_task.cancel()
            state.gap_task = None
            state.gap_started_at = None
            state.gap_started_expected_seq = None

    def _cleanup_if_idle(self, conversation_id: str, state: _OrderState) -> None:
        if (
            not state.buffer
            and state.gap_task is None
            and not state.gap_fill_in_flight
            and state.resolving == 0
            # A state a socket close replaced while this caller was awaiting
            # a handler is not the conversation's state any more.
            and self._order_states.get(conversation_id) is state
        ):
            self._order_states.pop(conversation_id, None)

    def _reset_order_states(self) -> None:
        for state in self._order_states.values():
            if state.gap_task is not None:
                state.gap_task.cancel()
        self._order_states.clear()

    async def _flush_parked_frames(self, reason: GapReason) -> None:
        """Hand every parked frame to the handlers in seq order per
        conversation, cancel the gap timers and clear the ordering state.

        Used when frames must not be dropped: on :meth:`disconnect` (the
        caller is leaving) and, under ``ack_on_receipt``, on every socket
        close (the frames are already acked). No gap-fill: an in-flight
        HTTP request would race the close.
        """
        # Works on the frames parked now. On a close the receive loop is the
        # caller, so no frame arrives; disconnect() stops taking frames in
        # before it flushes. Each frame leaves the buffer before its handler
        # is awaited, so one another caller took meanwhile is skipped.
        for state in self._order_states.values():
            if state.gap_task is not None:
                state.gap_task.cancel()
                state.gap_task = None
        for conversation_id, state in list(self._order_states.items()):
            seqs = sorted(state.buffer)
            for seq in seqs:
                await self._hand_over(state, seq)
            if seqs:
                await self._report_flush(conversation_id, state, seqs[0], reason)
        # A frame acked at receipt is never dropped: if one parked during the
        # awaits above (it cannot keep happening: nothing new is acked now),
        # hand it over too. The rest are unacked REST rows the drain routed
        # meanwhile; the server re-offers them.
        for state in list(self._order_states.values()):
            for seq in sorted(state.buffer):
                pending = state.buffer.get(seq)
                if pending is not None and pending.acked:
                    await self._hand_over(state, seq)
        for state in self._order_states.values():
            if state.gap_task is not None:
                state.gap_task.cancel()
                state.gap_task = None
        self._order_states.clear()

    async def _report_flush(
        self, conversation_id: str, state: _OrderState, first_seq: int, reason: GapReason
    ) -> None:
        if self._opts.on_sequence_gap is None:
            return
        if state.gap_started_expected_seq is not None:
            expected_seq = state.gap_started_expected_seq
        elif state.next_expected_seq is not None:
            expected_seq = state.next_expected_seq
        else:
            expected_seq = 0
        info = SequenceGapInfo(
            conversation_id=conversation_id,
            expected_seq=expected_seq,
            buffered_seq=first_seq,
            gap_ms=(
                int((time.monotonic() - state.gap_started_at) * 1000)
                if state.gap_started_at is not None
                else 0
            ),
            recovered=False,
            reason=reason,
        )
        await _invoke(self._opts.on_sequence_gap, info)


# ───────────────────────── Module helpers ─────────────────────────


_log = logging.getLogger("agentchat.realtime")


async def _invoke(handler: Any, arg: Any) -> bool:
    """Call a user handler (awaiting async ones). Returns ``False`` on raise.

    User-hook exceptions must not break the recv/reconnect loop, but they
    shouldn't vanish silently either — they're logged so apps can route
    them through their normal observability stack, and the ``False``
    return lets ack-mode withhold the delivery ack (handler failure ⇒
    server-side redelivery) without changing dispatch fan-out.
    """
    try:
        result = handler(arg)
        if inspect.isawaitable(result):
            await result
    except Exception:
        _log.warning("realtime handler raised", exc_info=True)
        return False
    return True


_NO_CONVERSATION_LANE = "events"


def _discard_awaitable(awaitable: Any) -> None:
    """Drop what an async handler returned without running it (its lane was
    released first): close a coroutine, cancel a future."""
    if awaitable is None:
        return
    if inspect.iscoroutine(awaitable):
        awaitable.close()
    elif isinstance(awaitable, asyncio.Future):
        awaitable.cancel()


def _lane_of(message: dict[str, Any]) -> str:
    """The dispatch lane of a frame: its conversation (``message.new`` and
    every frame that names one), or the one lane for frames of no
    conversation. A ``wake`` is a tray-level signal: the no-conversation
    lane, whatever conversation it names."""
    if message.get("type") != "wake":
        payload = message.get("payload")
        cid = payload.get("conversation_id") if isinstance(payload, dict) else None
        if isinstance(cid, str) and cid:
            return f"conversation:{cid}"
    return _NO_CONVERSATION_LANE


def _resolved(loop: asyncio.AbstractEventLoop, value: bool) -> asyncio.Future[bool]:
    future: asyncio.Future[bool] = loop.create_future()
    future.set_result(value)
    return future


def _log_handler_failure(task: asyncio.Future[Any]) -> None:
    """Done-callback for a handler started without anyone awaiting it."""
    if task.cancelled():
        return
    error = task.exception()
    if error is not None:
        _log.warning(
            "realtime handler raised", exc_info=(type(error), error, error.__traceback__)
        )


def wake_payload_problem(payload: Any, *, rewake: bool = True) -> str | None:
    """Why a ``wake`` frame's payload is not one, or ``None`` when it is.

    Without ``rewake`` (a socket that did not negotiate it) the 1.2 rule,
    unchanged: ``reason``, ``conversation_id``, ``anchor_message_id`` and
    ``at`` are non-empty strings and ``step`` an integer.

    With ``rewake``: every wake carries ``reason`` and ``at`` (non-empty
    strings). A ``reminder`` also carries ``conversation_id``,
    ``anchor_message_id`` and an integer ``step``; an ``unanswered`` wake a
    list of conversation ids (``conversation_ids``) and an integer ``step``.
    A reason this SDK does not know yet is forwarded (its ``step``, when
    present, is an integer): any wake means the same thing — read the tray.
    Pure.
    """
    if not isinstance(payload, dict):
        return "payload is not an object"

    def text(key: str) -> str | None:
        value = payload.get(key)
        return None if isinstance(value, str) and value else f"{key} is not a non-empty string"

    step = payload.get("step")
    integer_step = isinstance(step, int) and not isinstance(step, bool)
    if not rewake:
        return (
            text("reason")
            or text("conversation_id")
            or text("anchor_message_id")
            or text("at")
            or (None if integer_step else "step is not an integer")
        )
    problem = text("reason") or text("at")
    if problem is not None:
        return problem
    reason = payload["reason"]
    if reason == "reminder":
        return (
            text("conversation_id")
            or text("anchor_message_id")
            or (None if integer_step else "step is not an integer")
        )
    if reason == "unanswered":
        ids = payload.get("conversation_ids")
        if not isinstance(ids, list) or not all(isinstance(i, str) and i for i in ids):
            return "conversation_ids is not a list of ids"
        return None if integer_step else "step is not an integer"
    return None if "step" not in payload or integer_step else "step is not an integer"


async def _release_tasks(
    tasks: list[asyncio.Future[Any]], current: asyncio.Future[Any] | None
) -> None:
    """Cancel ``tasks`` and wait for them to finish, so a closed client
    leaves nothing behind in the loop. The calling task is skipped: a
    handler may close the client from inside one of these tasks."""
    pending = [task for task in tasks if task is not current and not task.done()]
    for task in pending:
        task.cancel()
    if not pending:
        return
    with contextlib.suppress(asyncio.CancelledError, asyncio.TimeoutError, Exception):
        await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=1.0)


def _extract_seq(message: dict[str, Any]) -> int | None:
    payload = message.get("payload")
    if not isinstance(payload, dict):
        return None
    seq = payload.get("seq")
    if isinstance(seq, int) and not isinstance(seq, bool):
        return seq
    return None


def _extract_message_id(message: dict[str, Any]) -> str | None:
    """Message id from a ``message.new`` frame's payload, if present."""
    payload = message.get("payload")
    if not isinstance(payload, dict):
        return None
    message_id = payload.get("id")
    if isinstance(message_id, str) and message_id:
        return message_id
    return None


def _is_valid_sync_row(row: object) -> bool:
    """Minimal structural validation for one ``/v1/messages/sync`` row.

    Mirrors the reference wire schema: ``id`` and ``conversation_id`` are
    required strings; ``delivery_id`` is a required string-or-null key;
    the optional envelope fields must be well-typed when present. Unknown
    extra fields always pass — additive server changes must never stall
    a drain.
    """
    if not isinstance(row, dict):
        return False
    if not isinstance(row.get("id"), str):
        return False
    if not isinstance(row.get("conversation_id"), str):
        return False
    if "delivery_id" not in row:
        return False
    delivery_id = row["delivery_id"]
    if delivery_id is not None and not isinstance(delivery_id, str):
        return False
    for key in ("sender", "type", "created_at"):
        if key in row and not isinstance(row[key], str):
            return False
    return "content" not in row or isinstance(row["content"], dict)


def _advance_past(state: _OrderState, seq: int) -> None:
    """Move the conversation's cursor past ``seq``, never backwards."""
    current = state.next_expected_seq
    if current is None or current <= seq:
        state.next_expected_seq = seq + 1


def _min_buffered_seq(state: _OrderState) -> int | None:
    if not state.buffer:
        return None
    return min(state.buffer.keys())


__all__ = [
    "ConnectHandler",
    "DisconnectHandler",
    "ErrorHandler",
    "MessageHandler",
    "RealtimeClient",
    "RealtimeOptions",
    "RealtimeState",
    "SequenceGapHandler",
    "SequenceGapInfo",
    "SocketCapabilityOptions",
]
