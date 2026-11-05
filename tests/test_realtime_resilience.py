"""The connection rules of :class:`agentchatme.RealtimeClient`.

No dead state except ``disconnect()``; our own closes are never evidence
about the server; only ``4401``/``4403`` or ``1008 'Invalid token'`` during
the handshake is a rejection; a rejection blocks and retries slowly; the
handshake and idle watchdogs close as client-initiated. The socket is the
in-memory stand-in from ``test_realtime``; the long waits go through the
client's ``_sleep`` hook so a test controls the clock instead of waiting.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest

from agentchatme import ConnectionError as AgentChatConnectionError
from agentchatme import RealtimeClient, _realtime

from .test_realtime import MockWebSocket, _make_client, _settle
from .test_realtime_engine import connected, message_new, other_tasks

# ─────────────── Fakes ───────────────


class PingingWebSocket(MockWebSocket):
    """A socket that answers (or never answers) the client's liveness probe."""

    def __init__(self, *, pong: bool = True) -> None:
        super().__init__()
        self.pong = pong
        self.pings = 0

    async def ping(self) -> asyncio.Future[float]:
        self.pings += 1
        waiter: asyncio.Future[float] = asyncio.get_running_loop().create_future()
        if self.pong:
            waiter.set_result(0.001)
        return waiter


class ControlledSleep:
    """Stands in for the client's reconnect / blocked-retry wait.

    Records every requested delay. With ``hold=True`` each wait parks until
    :meth:`release`; otherwise it yields once and returns, so a run of
    failures plays out in a few loop iterations instead of hours.
    """

    def __init__(self, *, hold: bool = False) -> None:
        self.delays: list[float] = []
        self.hold = hold
        self._gate = asyncio.Event()

    async def __call__(self, delay_s: float) -> None:
        self.delays.append(delay_s)
        if self.hold:
            await self._gate.wait()
            self._gate.clear()
        else:
            await asyncio.sleep(0)

    def release(self) -> None:
        self._gate.set()


def _client_with_sockets(
    sockets: list[MockWebSocket], *, reconnect: bool = True, **opts: Any
) -> tuple[RealtimeClient, list[MockWebSocket]]:
    """A client whose connects hand out ``sockets`` in order (the last one
    repeats), recording each one handed out in the returned list."""
    handed: list[MockWebSocket] = []

    async def fake_connect(_url: str, **_kw: Any) -> MockWebSocket:
        sock = sockets[min(len(handed), len(sockets) - 1)]
        handed.append(sock)
        return sock

    rt = RealtimeClient(
        api_key="sk_test",
        reconnect=reconnect,
        reconnect_interval_ms=10,
        max_reconnect_interval_ms=20,
        websocket_connect=fake_connect,
        **opts,
    )
    return rt, handed


async def _until(predicate: Any, *, timeout: float = 2.0) -> None:
    async def _spin() -> None:
        while not predicate():
            await asyncio.sleep(0)

    await asyncio.wait_for(_spin(), timeout=timeout)


# ─────────────── Close classification (pure) ───────────────


@pytest.mark.parametrize(
    ("code", "reason", "expected"),
    [
        (4401, "", True),
        (4403, "Forbidden", True),
        (1008, "Invalid token", True),
        (1008, "invalid token", True),
        (1008, "HELLO frame required", False),
        (1008, "HELLO frame required first", False),
        (1008, "", False),
        (1008, "Key rotated", False),
        (1008, "Signed out", False),
        (1001, "Heartbeat timeout", False),
        (1006, "", False),
        (1000, "Invalid token", False),
    ],
)
def test_is_auth_rejection_close_contract(code: int, reason: str, expected: bool) -> None:
    assert _realtime._is_auth_rejection_close(code, reason) is expected


# ─────────────── Rule 3: which server closes are rejections ───────────────


@pytest.mark.asyncio
async def test_server_1008_hello_frame_required_is_transient() -> None:
    """The server's own 5 s HELLO timer closes a slow handshake with 1008.
    A good key produces that after one slow leg: normal backoff, then the
    next hello.ok connects."""
    first, second = MockWebSocket(), MockWebSocket()
    rt, handed = _client_with_sockets([first, second])
    blocked: list[dict[str, Any]] = []
    rt.on("blocked", lambda evt: blocked.append(evt))
    try:
        await rt.connect()
        await _settle()
        await first.close(code=1008, reason="HELLO frame required")
        await _until(lambda: len(handed) >= 2)
        await _settle()
        assert rt.state == "connecting"
        assert rt.blocked_reason is None
        assert blocked == []
        await second.push({"type": "hello.ok", "capabilities": ["ack"]})
        await _settle()
        assert rt.state == "ready"
        assert rt.is_connected
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("code", "reason"),
    [(1008, "Invalid token"), (1008, "invalid token"), (4401, ""), (4403, "Forbidden")],
)
async def test_server_rejection_during_handshake_blocks(code: int, reason: str) -> None:
    sock = MockWebSocket()
    rt, handed = _client_with_sockets([sock])
    sleeper = ControlledSleep(hold=True)
    rt._sleep = sleeper
    errors: list[BaseException] = []
    blocked: list[dict[str, Any]] = []
    rt.on_error(lambda e: errors.append(e))
    rt.on("blocked", lambda evt: blocked.append(evt))
    try:
        await rt.connect()
        await _settle()
        await sock.close(code=code, reason=reason)
        await _settle()
        assert rt.state == "blocked"
        assert rt.blocked_reason == f"{code} {reason}".strip()
        assert not rt.is_connected
        assert len(handed) == 1  # no fast reconnect
        assert [e["code"] for e in blocked] == [code]
        assert blocked[0]["reason"] == reason  # as the server sent it, "" included
        assert any("auth rejection" in str(e) for e in errors)
        # The slow retry is armed, 10-15 min out.
        assert len(sleeper.delays) == 1
        assert 600.0 <= sleeper.delays[0] <= 900.0
        assert 600.0 <= blocked[0]["retry_in_ms"] / 1000.0 <= 900.0
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_blocked_retries_slowly_and_hello_ok_clears_it(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Rejected → blocked → (10-15 min) → rejected again → blocked, one error
    line in total → (10-15 min) → hello.ok → connected, reason cleared."""
    first, second, third = MockWebSocket(), MockWebSocket(), MockWebSocket()
    rt, handed = _client_with_sockets([first, second, third])
    sleeper = ControlledSleep(hold=True)
    rt._sleep = sleeper
    blocked: list[dict[str, Any]] = []
    rt.on("blocked", lambda evt: blocked.append(evt))
    try:
        with caplog.at_level(logging.DEBUG, logger="agentchat.realtime"):
            await rt.connect()
            await _settle()
            await first.close(code=1008, reason="Invalid token")
            await _settle()
            assert rt.state == "blocked"

            sleeper.release()  # the first slow retry fires
            await _until(lambda: len(handed) >= 2)
            await _settle()
            assert rt.state == "connecting"
            assert rt.blocked_reason == "1008 Invalid token"  # held until a hello.ok
            await second.close(code=1008, reason="Invalid token")
            await _settle()
            assert rt.state == "blocked"
            assert len(blocked) == 2
            assert len(sleeper.delays) == 2
            assert all(600.0 <= d <= 900.0 for d in sleeper.delays)

            sleeper.release()  # the second slow retry fires
            await _until(lambda: len(handed) >= 3)
            await _settle()
            await third.push({"type": "hello.ok", "capabilities": ["ack"]})
            await _settle()
            assert rt.state == "ready"
            assert rt.blocked_reason is None
            assert rt.ack_negotiated is True

        rejected = [
            r for r in caplog.records if "rejected this API key" in r.getMessage()
        ]
        assert [r.levelno for r in rejected] == [logging.ERROR, logging.DEBUG]
        assert any("blocked cleared" in r.getMessage() for r in caplog.records)
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_transport_failure_during_blocked_retry_takes_normal_backoff() -> None:
    """A failed open during the slow retry is no verdict: fast backoff, and
    the recorded rejection stays visible until a hello.ok."""
    sock = MockWebSocket()
    handed = 0

    async def flaky_connect(_url: str, **_kw: Any) -> MockWebSocket:
        nonlocal handed
        handed += 1
        if handed == 2:
            raise OSError("network down")
        return sock

    rt = RealtimeClient(
        api_key="sk_test", reconnect=True, reconnect_interval_ms=10,
        max_reconnect_interval_ms=20, websocket_connect=flaky_connect,
    )
    sleeper = ControlledSleep(hold=True)
    rt._sleep = sleeper
    try:
        await rt.connect()
        await _settle()
        await sock.close(code=4401, reason="")
        await _settle()
        assert rt.state == "blocked"
        sleeper.release()
        await _until(lambda: len(sleeper.delays) >= 2)
        assert rt.state == "backoff"
        assert rt.blocked_reason == "4401"
        assert sleeper.delays[1] < 1.0  # the normal backoff, not the slow retry
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_connect_while_blocked_retries_at_once() -> None:
    """An explicit connect() (the caller rotated its key) cancels the slow
    retry and tries now."""
    first, second = MockWebSocket(), MockWebSocket()
    rt, handed = _client_with_sockets([first, second])
    sleeper = ControlledSleep(hold=True)
    rt._sleep = sleeper
    try:
        await rt.connect()
        await _settle()
        await first.close(code=1008, reason="Invalid token")
        await _settle()
        assert rt.state == "blocked"
        timer = rt._reconnect_task
        assert timer is not None and not timer.done()
        await rt.connect()
        await _settle()
        assert timer.done()
        assert len(handed) == 2
        assert rt.state == "connecting"
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_blocked_with_reconnect_off_exposes_the_verdict_without_a_timer() -> None:
    sock = MockWebSocket()
    rt, _handed = _client_with_sockets([sock], reconnect=False)
    blocked: list[dict[str, Any]] = []
    rt.on("blocked", lambda evt: blocked.append(evt))
    try:
        await rt.connect()
        await _settle()
        await sock.close(code=1008, reason="Invalid token")
        await _settle()
        assert rt.state == "blocked"
        assert rt._reconnect_task is None
        assert blocked[0]["retry_in_ms"] is None
    finally:
        await rt.disconnect()


# ─────────────── Rule 1: no cap, one error line ───────────────


@pytest.mark.asyncio
async def test_sixty_one_failures_keep_reconnecting_at_the_cap_with_one_error_line(
    caplog: pytest.LogCaptureFixture,
) -> None:
    attempts = 0

    async def failing_connect(_url: str, **_kw: Any) -> MockWebSocket:
        nonlocal attempts
        attempts += 1
        raise OSError("connection refused")

    rt = RealtimeClient(
        api_key="sk_test",
        reconnect=True,
        reconnect_interval_ms=500,
        max_reconnect_interval_ms=30_000,
        websocket_connect=failing_connect,
    )
    sleeper = ControlledSleep()
    rt._sleep = sleeper
    try:
        with caplog.at_level(logging.DEBUG, logger="agentchat.realtime"):
            with pytest.raises(AgentChatConnectionError):
                await rt.connect()
            await _until(lambda: attempts >= 75, timeout=5.0)
        assert rt.state == "backoff"
        assert rt._reconnect_task is not None and not rt._reconnect_task.done()
        # Attempts 1..60 sit under the configured 30 s cap; from 61 on the
        # cap is five minutes. Jitter only goes below a step, so both caps
        # are true ceilings. Never a stop.
        early = sleeper.delays[:60]
        late = sleeper.delays[60:]
        assert late, "the client must still be scheduling attempts past 60"
        assert all(d <= 30.0 for d in early)
        assert all(300.0 * 0.75 <= d <= 300.0 for d in late)
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        assert "still reconnecting after 60" in errors[0].getMessage()
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_max_reconnect_attempts_is_deprecated_and_ignored() -> None:
    attempts = 0

    async def failing_connect(_url: str, **_kw: Any) -> MockWebSocket:
        nonlocal attempts
        attempts += 1
        raise OSError("refused")

    with pytest.warns(DeprecationWarning, match="max_reconnect_attempts"):
        rt = RealtimeClient(
            api_key="sk_test",
            reconnect=True,
            reconnect_interval_ms=1,
            max_reconnect_interval_ms=2,
            max_reconnect_attempts=1,
            websocket_connect=failing_connect,
        )
    rt._sleep = ControlledSleep()
    try:
        with pytest.raises(AgentChatConnectionError):
            await rt.connect()
        await _until(lambda: attempts >= 5)
        assert rt.state == "backoff"
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_hello_send_failure_is_our_close_and_reconnects() -> None:
    """A socket we cannot write HELLO to was a dead state before: nothing
    read it, nothing reconnected."""

    class _DeafSocket(MockWebSocket):
        async def send(self, data: str) -> None:
            raise OSError("broken pipe")

    deaf, good = _DeafSocket(), MockWebSocket()
    rt, handed = _client_with_sockets([deaf, good])
    try:
        await rt.connect()
        await _until(lambda: len(handed) >= 2)
        assert deaf.closed and deaf.close_code == _realtime._CLIENT_CLOSE_CODE
        assert rt.state in ("backoff", "connecting")
        assert rt.blocked_reason is None
    finally:
        await rt.disconnect()


# ─────────────── Rule 5: liveness ───────────────


@pytest.mark.asyncio
async def test_liveness_watchdog_holds_while_pings_are_answered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_realtime, "_LIVENESS_PROBE_TIMEOUT_S", 0.05)
    sock = PingingWebSocket(pong=True)
    rt, handed = _client_with_sockets([sock], liveness_timeout_ms=20)
    try:
        await rt.connect()
        await _settle()
        await sock.push({"type": "hello.ok"})
        await asyncio.sleep(0.2)
        assert sock.pings >= 2  # probed repeatedly, every probe answered
        assert not sock.closed
        assert rt.state == "ready"
        assert len(handed) == 1
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_liveness_watchdog_trips_without_pongs_and_reconnects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_realtime, "_LIVENESS_PROBE_TIMEOUT_S", 0.02)
    silent, next_sock = PingingWebSocket(pong=False), PingingWebSocket(pong=True)
    rt, handed = _client_with_sockets([silent, next_sock], liveness_timeout_ms=20)
    errors: list[BaseException] = []
    closes: list[dict[str, Any]] = []
    rt.on_error(lambda e: errors.append(e))
    rt.on_disconnect(lambda info: closes.append(info))
    try:
        await rt.connect()
        await _settle()
        await silent.push({"type": "hello.ok"})
        await _until(lambda: len(handed) >= 2)
        assert silent.pings == 1
        assert silent.closed and silent.close_code == _realtime._CLIENT_CLOSE_CODE
        assert silent.close_reason == "client liveness timeout"
        assert any("liveness timeout" in str(e) for e in errors)
        assert closes[0]["initiator"] == "client"
        assert rt.state in ("backoff", "connecting")  # ours: never blocked
        assert rt.blocked_reason is None
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_inbound_frames_keep_the_liveness_watchdog_quiet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_realtime, "_LIVENESS_PROBE_TIMEOUT_S", 0.02)
    sock = PingingWebSocket(pong=False)
    # A 200 ms window with a push every 20 ms: wide enough for Windows timer
    # granularity (15.6 ms) to never let the window lapse between pushes.
    rt, _handed = _client_with_sockets([sock], liveness_timeout_ms=200)
    rt.on("message.new", lambda m: None)
    try:
        await rt.connect()
        await _settle()
        await sock.push({"type": "hello.ok"})
        for seq in range(1, 16):
            await asyncio.sleep(0.02)
            await sock.push(message_new("c", seq))
        assert sock.pings == 0  # never silent long enough to probe
        assert not sock.closed
        assert rt.state == "ready"
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_liveness_watchdog_default_is_not_armed_on_a_socket_without_ping() -> None:
    rt, _ws = await connected()
    try:
        assert rt.state == "ready"
        assert rt._liveness_task is None
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_liveness_watchdog_zero_disables_it_on_a_pinging_socket() -> None:
    sock = PingingWebSocket(pong=True)
    rt, _handed = _client_with_sockets([sock], liveness_timeout_ms=0)
    try:
        await rt.connect()
        await _settle()
        await sock.push({"type": "hello.ok"})
        await _settle()
        assert rt.state == "ready"
        assert rt._liveness_task is None
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_explicit_liveness_timeout_counts_frames_alone_without_ping() -> None:
    """The caller asked for it on a socket that cannot be probed: silence
    for the whole window trips it."""
    quiet, next_sock = MockWebSocket(), MockWebSocket()
    rt, handed = _client_with_sockets([quiet, next_sock], liveness_timeout_ms=20)
    try:
        await rt.connect()
        await _settle()
        await quiet.push({"type": "hello.ok"})
        await _until(lambda: len(handed) >= 2)
        assert quiet.closed and quiet.close_reason == "client liveness timeout"
        assert rt.blocked_reason is None
    finally:
        await rt.disconnect()


# ─────────────── Typed state and events ───────────────


@pytest.mark.asyncio
async def test_state_walks_the_lifecycle_and_ack_negotiated_tracks_the_connection() -> None:
    sock = MockWebSocket()
    rt, _handed = _client_with_sockets([sock])
    assert rt.state == "idle"
    assert rt.ack_negotiated is False
    try:
        await rt.connect()
        await _settle()
        assert rt.state == "connecting"
        await sock.push({"type": "hello.ok", "capabilities": ["ack"]})
        await _settle()
        assert rt.state == "ready"
        assert rt.ack_negotiated is True
        await sock.close(code=1006)
        await _settle()
        assert rt.ack_negotiated is False
        assert rt.state in ("backoff", "connecting")
    finally:
        await rt.disconnect()
    assert rt.state == "stopped"


@pytest.mark.asyncio
async def test_on_state_reports_every_transition_in_order() -> None:
    sock = MockWebSocket()
    rt, _handed = _client_with_sockets([sock])
    seen: list[tuple[str, str]] = []
    rt.on("state", lambda evt: seen.append((evt["previous"], evt["state"])))
    sleeper = ControlledSleep(hold=True)
    rt._sleep = sleeper
    await rt.connect()
    await _settle()
    await sock.push({"type": "hello.ok"})
    await _settle()
    await sock.close(code=1006, reason="blip")
    await _settle()
    await rt.disconnect()
    assert seen == [
        ("idle", "connecting"),
        ("connecting", "ready"),
        ("ready", "backoff"),
        ("backoff", "stopped"),
    ]


@pytest.mark.asyncio
async def test_on_disconnect_names_the_server_as_initiator_for_a_wire_close() -> None:
    sock = MockWebSocket()
    rt, _handed = _client_with_sockets([sock])
    closes: list[dict[str, Any]] = []
    rt.on_disconnect(lambda info: closes.append(info))
    try:
        await rt.connect()
        await _settle()
        await sock.push({"type": "hello.ok"})
        await _settle()
        await sock.close(code=1001, reason="Heartbeat timeout")
        await _settle()
        assert closes == [
            {"code": 1001, "reason": "Heartbeat timeout", "was_clean": False, "initiator": "server"}
        ]
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_server_frames_typed_like_local_events_are_dropped() -> None:
    sock = MockWebSocket()
    rt, _handed = _client_with_sockets([sock])
    blocked: list[dict[str, Any]] = []
    stopped: list[dict[str, Any]] = []
    rt.on("blocked", lambda evt: blocked.append(evt))
    rt.on("stopped", lambda evt: stopped.append(evt))
    try:
        await rt.connect()
        await _settle()
        await sock.push({"type": "hello.ok"})
        await _settle()
        await sock.push({"type": "blocked", "code": 4401, "reason": "spoofed"})
        await sock.push({"type": "stopped"})
        await sock.push({"type": "state", "state": "stopped", "previous": "ready"})
        await _settle()
        assert blocked == []
        assert stopped == []
        assert rt.state == "ready"
    finally:
        await rt.disconnect()
    assert stopped == [{"type": "stopped"}]


@pytest.mark.asyncio
async def test_disconnect_from_blocked_stops_with_no_task_left() -> None:
    baseline = other_tasks()
    sock = MockWebSocket()
    rt, handed = _client_with_sockets([sock])
    sleeper = ControlledSleep(hold=True)
    rt._sleep = sleeper
    stopped: list[dict[str, Any]] = []
    rt.on("stopped", lambda evt: stopped.append(evt))
    await rt.connect()
    await _settle()
    await sock.close(code=1008, reason="Invalid token")
    await _settle()
    assert rt.state == "blocked"

    await rt.disconnect()
    assert rt.state == "stopped"
    assert stopped == [{"type": "stopped"}]
    assert other_tasks() - baseline == set()
    sleeper.release()
    await asyncio.sleep(0.05)
    assert len(handed) == 1
    assert other_tasks() - baseline == set()
    with pytest.raises(AgentChatConnectionError):
        await rt.connect()


@pytest.mark.asyncio
async def test_disconnect_from_pending_backoff_stops_with_no_task_left() -> None:
    baseline = other_tasks()
    sock = MockWebSocket()
    rt, handed = _client_with_sockets([sock])
    sleeper = ControlledSleep(hold=True)
    rt._sleep = sleeper
    stopped = 0

    def on_stopped(_evt: dict[str, Any]) -> None:
        nonlocal stopped
        stopped += 1

    rt.on("stopped", on_stopped)
    await rt.connect()
    await _settle()
    await sock.push({"type": "hello.ok"})
    await _settle()
    await sock.close(code=1006, reason="blip")
    await _settle()
    assert rt.state == "backoff"

    await rt.disconnect()
    assert rt.state == "stopped"
    assert stopped == 1
    assert other_tasks() - baseline == set()
    sleeper.release()
    await asyncio.sleep(0.05)
    assert len(handed) == 1
    assert other_tasks() - baseline == set()


@pytest.mark.asyncio
async def test_disconnect_during_a_connected_session_releases_the_idle_watchdog() -> None:
    baseline = other_tasks()
    sock = PingingWebSocket(pong=True)
    rt, _handed = _client_with_sockets([sock])
    await rt.connect()
    await _settle()
    await sock.push({"type": "hello.ok"})
    await _settle()
    assert rt._liveness_task is not None and not rt._liveness_task.done()
    await rt.disconnect()
    assert other_tasks() - baseline == set()


@pytest.mark.asyncio
async def test_on_disconnect_names_the_client_as_initiator_for_disconnect() -> None:
    sock = MockWebSocket()
    rt, _handed = _client_with_sockets([sock])
    closes: list[dict[str, Any]] = []
    rt.on_disconnect(lambda info: closes.append(info))
    await rt.connect()
    await _settle()
    await sock.push({"type": "hello.ok"})
    await _settle()
    await rt.disconnect()
    assert sock.closed and sock.close_code == 1000 and sock.close_reason == "client disconnect"
    assert closes == [
        {"code": 1000, "reason": "client disconnect", "was_clean": True, "initiator": "client"}
    ]


# ─────────────── Review fixes: handlers that act during a close ───────────────


@pytest.mark.asyncio
async def test_connect_from_on_disconnect_reopens_with_reconnect_off() -> None:
    """A caller running its own reconnects (reconnect=False) does it from
    on_disconnect; the closed socket is gone by then, so connect() opens."""
    first, second = MockWebSocket(), MockWebSocket()
    rt, handed = _client_with_sockets([first, second], reconnect=False)
    reopened = asyncio.Event()

    async def reopen(_info: dict[str, Any]) -> None:
        await rt.connect()
        reopened.set()

    rt.on_disconnect(reopen)
    try:
        await rt.connect()
        await _settle()
        await first.push({"type": "hello.ok"})
        await _settle()
        await first.close(code=1006, reason="blip")
        await asyncio.wait_for(reopened.wait(), timeout=2.0)
        await _settle()
        assert handed == [first, second]
        assert rt.state == "connecting"  # not parked in idle by the old close
        assert rt._reconnect_task is None
        await second.push({"type": "hello.ok"})
        await _settle()
        assert rt.state == "ready"
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_connect_from_on_disconnect_with_reconnect_on_arms_no_second_timer() -> None:
    first, second = MockWebSocket(), MockWebSocket()
    rt, handed = _client_with_sockets([first, second])
    rt.on_disconnect(lambda _info: rt.connect())
    try:
        await rt.connect()
        await _settle()
        await first.push({"type": "hello.ok"})
        await _settle()
        await first.close(code=1006, reason="blip")
        await asyncio.sleep(0.1)
        assert handed == [first, second]
        assert rt._reconnect_task is None
        assert rt.state == "connecting"
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_disconnect_from_a_state_handler_on_backoff_arms_no_timer() -> None:
    baseline = other_tasks()
    sock = MockWebSocket()
    rt, handed = _client_with_sockets([sock])
    sleeper = ControlledSleep(hold=True)
    rt._sleep = sleeper

    async def stop_on_backoff(evt: dict[str, Any]) -> None:
        if evt["state"] == "backoff":
            await rt.disconnect()

    rt.on("state", stop_on_backoff)
    await rt.connect()
    await _settle()
    await sock.push({"type": "hello.ok"})
    await _settle()
    await sock.close(code=1006, reason="blip")
    await asyncio.sleep(0.05)
    assert rt.state == "stopped"
    assert rt._reconnect_task is None
    assert sleeper.delays == []
    assert len(handed) == 1
    assert other_tasks() - baseline == set()


@pytest.mark.asyncio
async def test_on_error_fires_once_per_blocked_run_while_blocked_fires_per_rejection() -> None:
    socks = [MockWebSocket(), MockWebSocket(), MockWebSocket(), MockWebSocket()]
    rt, handed = _client_with_sockets(socks)
    sleeper = ControlledSleep(hold=True)
    rt._sleep = sleeper
    errors: list[BaseException] = []
    blocked: list[dict[str, Any]] = []
    rt.on_error(lambda e: errors.append(e))
    rt.on("blocked", lambda evt: blocked.append(evt))
    try:
        await rt.connect()
        for i in range(3):
            await _until(lambda i=i: len(handed) >= i + 1)
            await _settle()
            await socks[i].close(code=1008, reason="Invalid token")
            await _settle()
            assert rt.state == "blocked"
            sleeper.release()
        await _until(lambda: len(handed) >= 4)
        assert len(blocked) == 3
        assert len([e for e in errors if "auth rejection" in str(e)]) == 1
        # A hello.ok ends the run; the next rejection is a new run.
        await socks[3].push({"type": "hello.ok"})
        await _settle()
        assert rt.state == "ready"
        await socks[3].close(code=1008, reason="Invalid token")  # after hello.ok: eviction
        await _settle()
        assert rt.state == "backoff"
    finally:
        await rt.disconnect()


@pytest.mark.parametrize("value", [0, -1])
def test_non_positive_handshake_timeout_falls_back_to_the_default(value: int) -> None:
    from agentchatme import RealtimeOptions

    rt = RealtimeClient(api_key="k", handshake_timeout_ms=value)
    assert rt._opts.handshake_timeout_ms == _realtime._DEFAULT_HANDSHAKE_TIMEOUT_MS
    via_options = RealtimeClient(RealtimeOptions(api_key="k", handshake_timeout_ms=value))
    assert via_options._opts.handshake_timeout_ms == _realtime._DEFAULT_HANDSHAKE_TIMEOUT_MS


# ─────────────── Review fixes: a raising on_error never parks the client ───────────────


def _raising(_err: BaseException) -> None:
    raise RuntimeError("observer bug")


@pytest.mark.asyncio
async def test_raising_on_error_at_the_handshake_deadline_still_closes_and_reconnects() -> None:
    first, second = MockWebSocket(), MockWebSocket()
    rt, handed = _client_with_sockets([first, second], handshake_timeout_ms=20)
    rt.on_error(_raising)
    try:
        await rt.connect()
        await _until(lambda: len(handed) >= 2)
        assert first.closed and first.close_reason == "client handshake timeout"
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_raising_on_error_at_the_liveness_deadline_still_closes_and_reconnects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(_realtime, "_LIVENESS_PROBE_TIMEOUT_S", 0.02)
    silent, next_sock = PingingWebSocket(pong=False), PingingWebSocket(pong=True)
    rt, handed = _client_with_sockets([silent, next_sock], liveness_timeout_ms=20)
    rt.on_error(_raising)
    try:
        await rt.connect()
        await _settle()
        await silent.push({"type": "hello.ok"})
        await _until(lambda: len(handed) >= 2)
        assert silent.closed and silent.close_reason == "client liveness timeout"
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_raising_on_error_through_the_long_outage_notice_keeps_reconnecting(
    caplog: pytest.LogCaptureFixture,
) -> None:
    attempts = 0

    async def failing_connect(_url: str, **_kw: Any) -> MockWebSocket:
        nonlocal attempts
        attempts += 1
        raise OSError("refused")

    rt = RealtimeClient(
        api_key="k", reconnect=True, reconnect_interval_ms=1, max_reconnect_interval_ms=2,
        websocket_connect=failing_connect,
    )
    rt._sleep = ControlledSleep()
    rt.on_error(_raising)
    try:
        with caplog.at_level(logging.ERROR, logger="agentchat.realtime"):
            with pytest.raises(AgentChatConnectionError):
                await rt.connect()
            await _until(lambda: attempts >= 65, timeout=5.0)
        assert rt.state == "backoff"
        assert sum("still reconnecting after 60" in r.getMessage() for r in caplog.records) == 1
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_raising_on_error_when_entering_blocked_still_arms_the_slow_retry() -> None:
    sock = MockWebSocket()
    rt, _handed = _client_with_sockets([sock])
    sleeper = ControlledSleep(hold=True)
    rt._sleep = sleeper
    rt.on_error(_raising)
    try:
        await rt.connect()
        await _settle()
        await sock.close(code=1008, reason="Invalid token")
        await _settle()
        assert rt.state == "blocked"
        assert rt._reconnect_task is not None and not rt._reconnect_task.done()
        assert len(sleeper.delays) == 1 and 600.0 <= sleeper.delays[0] <= 900.0
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_raising_on_error_at_hello_send_failure_still_closes_and_reconnects() -> None:
    class _DeafSocket(MockWebSocket):
        async def send(self, data: str) -> None:
            raise OSError("broken pipe")

    deaf, good = _DeafSocket(), MockWebSocket()
    rt, handed = _client_with_sockets([deaf, good])
    rt.on_error(_raising)
    try:
        await rt.connect()
        await _until(lambda: len(handed) >= 2)
        assert deaf.closed and deaf.close_reason == "client HELLO send failed"
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_realtime_state_is_exported() -> None:
    import agentchatme

    assert "RealtimeState" in agentchatme.__all__
    rt, _ws = _make_client()
    try:
        assert rt.state == "idle"
    finally:
        await rt.disconnect()
