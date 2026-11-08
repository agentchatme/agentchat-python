"""Socket rules added for contract v1: handlers off the receive loop, no ack
without a handler, in-flight dedupe across reconnects, the ``heartbeat`` and
``rewake`` capabilities, the unanswered wake, and the connect budget (HTTP
429 / close 1013) as backoff with the server's delay."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import pytest

from agentchatme import RealtimeClient, _realtime
from agentchatme._realtime import wake_payload_problem

from .test_realtime import MockWebSocket, _ack_frames, _make_client, _settle
from .test_realtime_engine import connected, message_new
from .test_realtime_resilience import ControlledSleep, _client_with_sockets, _until

# ─────────────── handlers never hold up the receive loop ───────────────


@pytest.mark.asyncio
async def test_a_slow_handler_never_holds_up_the_next_frame_or_its_ack() -> None:
    rt, ws = await connected(capabilities=["ack"])
    release = asyncio.Event()
    calls: list[str] = []

    async def handler(m: dict[str, Any]) -> None:
        calls.append(m["payload"]["id"])
        if m["payload"]["id"] == "msg_1":
            await release.wait()

    presence: list[str] = []
    rt.on("message.new", handler)
    rt.on("presence.update", lambda m: presence.append(m["payload"]["handle"]))
    try:
        await ws.push(message_new("a", 1))
        await ws.push(message_new("b", 1, id="msg_b1"))
        await ws.push({"type": "presence.update", "payload": {"handle": "nova"}})
        await _settle()
        # The slow handler is still running; the receive loop went on.
        assert calls == ["msg_1", "msg_b1"]
        assert presence == ["nova"]
        assert _ack_frames(ws) == ["msg_b1"]
        release.set()
        await _settle()
        assert _ack_frames(ws) == ["msg_b1", "msg_1"]
    finally:
        release.set()
        await rt.disconnect()


@pytest.mark.asyncio
async def test_a_slow_async_handler_of_any_server_event_never_blocks_reads() -> None:
    rt, ws = await connected(capabilities=["ack"])
    gate = asyncio.Event()
    seen: list[str] = []

    async def slow_typing(_m: dict[str, Any]) -> None:
        await gate.wait()

    rt.on("typing.start", slow_typing)
    rt.on("message.new", lambda m: seen.append(m["payload"]["id"]))
    try:
        await ws.push({"type": "typing.start", "payload": {"conversation_id": "b"}})
        await ws.push(message_new("a", 1))
        await _settle()
        assert seen == ["msg_1"]
    finally:
        gate.set()
        await rt.disconnect()


@pytest.mark.asyncio
async def test_one_handler_at_a_time_per_conversation_and_conversations_in_parallel() -> None:
    rt, ws = await connected(capabilities=["ack"])
    release = asyncio.Event()
    started: list[str] = []
    finished: list[str] = []

    async def handler(m: dict[str, Any]) -> None:
        mid = m["payload"]["id"]
        started.append(mid)
        if mid == "msg_5":
            await release.wait()  # the first is slower
        finished.append(mid)

    rt.on("message.new", handler)
    try:
        await ws.push(message_new("c", 5))
        await ws.push(message_new("c", 6))
        await ws.push(message_new("d", 1, id="msg_d1"))
        await _settle()
        # seq 6 of c does not start until seq 5 finished; d runs meanwhile.
        assert started == ["msg_5", "msg_d1"]
        assert finished == ["msg_d1"]
        assert _ack_frames(ws) == ["msg_d1"]
        release.set()
        await _settle()
        assert started == ["msg_5", "msg_d1", "msg_6"]
        assert finished == ["msg_d1", "msg_5", "msg_6"]
        assert _ack_frames(ws) == ["msg_d1", "msg_5", "msg_6"]
    finally:
        release.set()
        await rt.disconnect()


@pytest.mark.asyncio
async def test_a_hung_handler_holds_its_conversation_at_most_in_flight_adopt_ms() -> None:
    rt, ws = await connected(capabilities=["ack"], in_flight_adopt_ms=50)
    started: list[str] = []

    async def handler(m: dict[str, Any]) -> None:
        started.append(m["payload"]["id"])
        if m["payload"]["id"] == "msg_1":
            await asyncio.Event().wait()  # never finishes

    rt.on("message.new", handler)
    try:
        await ws.push(message_new("c", 1))
        await ws.push(message_new("c", 2))
        await _settle()
        assert started == ["msg_1"]
        await asyncio.sleep(0.15)
        assert started == ["msg_1", "msg_2"]  # the conversation went on
        assert _ack_frames(ws) == ["msg_2"]  # the hung one is never acked
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_the_first_message_handler_after_a_hello_without_ack_renegotiates_once_quietly() -> None:
    first, second = MockWebSocket(), MockWebSocket()
    rt, handed = _client_with_sockets([first, second])
    errors: list[BaseException] = []
    rt.on_error(errors.append)
    sleep = ControlledSleep(hold=True)
    rt._sleep = sleep
    try:
        await rt.connect()
        await _settle()
        assert json.loads(first.sent[0])["capabilities"] == ["heartbeat"]
        await first.push({"type": "hello.ok", "capabilities": ["heartbeat"]})
        await _settle()
        seen: list[str] = []
        rt.on("message.new", lambda m: seen.append(m["payload"]["id"]))
        await _until(lambda: len(handed) == 2)
        assert first.closed and first.close_code == 1000
        assert json.loads(second.sent[0])["capabilities"] == ["ack", "heartbeat"]
        assert sleep.delays == [] and errors == []  # at once, no backoff step, no error
        await second.push({"type": "hello.ok", "capabilities": ["ack", "heartbeat"]})
        await _settle()
        assert rt.ack_negotiated
        rt.on("message.new", lambda _m: None)  # a second handler: nothing more
        await _settle()
        assert len(handed) == 2
    finally:
        sleep.release()
        await rt.disconnect()


@pytest.mark.asyncio
async def test_frames_of_no_conversation_share_one_serial_lane() -> None:
    rt, ws = await connected(capabilities=["ack"])
    release = asyncio.Event()
    order: list[str] = []

    async def presence(_m: dict[str, Any]) -> None:
        order.append("presence:start")
        await release.wait()
        order.append("presence:end")

    rt.on("presence.update", presence)
    rt.on("wake", lambda _m: order.append("wake"))
    rt.on("message.new", lambda m: order.append(m["payload"]["id"]))
    try:
        await ws.push({"type": "presence.update", "payload": {"handle": "nova"}})
        await ws.push(
            {
                "type": "wake",
                "payload": {
                    "reason": "reminder",
                    "conversation_id": "c",
                    "anchor_message_id": "m",
                    "step": 1,
                    "at": "t",
                },
            }
        )
        await ws.push(message_new("c", 1))
        await _settle()
        # msg_1 is on its conversation's lane; the wake waits its turn on the
        # no-conversation lane behind the running presence handler.
        assert sorted(order) == ["msg_1", "presence:start"]
        release.set()
        await _settle()
        assert order.index("wake") > order.index("presence:end")
    finally:
        release.set()
        await rt.disconnect()


@pytest.mark.asyncio
async def test_handlers_of_one_conversation_start_in_seq_order() -> None:
    rt, ws = await connected(capabilities=["ack"])
    started: list[int] = []

    async def handler(m: dict[str, Any]) -> None:
        started.append(m["payload"]["seq"])
        await asyncio.sleep(0)

    rt.on("message.new", handler)
    try:
        await ws.push(message_new("c", 1))
        await ws.push(message_new("c", 3))
        await ws.push(message_new("c", 2))
        await asyncio.sleep(0.05)
        assert started == [1, 2, 3]
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_an_async_on_connect_handler_never_holds_up_the_backlog() -> None:
    rt, ws = _make_client()
    gate = asyncio.Event()
    seen: list[str] = []

    async def on_connect() -> None:
        await gate.wait()

    rt.on_connect(on_connect)
    rt.on("message.new", lambda m: seen.append(m["payload"]["id"]))
    try:
        await rt.connect()
        await _settle()
        await ws.push({"type": "hello.ok", "capabilities": ["ack"]})
        await ws.push(message_new("c", 1))
        await _settle()
        assert seen == ["msg_1"]
    finally:
        gate.set()
        await rt.disconnect()


@pytest.mark.asyncio
async def test_disconnect_releases_a_handler_that_never_returns() -> None:
    rt, ws = await connected(capabilities=["ack"])

    async def hangs(_m: dict[str, Any]) -> None:
        await asyncio.Event().wait()

    rt.on("message.new", hangs)
    await ws.push(message_new("c", 1))
    await _settle()
    started = time.monotonic()
    await rt.disconnect()
    assert time.monotonic() - started < _realtime._DISPOSE_GRACE_S + 1.0
    assert rt._dispatch_tasks == set()
    await _settle()
    assert rt._lanes == {}
    assert rt._inflight == {}
    assert _ack_frames(ws) == []


# ─────────────── never ack without a handler ───────────────


@pytest.mark.parametrize("ack_on_receipt", [False, True])
@pytest.mark.asyncio
async def test_a_frame_is_never_acked_while_no_message_handler_is_attached(
    ack_on_receipt: bool,
) -> None:
    # A server that echoes ack although HELLO did not offer it (no handler
    # was attached): the capability does not hold, and nothing is acked.
    rt, ws = await connected(capabilities=["ack"], with_handler=False, ack_on_receipt=ack_on_receipt)
    try:
        assert json.loads(ws.sent[0])["capabilities"] == ["heartbeat"]
        assert rt.ack_negotiated is False
        await ws.push(message_new("c", 1))
        await _settle()
        assert _ack_frames(ws) == []
    finally:
        await rt.disconnect()
    # A handler attached, but removed before the frame: not acked, and not
    # remembered, so the re-offer is handled once a handler is back.
    rt, ws = await connected(capabilities=["ack"], with_handler=False, ack_on_receipt=ack_on_receipt)
    try:
        off = rt.on("message.new", lambda _m: None)
        off()
        await ws.push(message_new("c", 1))
        await _settle()
        assert _ack_frames(ws) == []
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_ack_is_offered_and_negotiated_only_with_a_message_handler() -> None:
    rt, ws = await connected(capabilities=["ack", "heartbeat"])
    try:
        assert json.loads(ws.sent[0])["capabilities"] == ["ack", "heartbeat"]
        assert rt.capabilities == frozenset({"ack", "heartbeat"})
        seen: list[str] = []
        rt.on("message.new", lambda m: seen.append(m["payload"]["id"]))
        await ws.push(message_new("c", 1))
        await _settle()
        assert seen == ["msg_1"] and _ack_frames(ws) == ["msg_1"]
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_a_wake_handler_alone_does_not_make_message_frames_ackable() -> None:
    rt, ws = await connected(capabilities=["ack"], with_handler=False, ack_on_receipt=True)
    rt.on("wake", lambda _m: None)
    try:
        await ws.push(message_new("c", 1))
        await _settle()
        assert _ack_frames(ws) == []
    finally:
        await rt.disconnect()


# ─────────────── capabilities: heartbeat, rewake ───────────────


@pytest.mark.parametrize(
    ("offered", "caps"),
    [
        (None, ["ack", "heartbeat"]),
        ({"rewake": True}, ["ack", "heartbeat", "rewake"]),
        ({"heartbeat": False}, ["ack"]),
        ({"heartbeat": False, "rewake": True}, ["ack", "rewake"]),
    ],
)
@pytest.mark.asyncio
async def test_hello_offers_heartbeat_by_default_and_rewake_only_when_asked(
    offered: Any, caps: list[str]
) -> None:
    rt, ws = _make_client(capabilities=offered)
    rt.on("message.new", lambda _m: None)
    try:
        await rt.connect()
        await _settle()
        assert json.loads(ws.sent[0])["capabilities"] == caps
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_negotiated_capabilities_are_exposed_and_cleared_on_close() -> None:
    rt, ws = await connected(capabilities=["ack", "heartbeat", "rewake"], offer={"rewake": True})
    try:
        assert rt.capabilities == frozenset({"ack", "heartbeat", "rewake"})
        await ws.close(code=1006)
        await _settle()
        assert rt.capabilities == frozenset()
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_heartbeat_arms_the_idle_watchdog_on_a_socket_without_ping() -> None:
    # MockWebSocket has no ping(): without heartbeat there is no watchdog;
    # with it, frames are the evidence.
    rt, _ws = await connected(capabilities=["ack"])
    try:
        assert rt._liveness_task is None
    finally:
        await rt.disconnect()
    rt, _ws = await connected(capabilities=["ack", "heartbeat"])
    try:
        assert rt._liveness_task is not None
        assert rt._liveness_timeout_ms(_ws) == _realtime._HEARTBEAT_LIVENESS_TIMEOUT_MS
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_heartbeat_frames_keep_a_quiet_socket_alive_and_their_absence_trips_it() -> None:
    errors: list[BaseException] = []
    rt, ws = await connected(capabilities=["ack", "heartbeat"], liveness_timeout_ms=150)
    rt.on_error(errors.append)
    beats: list[str] = []
    rt.on("heartbeat", lambda m: beats.append(m["payload"]["at"]))
    try:
        for i in range(4):
            await asyncio.sleep(0.06)
            await ws.push({"type": "heartbeat", "payload": {"at": f"t{i}"}})
        await _settle()
        assert not ws.closed
        assert beats == ["t0", "t1", "t2", "t3"]
        await asyncio.sleep(0.35)  # the heartbeats stop
        await _settle()
        assert ws.closed
        assert ws.close_code == 4000
        assert any("liveness timeout" in str(e) for e in errors)
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_an_unanswered_wake_reaches_wake_handlers() -> None:
    rt, ws = await connected(capabilities=["ack", "heartbeat", "rewake"], offer={"rewake": True})
    wakes: list[dict[str, Any]] = []
    rt.on("wake", lambda m: wakes.append(m["payload"]))
    payload = {
        "reason": "unanswered",
        "conversation_ids": ["conv_a", "conv_b"],
        "step": 2,
        "at": "2026-11-08T00:05:00.000Z",
    }
    try:
        await ws.push({"type": "wake", "payload": payload})
        await _settle()
        assert wakes == [payload]
        assert _ack_frames(ws) == []
    finally:
        await rt.disconnect()


@pytest.mark.parametrize(
    ("payload", "problem"),
    [
        ({"reason": "unanswered", "conversation_ids": ["a"], "step": 1, "at": "t"}, None),
        ({"reason": "unanswered", "conversation_ids": ["a"] * 60, "step": 4, "at": "t"}, None),
        ({"reason": "reminder", "conversation_id": "c", "anchor_message_id": "m", "step": 1, "at": "t"}, None),
        ({"reason": "something_new", "at": "t"}, None),
        ({"reason": "something_new", "at": "t", "conversation_ids": ["a"], "step": 3}, None),
        ({"reason": "something_new"}, "at"),
        ({"reason": "something_new", "at": "t", "step": "3"}, "step"),
        ({"reason": "unanswered", "step": 1, "at": "t"}, "conversation_ids"),
        ({"reason": "unanswered", "conversation_ids": "a", "step": 1, "at": "t"}, "conversation_ids"),
        ({"reason": "unanswered", "conversation_ids": [""], "step": 1, "at": "t"}, "conversation_ids"),
        ({"reason": "unanswered", "conversation_ids": ["a"], "step": "1", "at": "t"}, "step"),
        ({"reason": "unanswered", "conversation_ids": ["a"], "at": "t"}, "step"),
        ({"reason": "reminder", "conversation_id": "c", "step": 1, "at": "t"}, "anchor_message_id"),
        ({"reason": "", "at": "t"}, "reason"),
        ({}, "reason"),
        ([], "payload is not an object"),
    ],
)
def test_wake_payload_problem(payload: Any, problem: str | None) -> None:
    got = wake_payload_problem(payload, rewake=True)
    if problem is None:
        assert got is None
    else:
        assert got is not None and problem in got


@pytest.mark.parametrize(
    "payload",
    [
        {"reason": "unanswered", "conversation_ids": ["a"], "step": 1, "at": "t"},
        {"reason": "something_new", "at": "t"},
    ],
)
def test_without_rewake_the_1_2_wake_rule_holds(payload: dict[str, Any]) -> None:
    assert wake_payload_problem(payload, rewake=False) is not None
    reminder = {"reason": "reminder", "conversation_id": "c", "anchor_message_id": "m", "step": 1, "at": "t"}
    assert wake_payload_problem(reminder, rewake=False) is None


@pytest.mark.asyncio
async def test_an_unanswered_wake_on_a_socket_without_rewake_is_rejected() -> None:
    rt, ws = await connected(capabilities=["ack", "heartbeat"])
    wakes: list[Any] = []
    errors: list[BaseException] = []
    rt.on("wake", wakes.append)
    rt.on_error(errors.append)
    try:
        await ws.push({"type": "wake", "payload": {"reason": "unanswered", "conversation_ids": ["c"], "step": 1, "at": "t"}})
        await _settle()
        assert wakes == [] and any("wake frame failed validation" in str(e) for e in errors)
    finally:
        await rt.disconnect()


# ─────────────── in-flight adoption across reconnects is bounded ───────────────


@pytest.mark.asyncio
async def test_a_re_offer_after_the_adoption_window_is_dispatched_afresh() -> None:
    rt, ws = await connected(capabilities=["ack"], in_flight_adopt_ms=0)
    calls: list[str] = []

    async def hangs(m: dict[str, Any]) -> None:
        calls.append(m["payload"]["id"])
        await asyncio.Event().wait()

    rt.on("message.new", hangs)
    try:
        await ws.push(message_new("c", 1))
        await _settle()
        await ws.close(code=1006)
        await _settle()
        ws2 = MockWebSocket()

        async def connect2(_url: str, **_kw: Any) -> MockWebSocket:
            return ws2

        rt._websocket_connect = connect2
        await rt.connect()
        await ws2.push({"type": "hello.ok", "capabilities": ["ack"]})
        await ws2.push(message_new("c", 1))
        await _settle()
        # Past the window the re-offer is not adopted, and the hung handler no
        # longer holds its conversation: the re-offer is dispatched afresh.
        await asyncio.sleep(0.02)
        assert calls == ["msg_1", "msg_1"]
    finally:
        await rt.disconnect()


# ─────────────── the connect budget ───────────────


@pytest.mark.asyncio
async def test_a_1013_close_backs_off_with_the_servers_delay_and_never_blocks() -> None:
    first, second = MockWebSocket(), MockWebSocket()
    rt, handed = _client_with_sockets([first, second])
    sleep = ControlledSleep(hold=True)
    rt._sleep = sleep
    disconnects: list[dict[str, Any]] = []
    rt.on_disconnect(disconnects.append)
    try:
        await rt.connect()
        await _settle()
        await first.push({"type": "hello.ok", "capabilities": ["ack"]})
        await _settle()
        # The server's budget closes the socket after HELLO with a hint.
        first.close_code = 1013
        first.close_reason = "retry_after=30"
        await first._inbox.put(None)
        await _until(lambda: bool(sleep.delays))
        assert rt.state == "backoff"
        assert rt.blocked_reason is None
        assert 30.0 <= sleep.delays[0] <= 33.0
        assert disconnects[-1]["retry_after_ms"] == 30_000
        assert rt.rate_limited_ms == 30_000
        sleep.release()
        await _until(lambda: len(handed) == 2)
        await second.push({"type": "hello.ok", "capabilities": ["ack"]})
        await _until(lambda: rt.state == "ready")
        assert rt.rate_limited_ms is None
    finally:
        sleep.release()
        await rt.disconnect()


@pytest.mark.asyncio
async def test_a_1013_without_a_hint_is_the_ordinary_backoff_never_blocked() -> None:
    # The server's slow-consumer close (1013 "overload"), even with the auth
    # reason in the text: ordinary backoff, never blocked.
    sock = MockWebSocket()
    rt, _handed = _client_with_sockets([sock])
    sleep = ControlledSleep(hold=True)
    rt._sleep = sleep
    try:
        await rt.connect()
        await _settle()
        sock.close_code = 1013
        sock.close_reason = "overload"
        await sock._inbox.put(None)
        await _until(lambda: bool(sleep.delays))
        assert rt.state == "backoff"
        assert rt.blocked_reason is None
        assert sleep.delays[0] <= 0.02  # the 10-20 ms test backoff
        assert rt.rate_limited_ms is None
    finally:
        sleep.release()
        await rt.disconnect()


@pytest.mark.asyncio
async def test_a_hinted_1013_during_the_handshake_is_not_an_auth_rejection() -> None:
    sock = MockWebSocket()
    rt, _handed = _client_with_sockets([sock])
    sleep = ControlledSleep(hold=True)
    rt._sleep = sleep
    try:
        await rt.connect()
        await _settle()
        sock.close_code = 1013
        sock.close_reason = "invalid token; retry_after=5"
        await sock._inbox.put(None)
        await _until(lambda: bool(sleep.delays))
        assert rt.state == "backoff" and rt.blocked_reason is None
        assert 5.0 <= sleep.delays[0] <= 5.5
    finally:
        sleep.release()
        await rt.disconnect()


class _InvalidStatus(Exception):
    """The shape of websockets' asyncio-client ``InvalidStatus``."""

    def __init__(self, status: int, headers: dict[str, str]) -> None:
        super().__init__(f"server rejected WebSocket connection: HTTP {status}")
        self.status_code = status
        self.headers = headers


@pytest.mark.asyncio
async def test_an_upgrade_answered_429_is_the_ordinary_backoff_never_blocked() -> None:
    attempts = 0
    ok = MockWebSocket()

    async def connect(_url: str, **_kw: Any) -> MockWebSocket:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise _InvalidStatus(429, {"Retry-After": "3600"})
        return ok

    rt = RealtimeClient(
        api_key="sk_test",
        reconnect=True,
        reconnect_interval_ms=10,
        max_reconnect_interval_ms=20,
        websocket_connect=connect,
    )
    sleep = ControlledSleep(hold=True)
    rt._sleep = sleep
    try:
        with pytest.raises(Exception, match="HTTP 429"):
            await rt.connect()
        assert rt.state == "backoff"
        await _until(lambda: bool(sleep.delays))
        assert sleep.delays[0] <= 0.02  # Retry-After ignored: the ordinary step
        assert rt.blocked_reason is None and rt.rate_limited_ms is None
        sleep.release()
        await _until(lambda: attempts == 2)
        await ok.push({"type": "hello.ok"})
        await _until(lambda: rt.state == "ready")
    finally:
        sleep.release()
        await rt.disconnect()


