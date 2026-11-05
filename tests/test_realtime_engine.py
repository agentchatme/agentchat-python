"""Realtime engine behaviour: ``wake`` frames, ``ack_on_receipt``, and running
as a guest (several instances per loop, a clean stop).

Shares the socket stand-ins with :mod:`tests.test_realtime`.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from agentchatme import RealtimeClient
from agentchatme.types.ws import WakePayload

from .test_realtime import MockAsyncClient, MockWebSocket, _ack_frames, _make_client, _settle

WAKE: dict[str, Any] = {
    "type": "wake",
    "payload": {
        "reason": "reminder",
        "conversation_id": "conv_1",
        "anchor_message_id": "msg_1",
        "step": 2,
        "at": "2026-11-05T00:00:00.000Z",
    },
}


def message_new(conversation_id: str, seq: int, **payload: Any) -> dict[str, Any]:
    body: dict[str, Any] = {"id": f"msg_{seq}", "conversation_id": conversation_id, "seq": seq}
    body.update(payload)
    return {"type": "message.new", "payload": body}


async def connected(
    *, capabilities: list[str] | None = None, **opts: Any
) -> tuple[RealtimeClient, MockWebSocket]:
    rt, ws = _make_client(**opts)
    await rt.connect()
    await _settle()
    hello_ok: dict[str, Any] = {"type": "hello.ok"}
    if capabilities is not None:
        hello_ok["capabilities"] = capabilities
    await ws.push(hello_ok)
    await _settle()
    return rt, ws


def other_tasks() -> set[asyncio.Task[Any]]:
    current = asyncio.current_task()
    return {t for t in asyncio.all_tasks() if t is not current}


# ─────────────── wake frames ───────────────


@pytest.mark.asyncio
async def test_wake_frame_is_dispatched_to_wake_handlers_with_its_payload() -> None:
    rt, ws = await connected(capabilities=["ack"])
    seen: list[str] = []

    def on_wake(frame: dict[str, Any]) -> None:
        payload = WakePayload.model_validate(frame["payload"])
        seen.append(f"{payload.reason}:{payload.conversation_id}:{payload.step}")

    rt.on("wake", on_wake)
    try:
        await ws.push(WAKE)
        await _settle()
        assert seen == ["reminder:conv_1:2"]
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_wake_is_never_acked_and_never_reaches_message_handlers() -> None:
    rt, ws = await connected(capabilities=["ack"])
    messages: list[dict[str, Any]] = []
    wakes: list[dict[str, Any]] = []
    rt.on("message.new", lambda m: messages.append(m))
    rt.on("wake", lambda m: wakes.append(m))
    try:
        await ws.push(WAKE)
        await _settle()
        assert messages == []
        assert wakes == [WAKE]
        assert _ack_frames(ws) == []
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_malformed_wake_is_surfaced_through_on_error_not_dispatched() -> None:
    rt, ws = await connected(capabilities=["ack"])
    wakes: list[dict[str, Any]] = []
    errors: list[BaseException] = []
    rt.on("wake", lambda m: wakes.append(m))
    rt.on_error(lambda e: errors.append(e))
    try:
        await ws.push({"type": "wake", "payload": {"reason": "reminder"}})
        await ws.push({"type": "wake", "payload": "soon"})
        await ws.push({"type": "wake"})
        await ws.push({"type": "wake", "payload": {**WAKE["payload"], "step": "1"}})
        await _settle()
        assert wakes == []
        assert len(errors) == 4
        assert all("wake frame failed validation" in str(e) for e in errors)
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_frame_types_without_a_subscriber_are_still_ignored() -> None:
    rt, ws = await connected(capabilities=["ack"])
    errors: list[BaseException] = []
    rt.on_error(lambda e: errors.append(e))
    try:
        await ws.push({"type": "something.new", "payload": {"x": 1}})
        await ws.push(WAKE)  # no wake handler registered either
        await _settle()
        assert errors == []
        assert _ack_frames(ws) == []
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_wake_handler_unsubscribes() -> None:
    rt, ws = await connected(capabilities=["ack"])
    wakes: list[dict[str, Any]] = []
    off = rt.on("wake", lambda m: wakes.append(m))
    off()
    try:
        await ws.push(WAKE)
        await _settle()
        assert wakes == []
    finally:
        await rt.disconnect()


# ─────────────── ack_on_receipt ───────────────


@pytest.mark.asyncio
async def test_ack_on_receipt_acks_before_any_handler_runs_and_exactly_once() -> None:
    rt, ws = await connected(capabilities=["ack"], ack_on_receipt=True)
    acks_when_handler_ran: list[int] = []
    rt.on("message.new", lambda m: acks_when_handler_ran.append(len(_ack_frames(ws))))
    try:
        await ws.push(message_new("c", 1))
        await _settle()
        assert acks_when_handler_ran == [1]
        assert _ack_frames(ws) == ["msg_1"]
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_ack_on_receipt_acks_even_when_the_handler_raises() -> None:
    rt, ws = await connected(capabilities=["ack"], ack_on_receipt=True)

    def boom(_m: dict[str, Any]) -> None:
        raise RuntimeError("boom")

    rt.on("message.new", boom)
    try:
        await ws.push(message_new("c", 1))
        await _settle()
        assert _ack_frames(ws) == ["msg_1"]
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_ack_on_receipt_acks_a_parked_frame_at_receipt_not_at_dispatch() -> None:
    rt, ws = await connected(capabilities=["ack"], ack_on_receipt=True)
    dispatched: list[int] = []
    rt.on("message.new", lambda m: dispatched.append(m["payload"]["seq"]))
    try:
        await ws.push(message_new("c", 1))
        await ws.push(message_new("c", 3))  # parked until seq 2 arrives
        await _settle()
        assert dispatched == [1]
        assert _ack_frames(ws) == ["msg_1", "msg_3"]

        await ws.push(message_new("c", 2))
        await _settle()
        assert dispatched == [1, 2, 3]
        # No second ack for seq 3 once it leaves the buffer.
        assert _ack_frames(ws) == ["msg_1", "msg_3", "msg_2"]
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_ack_on_receipt_acks_a_redelivered_duplicate_without_redispatch() -> None:
    rt, ws = await connected(capabilities=["ack"], ack_on_receipt=True)
    handled: list[str] = []
    rt.on("message.new", lambda m: handled.append(m["payload"]["id"]))
    try:
        await ws.push(message_new("c", 1))
        await _settle()
        # Same message id in another conversation/seq: dedup is by id.
        await ws.push(message_new("d", 7, id="msg_1"))
        await _settle()
        assert handled == ["msg_1"]
        assert _ack_frames(ws) == ["msg_1", "msg_1"]
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_ack_on_receipt_sends_nothing_without_a_negotiated_ack() -> None:
    rt, ws = await connected(ack_on_receipt=True)  # hello.ok without capabilities
    rt.on("message.new", lambda m: None)
    try:
        await ws.push(message_new("c", 1))
        await _settle()
        assert _ack_frames(ws) == []
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_default_mode_still_acks_after_handlers_settle() -> None:
    rt, ws = await connected(capabilities=["ack"])
    release = asyncio.Event()

    async def slow(_m: dict[str, Any]) -> None:
        await release.wait()

    rt.on("message.new", slow)
    try:
        await ws.push(message_new("c", 1))
        await _settle()
        assert _ack_frames(ws) == []
        release.set()
        await _settle()
        assert _ack_frames(ws) == ["msg_1"]
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_ack_on_receipt_acks_gap_fill_rows_when_fetched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("agentchatme._realtime._GAP_FILL_WINDOW_S", 0.05)
    filled = {"id": "msg_2", "conversation_id": "c", "seq": 2}
    api = MockAsyncClient(get_messages_result=[filled])
    rt, ws = await connected(
        capabilities=["ack"], ack_on_receipt=True, client=api, auto_drain_on_connect=False
    )
    dispatched: list[int] = []
    rt.on("message.new", lambda m: dispatched.append(m["payload"]["seq"]))
    try:
        await ws.push(message_new("c", 1))
        await ws.push(message_new("c", 3))
        await asyncio.sleep(0.12)
        await _settle()
        assert dispatched == [1, 2, 3]
        # Live frames at receipt, the gap-fill row at fetch, each exactly once.
        assert _ack_frames(ws) == ["msg_1", "msg_3", "msg_2"]
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_ack_on_receipt_flushes_acked_parked_frames_when_the_socket_closes() -> None:
    # The parked frame was acked at receipt, so the server will never
    # re-offer it: on a socket drop it must reach the handlers, in seq
    # order, instead of vanishing with the ordering state.
    gaps: list[Any] = []
    rt, ws = await connected(
        capabilities=["ack"], ack_on_receipt=True, on_sequence_gap=lambda info: gaps.append(info)
    )
    dispatched: list[int] = []
    rt.on("message.new", lambda m: dispatched.append(m["payload"]["seq"]))
    try:
        await ws.push(message_new("c", 1))
        await ws.push(message_new("c", 4))
        await ws.push(message_new("c", 3))
        await _settle()
        assert dispatched == [1]
        assert _ack_frames(ws) == ["msg_1", "msg_4", "msg_3"]
        await ws.close(code=1006, reason="dropped")
        await _settle()
        assert dispatched == [1, 3, 4]
        assert [g.reason for g in gaps] == ["connection_closed"]
        assert gaps[0].recovered is False and gaps[0].buffered_seq == 3
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_default_mode_leaves_unacked_parked_frames_to_the_next_drain() -> None:
    gaps: list[Any] = []
    rt, ws = await connected(capabilities=["ack"], on_sequence_gap=lambda info: gaps.append(info))
    dispatched: list[int] = []
    rt.on("message.new", lambda m: dispatched.append(m["payload"]["seq"]))
    try:
        await ws.push(message_new("c", 1))
        await ws.push(message_new("c", 3))
        await _settle()
        await ws.close(code=1006, reason="dropped")
        await _settle()
        # Not acked, so not flushed: the server re-offers seq 3 in order.
        assert dispatched == [1]
        assert _ack_frames(ws) == ["msg_1"]
        assert gaps == []
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_ack_on_receipt_delivers_a_below_anchor_frame_it_already_acked() -> None:
    # The server registers the socket before it starts the reconnect drain,
    # so pub/sub can deliver a fresh seq 5 first; the drain then pushes the
    # stored seq 3 and 4. Acked at parse, they must reach the handlers
    # (out of order, not lost); a redelivery of one is acked again only.
    rt, ws = await connected(capabilities=["ack"], ack_on_receipt=True)
    dispatched: list[int] = []
    rt.on("message.new", lambda m: dispatched.append(m["payload"]["seq"]))
    try:
        await ws.push(message_new("c", 5))
        await ws.push(message_new("c", 3))
        await ws.push(message_new("c", 4))
        await _settle()
        assert dispatched == [5, 3, 4]
        assert _ack_frames(ws) == ["msg_5", "msg_3", "msg_4"]

        await ws.push(message_new("c", 3))  # server double-publish
        await _settle()
        assert dispatched == [5, 3, 4]
        assert _ack_frames(ws) == ["msg_5", "msg_3", "msg_4", "msg_3"]
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_default_mode_acks_gap_fill_rows_after_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("agentchatme._realtime._GAP_FILL_WINDOW_S", 0.05)
    filled = {"id": "msg_2", "conversation_id": "c", "seq": 2}
    api = MockAsyncClient(get_messages_result=[filled])
    rt, ws = await connected(capabilities=["ack"], client=api, auto_drain_on_connect=False)
    dispatched: list[int] = []
    rt.on("message.new", lambda m: dispatched.append(m["payload"]["seq"]))
    try:
        await ws.push(message_new("c", 1))
        await ws.push(message_new("c", 3))
        await asyncio.sleep(0.12)
        await _settle()
        assert dispatched == [1, 2, 3]
        # The fetched row rides the live ack path: acked once dispatched.
        assert _ack_frames(ws) == ["msg_1", "msg_2", "msg_3"]
    finally:
        await rt.disconnect()


# ─────────────── a guest in a host process ───────────────


@pytest.mark.asyncio
async def test_two_instances_run_side_by_side_and_closing_one_leaves_the_other_untouched() -> None:
    baseline = other_tasks()
    ws_a, ws_b = MockWebSocket(), MockWebSocket()
    connects: list[str] = []

    def connector(sock: MockWebSocket, label: str) -> Any:
        async def fake_connect(_url: str, **_kw: Any) -> MockWebSocket:
            connects.append(label)
            return sock

        return fake_connect

    a = RealtimeClient(
        api_key="ka", reconnect=True, reconnect_interval_ms=10, max_reconnect_interval_ms=20,
        ack_on_receipt=True, websocket_connect=connector(ws_a, "a"),
    )
    b = RealtimeClient(
        api_key="kb", reconnect=True, reconnect_interval_ms=10, max_reconnect_interval_ms=20,
        websocket_connect=connector(ws_b, "b"),
    )
    seen_a: list[str] = []
    seen_b: list[str] = []
    a.on("message.new", lambda m: seen_a.append(m["payload"]["id"]))
    b.on("message.new", lambda m: seen_b.append(m["payload"]["id"]))

    await a.connect()
    await b.connect()
    await _settle()
    await ws_a.push({"type": "hello.ok", "capabilities": ["ack"]})
    await ws_b.push({"type": "hello.ok", "capabilities": ["ack"]})
    await _settle()
    assert a.is_connected and b.is_connected
    # Each socket authenticated with its own key.
    assert json.loads(ws_a.sent[0])["api_key"] == "ka"
    assert json.loads(ws_b.sent[0])["api_key"] == "kb"

    # Park a frame on a seq gap in a, so a owns a gap timer too.
    await ws_a.push(message_new("c", 1))
    await ws_a.push(message_new("c", 3))
    await _settle()

    await a.disconnect()
    assert ws_a.closed and not ws_b.closed
    assert not a.is_connected and b.is_connected
    # The shutdown flush handed the parked frame over; nothing of a remains.
    assert seen_a == ["msg_1", "msg_3"]

    # b still receives, dedups and acks on its own socket.
    await ws_b.push(message_new("c", 1))
    await _settle()
    assert seen_b == ["msg_1"]
    assert _ack_frames(ws_b) == ["msg_1"]
    assert _ack_frames(ws_a) == ["msg_1", "msg_3"]

    # a is not reopened, however long we wait past its backoff.
    await asyncio.sleep(0.1)
    assert connects == ["a", "b"]
    assert b.is_connected

    await b.disconnect()
    await _settle()
    # Every task either instance owned is gone from the loop.
    assert other_tasks() - baseline == set()


@pytest.mark.asyncio
async def test_disconnect_releases_a_pending_reconnect_task() -> None:
    baseline = other_tasks()
    sock = MockWebSocket()
    connects = 0

    async def fake_connect(_url: str, **_kw: Any) -> MockWebSocket:
        nonlocal connects
        connects += 1
        return sock

    rt = RealtimeClient(
        api_key="k", reconnect=True, reconnect_interval_ms=50, max_reconnect_interval_ms=50,
        websocket_connect=fake_connect,
    )
    await rt.connect()
    await _settle()
    await sock.push({"type": "hello.ok"})
    await _settle()
    await sock.close(code=1006, reason="blip")  # schedules a reconnect
    await _settle()
    assert rt._reconnect_task is not None and not rt._reconnect_task.done()

    await rt.disconnect()
    await asyncio.sleep(0.1)
    assert connects == 1
    assert other_tasks() - baseline == set()


@pytest.mark.asyncio
async def test_disconnect_releases_the_hello_ack_timer_mid_handshake() -> None:
    baseline = other_tasks()
    rt, _ws = _make_client()
    await rt.connect()  # HELLO sent, 15 s handshake watchdog armed, no hello.ok
    await _settle()
    assert rt._handshake_task is not None and not rt._handshake_task.done()
    await rt.disconnect()
    assert other_tasks() - baseline == set()


@pytest.mark.asyncio
async def test_disconnect_releases_an_in_flight_gap_fill_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("agentchatme._realtime._GAP_FILL_WINDOW_S", 0.01)
    baseline = other_tasks()
    started = asyncio.Event()
    release = asyncio.Event()

    class _SlowFill(MockAsyncClient):
        async def get_messages(self, conversation_id: str, **kwargs: Any) -> list[dict[str, Any]]:
            started.set()
            await release.wait()
            return await super().get_messages(conversation_id, **kwargs)

    api = _SlowFill(get_messages_result=[{"id": "msg_2", "conversation_id": "c", "seq": 2}])
    rt, ws = await connected(capabilities=["ack"], client=api, auto_drain_on_connect=False)
    rt.on("message.new", lambda m: None)
    await ws.push(message_new("c", 1))
    await ws.push(message_new("c", 3))
    await asyncio.wait_for(started.wait(), timeout=2.0)  # the fetch is in flight

    await rt.disconnect()
    assert other_tasks() - baseline == set()
    release.set()
    await _settle()
    assert other_tasks() - baseline == set()


@pytest.mark.asyncio
async def test_two_instances_from_one_options_object_do_not_interfere() -> None:
    from agentchatme import RealtimeOptions

    sockets: dict[str, MockWebSocket] = {}
    connects: list[str] = []
    shared = RealtimeOptions(
        api_key="k", reconnect=True, reconnect_interval_ms=10, max_reconnect_interval_ms=20
    )

    def connector(label: str) -> Any:
        async def fake_connect(_url: str, **_kw: Any) -> MockWebSocket:
            connects.append(label)
            sockets[label] = MockWebSocket()
            return sockets[label]

        return fake_connect

    a = RealtimeClient(shared, websocket_connect=connector("a"))
    b = RealtimeClient(shared, websocket_connect=connector("b"))
    await a.connect()
    await b.connect()
    await _settle()
    await sockets["a"].push({"type": "hello.ok"})
    await sockets["b"].push({"type": "hello.ok"})
    await _settle()

    await a.disconnect()
    # The caller's object is untouched, and so is b.
    assert shared.reconnect is True
    assert shared.auto_drain_on_connect is None
    assert b._opts.reconnect is True

    # b still reconnects after a server-side drop; a stays closed.
    await sockets["b"].close(code=1006, reason="blip")
    await asyncio.sleep(0.1)
    assert connects == ["a", "b", "b"]
    await b.disconnect()


@pytest.mark.asyncio
async def test_disconnect_from_inside_a_handler_is_safe() -> None:
    rt, ws = await connected(capabilities=["ack"])
    done = asyncio.Event()

    async def close_from_handler(_m: dict[str, Any]) -> None:
        await rt.disconnect()
        done.set()

    rt.on("message.new", close_from_handler)
    await ws.push(message_new("c", 1))
    await asyncio.wait_for(done.wait(), timeout=2.0)
    await _settle()
    assert not rt.is_connected
    assert ws.closed
