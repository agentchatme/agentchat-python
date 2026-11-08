"""Realtime engine behaviour: ``wake`` frames, ``ack_on_receipt``, and running
as a guest (several instances per loop, a clean stop).

Shares the socket stand-ins with :mod:`tests.test_realtime`.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import pytest

from agentchatme import RealtimeClient, _realtime
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


def _noop(_message: dict[str, Any]) -> None:
    return None


async def connected(
    *,
    capabilities: list[str] | None = None,
    with_handler: bool = True,
    offer: dict[str, bool] | None = None,
    **opts: Any,
) -> tuple[RealtimeClient, MockWebSocket]:
    """A client past hello.ok. HELLO offers ``ack`` only while a
    ``message.new`` handler is attached, so a no-op one is attached first
    (``with_handler=False`` for a handler-less client). ``capabilities`` is
    what the server echoes; ``offer`` the client's capability options."""
    if offer is not None:
        opts["capabilities"] = offer
    rt, ws = _make_client(**opts)
    if with_handler:
        rt.on("message.new", _noop)
    await rt.connect()
    await _settle()
    hello_ok: dict[str, Any] = {"type": "hello.ok"}
    if capabilities is not None:
        hello_ok["capabilities"] = capabilities
    await ws.push(hello_ok)
    await _settle()
    return rt, ws


def _connector(sock: MockWebSocket) -> Any:
    async def connect(_url: str, **_kw: Any) -> MockWebSocket:
        return sock

    return connect


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
async def test_default_mode_acks_a_below_anchor_repeat_it_already_handled() -> None:
    # Seq 3 parks, so the ordering state stays live and the repeat of seq 1
    # really is below the anchor. It is not handed over again, and it is
    # acknowledged again: the earlier dispatch is the proof (the dedup rule
    # of the delivery-ack protocol).
    rt, ws = await connected(capabilities=["ack"])
    handled: list[str] = []
    rt.on("message.new", lambda m: handled.append(m["payload"]["id"]))
    try:
        await ws.push(message_new("c", 1))
        await ws.push(message_new("c", 3))
        await _settle()
        await ws.push(message_new("c", 1))
        await _settle()
        assert handled == ["msg_1"]
        assert _ack_frames(ws) == ["msg_1", "msg_1"]

        await ws.push(message_new("c", 2))
        await _settle()
        assert handled == ["msg_1", "msg_2", "msg_3"]
        assert _ack_frames(ws) == ["msg_1", "msg_1", "msg_2", "msg_3"]
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_default_mode_delivers_a_below_anchor_frame_nobody_handled_late() -> None:
    # Live seq 5 anchors before the server's reconnect backlog brings the
    # stored seq 3. Nobody has seen it: handed over late (out of order, never
    # stalled) and acked once handled.
    rt, ws = await connected(capabilities=["ack"])
    handled: list[str] = []
    rt.on("message.new", lambda m: handled.append(m["payload"]["id"]))
    try:
        await ws.push(message_new("c", 5))
        await ws.push(message_new("c", 7))
        await _settle()
        await ws.push(message_new("c", 3))
        await _settle()
        assert handled == ["msg_5", "msg_3"]
        assert _ack_frames(ws) == ["msg_5", "msg_3"]
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


# ─────────────── a repeat while the first copy is still with the handlers ───────────────
#
# After a reconnect the server pushes its stored backlog on the socket and the
# REST drain brings the same rows. With a slow handler the second copy comes
# before the first one finished, so the dedup cache (filled on success) cannot
# catch it.


def _row(seq: int, *, mid: str | None = None) -> dict[str, Any]:
    return {
        "id": mid or f"msg_{seq}",
        "conversation_id": "c",
        "seq": seq,
        "delivery_id": f"del_{seq}",
        "sender": "nova",
        "type": "text",
        "content": {"text": "hi"},
        "created_at": "2026-11-04T23:00:00.000Z",
    }


@pytest.mark.parametrize("ack_on_receipt", [False, True])
@pytest.mark.asyncio
async def test_a_drain_repeat_of_a_running_message_is_not_dispatched_and_never_waited_for(
    ack_on_receipt: bool,
) -> None:
    # The live copy of seq 2 has a slow handler. The drain brings the same
    # row and, after it, a row of another conversation. The repeat is not
    # dispatched, the drain does not wait for the live handler (the other
    # conversation's row is handled meanwhile and the drain finishes), and
    # the cursor stops at the repeat; the next drain covers it as a dedup hit.
    other = {**_row(1, mid="msg_d1"), "conversation_id": "d", "delivery_id": "del_d1"}
    api = MockAsyncClient(sync_pages=[[_row(2), other], [_row(2), other]])
    rt, ws = await connected(
        capabilities=["ack"], client=api, auto_drain_on_connect=False, ack_on_receipt=ack_on_receipt
    )
    release = asyncio.Event()
    calls: list[str] = []

    async def slow(m: dict[str, Any]) -> None:
        calls.append(m["payload"]["id"])
        if m["payload"]["id"] == "msg_2":
            await release.wait()

    rt.on("message.new", slow)
    try:
        await ws.push(message_new("c", 1))
        await ws.push(message_new("c", 2))  # in order; its handler is slow
        await _settle()
        await asyncio.wait_for(rt.drain_offline_envelopes(), timeout=1.0)
        await _settle()  # the other row's handler runs as its own task
        assert calls == ["msg_1", "msg_2", "msg_d1"]
        assert api.sync_ack_calls == []

        release.set()
        await _settle()
        assert _ack_frames(ws) == ["msg_1", "msg_2"]
        await asyncio.wait_for(rt.drain_offline_envelopes(), timeout=1.0)
        assert calls == ["msg_1", "msg_2", "msg_d1"]
        assert api.sync_ack_calls == ["del_d1"]
    finally:
        release.set()
        await rt.disconnect()


@pytest.mark.asyncio
async def test_a_live_repeat_of_a_first_copy_that_failed_is_never_acked() -> None:
    # The first copy is a drain row whose handler fails; the server's
    # backlog push brings the same message live, below the anchor, while it
    # runs. The repeat adopts the first copy, so it must not be acked.
    api = MockAsyncClient(sync_pages=[[_row(1)]])
    rt, ws = await connected(capabilities=["ack"], client=api, auto_drain_on_connect=False)
    release = asyncio.Event()

    async def fails(_m: dict[str, Any]) -> None:
        await release.wait()
        raise RuntimeError("model call failed")

    rt.on("message.new", fails)
    try:
        drain = asyncio.create_task(rt.drain_offline_envelopes())
        await _settle()
        await ws.push(message_new("c", 1))
        await _settle()
        release.set()
        await asyncio.wait_for(drain, timeout=1.0)
        await _settle()
        assert _ack_frames(ws) == []
        assert api.sync_ack_calls == []
    finally:
        release.set()
        await rt.disconnect()


@pytest.mark.asyncio
async def test_a_copy_on_a_later_connection_adopts_the_first_copy_still_running() -> None:
    # In-flight dedupe holds across reconnects: the server re-offers an
    # unacked frame on the next connection while its first handler may still
    # run. The re-offer is not dispatched again; it takes the first copy's
    # outcome without waiting, and is acked once that copy finished cleanly.
    rt, ws = await connected(capabilities=["ack"])
    release = asyncio.Event()
    calls: list[str] = []

    async def slow(m: dict[str, Any]) -> None:
        calls.append(m["payload"]["id"])
        await release.wait()

    rt.on("message.new", slow)
    try:
        await ws.push(message_new("c", 1))
        await _settle()
        assert calls == ["msg_1"]
        await ws.close(code=1006, reason="dropped")  # the first handler still runs
        await _settle()
        ws2 = MockWebSocket()
        rt._websocket_connect = _connector(ws2)
        await rt.connect()
        await ws2.push({"type": "hello.ok", "capabilities": ["ack"]})
        await ws2.push(message_new("c", 1))  # the server's re-offer
        await _settle()
        assert calls == ["msg_1"]
        assert _ack_frames(ws2) == []
        release.set()
        await _settle()
        assert calls == ["msg_1"]
        # The first copy's settlement and the adopted re-offer each ack on
        # the current socket (the TypeScript SDK's behaviour; the server
        # takes a repeated ack as a no-op).
        assert _ack_frames(ws2) == ["msg_1", "msg_1"]
    finally:
        release.set()
        await rt.disconnect()


@pytest.mark.asyncio
async def test_a_live_repeat_below_the_anchor_never_holds_up_the_socket() -> None:
    # The first copy is a drain row whose handler is slow. The server's
    # backlog push brings the same message live, below the anchor. It is not
    # dispatched, the socket keeps flowing (another conversation's frame is
    # handled meanwhile), and it is acked once the first copy finished.
    api = MockAsyncClient(sync_pages=[[_row(1)]])
    rt, ws = await connected(capabilities=["ack"], client=api, auto_drain_on_connect=False)
    release = asyncio.Event()
    calls: list[str] = []

    async def slow(m: dict[str, Any]) -> None:
        calls.append(m["payload"]["id"])
        if m["payload"]["id"] == "msg_1":
            await release.wait()

    rt.on("message.new", slow)
    try:
        drain = asyncio.create_task(rt.drain_offline_envelopes())
        await _settle()
        assert calls == ["msg_1"]
        await ws.push(message_new("c", 1))  # the same message, live
        await ws.push(message_new("other", 9, id="msg_other"))
        await _settle()
        assert calls == ["msg_1", "msg_other"]
        assert _ack_frames(ws) == ["msg_other"]

        release.set()
        await asyncio.wait_for(drain, timeout=1.0)
        await _settle()
        assert calls == ["msg_1", "msg_other"]
        assert _ack_frames(ws) == ["msg_other", "msg_1"]
        assert api.sync_ack_calls == ["del_1"]
    finally:
        release.set()
        await rt.disconnect()


@pytest.mark.asyncio
async def test_a_failed_first_copy_leaves_the_repeat_unacked_and_a_later_copy_is_dispatched() -> None:
    api = MockAsyncClient(sync_pages=[[_row(2)]])
    rt, ws = await connected(capabilities=["ack"], client=api, auto_drain_on_connect=False)
    release = asyncio.Event()
    calls: list[str] = []

    async def flaky(m: dict[str, Any]) -> None:
        calls.append(m["payload"]["id"])
        if m["payload"]["id"] == "msg_2" and calls.count("msg_2") == 1:
            await release.wait()
            raise RuntimeError("model call failed")

    rt.on("message.new", flaky)
    try:
        await ws.push(message_new("c", 1))
        await ws.push(message_new("c", 2))
        await _settle()
        drain = asyncio.create_task(rt.drain_offline_envelopes())
        await _settle()
        release.set()
        await asyncio.wait_for(drain, timeout=1.0)
        await _settle()
        # Neither copy is acknowledged: the server re-offers the message.
        assert calls == ["msg_1", "msg_2"]
        assert api.sync_ack_calls == []
        assert _ack_frames(ws) == ["msg_1"]

        # The re-offer is a fresh attempt.
        await ws.push(message_new("c", 2))
        await _settle()
        assert calls == ["msg_1", "msg_2", "msg_2"]
        assert _ack_frames(ws) == ["msg_1", "msg_2"]
    finally:
        release.set()
        await rt.disconnect()


@pytest.mark.asyncio
async def test_a_cancelled_first_copy_counts_as_not_handled() -> None:
    rt, ws = await connected(capabilities=["ack"])
    entered = asyncio.Event()

    async def hangs(_m: dict[str, Any]) -> None:
        entered.set()
        await asyncio.Event().wait()

    rt.on("message.new", hangs)
    try:
        frame = message_new("c", 1)
        settlement = await rt._deliver_message_new(frame, ws_ack=True)
        await entered.wait()
        assert rt._adopt_in_flight(dict(frame), ws_ack=True) is not None
        settlement.cancel()
        await _settle()
        assert _ack_frames(ws) == []
        assert rt._inflight == {}
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_with_the_dedup_cache_off_every_copy_is_dispatched() -> None:
    rt, _ws = await connected(capabilities=["ack"], dedup_cache_size=0)
    release = asyncio.Event()
    calls: list[str] = []

    async def slow(m: dict[str, Any]) -> None:
        calls.append(m["payload"]["id"])
        await release.wait()

    rt.on("message.new", slow)
    try:
        frame = message_new("c", 1)
        tasks = [
            asyncio.create_task(rt._deliver_message_new(dict(frame), ws_ack=False))
            for _ in range(2)
        ]
        await _settle()
        assert calls == ["msg_1", "msg_1"]
        release.set()
        await asyncio.gather(*tasks)
    finally:
        release.set()
        await rt.disconnect()


def test_the_ordering_cursor_never_moves_backwards() -> None:
    state = _realtime._OrderState(next_expected_seq=8)
    _realtime._advance_past(state, 5)  # a late copy of seq 5 finishing
    assert state.next_expected_seq == 8
    _realtime._advance_past(state, 8)
    assert state.next_expected_seq == 9


@pytest.mark.asyncio
async def test_cleanup_never_drops_a_state_a_close_replaced() -> None:
    rt, _ws = await connected(capabilities=["ack"])
    try:
        stale = _realtime._OrderState(next_expected_seq=3)
        current = _realtime._OrderState(next_expected_seq=1)
        current.buffer[2] = _realtime._PendingEnvelope(message=message_new("c", 2), ws_ack=True)
        rt._order_states["c"] = current
        rt._cleanup_if_idle("c", stale)  # an idle orphan from before the close
        assert rt._order_states["c"] is current
    finally:
        await rt.disconnect()


# ─────────────── resolving a gap while a handler runs ───────────────
#
# When the gap window lapses, the gap task hands the parked frames over and
# awaits each handler. The receive loop keeps running meanwhile, so frames
# arrive while the gap is being resolved.


def _gap_hook(errors: list[BaseException]) -> None:
    asyncio.get_running_loop().set_exception_handler(
        lambda _loop, context: errors.append(context.get("exception") or RuntimeError(context["message"]))
    )


@pytest.mark.parametrize("ack_on_receipt", [False, True])
@pytest.mark.asyncio
async def test_a_frame_that_parks_while_a_resolved_gap_is_handled_is_dispatched(
    monkeypatch: pytest.MonkeyPatch, ack_on_receipt: bool
) -> None:
    # No REST client, so the lapsed gap is resolved without a fill. Seq 5
    # parks while the handler for seq 3 runs. It was cleared with the buffer
    # without ever being dispatched; under ack_on_receipt it was already
    # acked, so no handler would ever see it.
    monkeypatch.setattr("agentchatme._realtime._GAP_FILL_WINDOW_S", 0.05)
    rt, ws = await connected(capabilities=["ack"], ack_on_receipt=ack_on_receipt)
    release = asyncio.Event()
    entered = asyncio.Event()
    seen: list[int] = []

    async def slow(m: dict[str, Any]) -> None:
        seen.append(m["payload"]["seq"])
        if m["payload"]["seq"] == 3:
            entered.set()
            await release.wait()

    rt.on("message.new", slow)
    try:
        await ws.push(message_new("c", 1))
        await ws.push(message_new("c", 3))
        await asyncio.wait_for(entered.wait(), timeout=1.0)  # gap lapsed; 3 handed over
        await ws.push(message_new("c", 5))
        await _settle()
        release.set()
        await asyncio.sleep(0.2)
        assert seen == [1, 3, 5]
        assert sorted(_ack_frames(ws)) == ["msg_1", "msg_3", "msg_5"]
    finally:
        release.set()
        await rt.disconnect()


@pytest.mark.asyncio
async def test_the_missing_seq_arriving_while_a_resolved_gap_is_handled_is_not_a_crash(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Seq 2 arrives while the lapsed gap's handler for seq 3 runs. The
    # receive loop dispatches it in order and drains the buffer; the gap
    # task must not then look up an entry that is gone (KeyError) or hand
    # seq 4 over a second time.
    monkeypatch.setattr("agentchatme._realtime._GAP_FILL_WINDOW_S", 0.05)
    errors: list[BaseException] = []
    _gap_hook(errors)
    rt, ws = await connected(capabilities=["ack"], ack_on_receipt=True)
    release = asyncio.Event()
    entered = asyncio.Event()
    seen: list[int] = []

    async def slow(m: dict[str, Any]) -> None:
        seen.append(m["payload"]["seq"])
        if m["payload"]["seq"] == 3:
            entered.set()
            await release.wait()

    rt.on("message.new", slow)
    try:
        await ws.push(message_new("c", 1))
        await ws.push(message_new("c", 3))
        await ws.push(message_new("c", 4))
        await asyncio.wait_for(entered.wait(), timeout=1.0)
        await ws.push(message_new("c", 2))
        await _settle()
        release.set()
        await asyncio.sleep(0.2)
        assert sorted(seen) == [1, 2, 3, 4]
        assert len(seen) == 4
        assert errors == []
        assert rt._order_states.get("c") is None or not rt._order_states["c"].buffer
    finally:
        release.set()
        await rt.disconnect()


@pytest.mark.asyncio
async def test_disconnect_takes_no_frame_in_once_it_began() -> None:
    # disconnect() flushes parked frames while the socket is still open. A
    # frame that arrives meanwhile is neither acked nor dispatched (the
    # server keeps it), so the flush ends on a fixed set.
    rt, ws = await connected(capabilities=["ack"], ack_on_receipt=True)
    release = asyncio.Event()
    entered = asyncio.Event()
    seen: list[int] = []

    async def slow(m: dict[str, Any]) -> None:
        seen.append(m["payload"]["seq"])
        if m["payload"]["seq"] == 3:
            entered.set()
            await release.wait()

    rt.on("message.new", slow)
    await ws.push(message_new("c", 1))
    await ws.push(message_new("c", 3))
    await _settle()
    stopping = asyncio.create_task(rt.disconnect())
    await asyncio.wait_for(entered.wait(), timeout=1.0)
    await ws.push(message_new("c", 6))
    await _settle()
    release.set()
    await asyncio.wait_for(stopping, timeout=3.0)
    assert seen == [1, 3]
    assert _ack_frames(ws) == ["msg_1", "msg_3"]


@pytest.mark.parametrize("ack_on_receipt", [False, True])
@pytest.mark.asyncio
async def test_disconnect_returns_promptly_while_frames_keep_arriving(ack_on_receipt: bool) -> None:
    rt, ws = await connected(capabilities=["ack"], ack_on_receipt=ack_on_receipt)

    async def slow(_m: dict[str, Any]) -> None:
        await asyncio.sleep(0.03)

    rt.on("message.new", slow)
    await ws.push(message_new("c", 1))
    await asyncio.sleep(0.05)
    await ws.push(message_new("c", 3))  # parks on the hole at 2
    await _settle()
    started = time.monotonic()
    stopping = asyncio.create_task(rt.disconnect())
    seq = 4
    while not stopping.done() and time.monotonic() - started < 2.0:
        await ws.push(message_new("c", seq))
        seq += 1
        await asyncio.sleep(0.01)
    await asyncio.wait_for(stopping, timeout=3.0)
    assert time.monotonic() - started < 0.5


@pytest.mark.asyncio
async def test_a_handler_that_disconnects_while_a_repeat_of_its_message_arrives_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("agentchatme._realtime._GAP_FILL_WINDOW_S", 0.05)
    rt, ws = await connected(capabilities=["ack"])
    entered = asyncio.Event()
    go = asyncio.Event()
    returned = asyncio.Event()

    async def h(m: dict[str, Any]) -> None:
        if m["payload"]["seq"] == 3:
            entered.set()
            await go.wait()
            await rt.disconnect()
            returned.set()

    rt.on("message.new", h)
    await ws.push(message_new("c", 1))
    await ws.push(message_new("c", 3))
    await asyncio.wait_for(entered.wait(), timeout=1.0)  # the lapsed gap handed 3 over
    await ws.push(message_new("c", 3))  # the server repeats it while its handler runs
    await _settle()
    go.set()
    await asyncio.wait_for(returned.wait(), timeout=2.0)
    assert rt.state == "stopped"
    # The handler's own dispatch settles once it returns (it ran off the
    # receive loop; disconnect() skipped the task calling it).
    await _settle()
    assert rt._inflight == {}


@pytest.mark.asyncio
async def test_a_gap_fill_that_returns_during_disconnect_acks_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Under ack_on_receipt a gap fill acks its rows as it receives them. If
    # the fetch returns while disconnect() is flushing, the shutdown then
    # cancels the gap task, possibly mid-handover: an acked row would never
    # be handled. Once disconnect() has begun the fill takes nothing in.
    monkeypatch.setattr("agentchatme._realtime._GAP_FILL_WINDOW_S", 0.05)
    fetch_gate = asyncio.Event()
    fetching = asyncio.Event()

    class _SlowFill(MockAsyncClient):
        async def get_messages(self, conversation_id: str, **kwargs: Any) -> list[dict[str, Any]]:
            fetching.set()
            await fetch_gate.wait()
            return [message_new("c", 2)["payload"]]

    rt, ws = await connected(
        capabilities=["ack"], client=_SlowFill(), auto_drain_on_connect=False, ack_on_receipt=True
    )
    release = asyncio.Event()
    entered = asyncio.Event()

    async def slow(m: dict[str, Any]) -> None:
        if m["payload"]["seq"] == 3:
            entered.set()
            await release.wait()

    rt.on("message.new", slow)
    await ws.push(message_new("c", 1))
    await ws.push(message_new("c", 3))
    await asyncio.wait_for(fetching.wait(), timeout=1.0)  # the gap fill is in flight
    stopping = asyncio.create_task(rt.disconnect())
    await asyncio.wait_for(entered.wait(), timeout=1.0)  # the flush hands 3 over
    fetch_gate.set()  # the fetch returns mid-shutdown
    await _settle()
    release.set()
    await asyncio.wait_for(stopping, timeout=3.0)
    assert "msg_2" not in _ack_frames(ws)


@pytest.mark.asyncio
async def test_an_adopted_in_order_repeat_moves_the_cursor_on(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The drain hands seq 2 over first and its handler is slow; the live
    # repeat of seq 2 adopts it. The next live frame (seq 3) must be taken in
    # order, not parked behind a gap that does not exist (2 s later: a REST
    # gap fill and a gap report although nothing was missing).
    monkeypatch.setattr("agentchatme._realtime._GAP_FILL_WINDOW_S", 0.05)
    gaps: list[Any] = []
    api = MockAsyncClient(sync_pages=[[_row(2)]])
    rt, ws = await connected(
        capabilities=["ack"], client=api, auto_drain_on_connect=False, on_sequence_gap=gaps.append
    )
    release = asyncio.Event()
    seen: list[int] = []

    async def slow(m: dict[str, Any]) -> None:
        seen.append(m["payload"]["seq"])
        if m["payload"]["seq"] == 2 and seen.count(2) == 1:
            await release.wait()

    rt.on("message.new", slow)
    try:
        await ws.push(message_new("c", 1))
        await _settle()
        drain = asyncio.create_task(rt.drain_offline_envelopes())
        await _settle()
        assert seen == [1, 2]  # the drain row's handler is running
        await ws.push(message_new("c", 2))  # live repeat: adopted
        await ws.push(message_new("c", 3))
        await _settle()
        assert seen == [1, 2, 3]
        await asyncio.sleep(0.2)  # well past the gap window
        assert gaps == []
        assert api.get_messages_calls == []
        release.set()
        await asyncio.wait_for(drain, timeout=1.0)
        await _settle()
        assert sorted(_ack_frames(ws)) == ["msg_1", "msg_2", "msg_3"]
    finally:
        release.set()
        await rt.disconnect()


@pytest.mark.asyncio
async def test_a_frame_behind_a_new_hole_gets_its_own_gap_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # While a lapsed gap's handler for seq 3 runs, seq 4 arrives in order and
    # seq 6 behind a new hole. Seq 4 is dispatched at once by the receive
    # loop; seq 6 waits for its own gap window and its own report.
    monkeypatch.setattr("agentchatme._realtime._GAP_FILL_WINDOW_S", 0.1)
    gaps: list[Any] = []
    rt, ws = await connected(capabilities=["ack"], on_sequence_gap=gaps.append)
    release = asyncio.Event()
    entered = asyncio.Event()
    seen: list[int] = []

    async def slow(m: dict[str, Any]) -> None:
        seen.append(m["payload"]["seq"])
        if m["payload"]["seq"] == 3:
            entered.set()
            await release.wait()

    rt.on("message.new", slow)
    try:
        await ws.push(message_new("c", 1))
        await ws.push(message_new("c", 3))
        await asyncio.wait_for(entered.wait(), timeout=1.0)
        await ws.push(message_new("c", 4))
        await ws.push(message_new("c", 6))
        await _settle()
        assert seen == [1, 3, 4]
        release.set()
        await _settle()
        assert seen == [1, 3, 4]
        assert [g.expected_seq for g in gaps] == [2]
        await asyncio.sleep(0.3)
        assert seen == [1, 3, 4, 6]
        assert [g.expected_seq for g in gaps] == [2, 5]
    finally:
        release.set()
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
