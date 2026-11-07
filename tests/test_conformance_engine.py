"""``conformance/v1/engine-scenarios.json``: the realtime engine's delivery
rules (drain cursor, parked frames across a close, below-anchor repeats) as
scenarios both SDKs run byte for byte.

The fixture holds the steps and the expected outcomes; this file only
interprets them. See the fixture's ``about``, ``ops`` and ``invariants``.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from agentchatme import RealtimeClient, _realtime

from .test_realtime import MockWebSocket, _make_client

FIXTURE = Path(__file__).resolve().parent.parent / "conformance" / "v1" / "engine-scenarios.json"
DOC: dict[str, Any] = json.loads(FIXTURE.read_text(encoding="utf-8"))

MODES = ["default", "ack_on_receipt"]
OPS = ["close", "connect", "drain", "frame", "gap_window_lapses", "release", "wait_handler"]
EXPECTATIONS = ["cursor_acks", "dispatched", "dispatched_includes", "ws_acks"]
INVARIANTS = ["acked_was_handled", "acks_follow_handling", "dispatched_once", "seq_order"]


async def _idle() -> None:
    """Let the client run until nothing is ready to run (held handlers stay
    pending): the drain task, the receive loop and their acks."""
    for _ in range(50):
        await asyncio.sleep(0)


class _RecordingSocket(MockWebSocket):
    def __init__(self, events: list[tuple[str, str]]) -> None:
        super().__init__()
        self._events = events

    async def send(self, data: str) -> None:
        frame = json.loads(data)
        if frame.get("type") == "ack":
            self._events.append(("ws_ack", str(frame.get("message_id"))))
        await super().send(data)


class _ScenarioApi:
    """The REST surface the engine uses: ``sync`` serves the scenario's pages
    in order and then empty pages, ``sync_ack`` records the cursor, gap-fill
    returns nothing."""

    def __init__(self, pages: list[list[dict[str, Any]]], events: list[tuple[str, str]]) -> None:
        self._pages = [list(page) for page in pages]
        self._events = events
        self.served: list[dict[str, Any]] = []

    async def sync(self, **_kwargs: Any) -> list[dict[str, Any]]:
        page = self._pages.pop(0) if self._pages else []
        self.served.extend(page)
        return page

    async def sync_ack(self, last_delivery_id: str, **_kwargs: Any) -> dict[str, Any]:
        self._events.append(("cursor", last_delivery_id))
        return {"acked": 1}

    async def get_messages(self, _conversation_id: str, **_kwargs: Any) -> list[dict[str, Any]]:
        return []


@dataclass
class _Outcome:
    dispatched: list[dict[str, Any]] = field(default_factory=list)
    # ("handled", message_id) | ("ws_ack", message_id) | ("cursor", delivery_id)
    events: list[tuple[str, str]] = field(default_factory=list)
    served: list[dict[str, Any]] = field(default_factory=list)


async def _run(scenario: dict[str, Any], mode: str) -> _Outcome:
    out = _Outcome()
    api = _ScenarioApi(scenario.get("sync_pages", []), out.events)
    holds = {mid: asyncio.Event() for mid in scenario.get("hold", [])}
    failing = set(scenario.get("fail", []))

    async def handler(message: dict[str, Any]) -> None:
        payload = message["payload"]
        out.dispatched.append(payload)
        hold = holds.get(payload["id"])
        if hold is not None:
            await hold.wait()
        if payload["id"] in failing:
            raise RuntimeError(f"scenario handler fails {payload['id']}")
        out.events.append(("handled", payload["id"]))

    rt: RealtimeClient | None = None
    ws: _RecordingSocket | None = None
    drains: list[asyncio.Task[None]] = []
    try:
        for step in scenario["steps"]:
            op = step["op"]
            if op == "connect":
                ws = _RecordingSocket(out.events)
                rt, _ = _make_client(
                    ws=ws,
                    client=api,  # type: ignore[arg-type]
                    auto_drain_on_connect=False,
                    ack_on_receipt=mode == "ack_on_receipt",
                )
                rt.on("message.new", handler)
                await rt.connect()
                await _idle()
                await ws.push({"type": "hello.ok", "capabilities": ["ack"]})
            elif op == "frame":
                assert ws is not None
                await ws.push({"type": "message.new", "payload": step["payload"]})
            elif op == "drain":
                assert rt is not None
                drains.append(asyncio.create_task(rt.drain_offline_envelopes()))
            elif op == "gap_window_lapses":
                # The client's gap window plus slack, in real time.
                await asyncio.sleep(_realtime._GAP_FILL_WINDOW_S + 0.15)
            elif op == "wait_handler":
                for _ in range(100):
                    if any(p["id"] == step["message_id"] for p in out.dispatched):
                        break
                    await asyncio.sleep(0.001)
                assert any(p["id"] == step["message_id"] for p in out.dispatched), (
                    f"handler entered for {step['message_id']}"
                )
            elif op == "close":
                assert ws is not None
                await ws.close(code=1006, reason="abnormal")
            elif op == "release":
                holds[step["message_id"]].set()
            else:
                raise AssertionError(f"unknown op {op}")
            await _idle()
        if drains:
            await asyncio.wait_for(asyncio.gather(*drains), timeout=2.0)
        await _idle()
        out.served = list(api.served)
        return _Outcome(list(out.dispatched), list(out.events), out.served)
    finally:
        for hold in holds.values():
            hold.set()
        if rt is not None:
            await rt.disconnect()


def _acknowledged(event: tuple[str, str], served: list[dict[str, Any]]) -> list[str]:
    """Message ids an event acknowledges: an ack frame names one; a cursor
    covers every row served so far up to and including the row carrying it."""
    kind, value = event
    if kind == "ws_ack":
        return [value]
    if kind != "cursor":
        return []
    index = next((i for i, row in enumerate(served) if row.get("delivery_id") == value), None)
    assert index is not None, f"cursor {value} names a served row"
    return [row["id"] for row in served[: index + 1]]


def _check_invariants(out: _Outcome, mode: str) -> None:
    handled_at: dict[str, int] = {}
    for at, (kind, value) in enumerate(out.events):
        if kind == "handled":
            handled_at.setdefault(value, at)

    for at, event in enumerate(out.events):
        for mid in _acknowledged(event, out.served):
            # acked_was_handled
            assert mid in handled_at, f"{mid} acknowledged but never handled"
            # acks_follow_handling
            if event[0] == "cursor" or mode == "default":
                assert handled_at[mid] < at, f"{mid} acknowledged before it was handled"

    # dispatched_once
    ids = [p["id"] for p in out.dispatched]
    assert len(set(ids)) == len(ids), f"a message reached the handler twice: {ids}"

    # seq_order
    last: dict[str, int] = {}
    for p in out.dispatched:
        previous = last.get(p["conversation_id"])
        if previous is not None:
            assert p["seq"] > previous, f"{p['conversation_id']}: seq {p['seq']} after {previous}"
        last[p["conversation_id"]] = p["seq"]


def _check_expectations(out: _Outcome, expected: dict[str, Any]) -> None:
    ids = [p["id"] for p in out.dispatched]
    if "dispatched" in expected:
        assert ids == expected["dispatched"]
    for mid in expected.get("dispatched_includes", []):
        assert mid in ids
    if "ws_acks" in expected:
        assert [v for k, v in out.events if k == "ws_ack"] == expected["ws_acks"]
    if "cursor_acks" in expected:
        assert [v for k, v in out.events if k == "cursor"] == expected["cursor_acks"]


def test_names_only_ops_expectations_and_invariants_this_harness_implements() -> None:
    assert sorted(DOC["ops"]) == OPS
    assert sorted(DOC["expectations"]) == EXPECTATIONS
    assert sorted(DOC["invariants"]) == INVARIANTS
    for scenario in DOC["scenarios"]:
        for step in scenario["steps"]:
            assert step["op"] in OPS
        assert sorted(scenario["expect"]) == sorted(MODES)
        for mode in MODES:
            for key in scenario["expect"][mode]:
                assert key in EXPECTATIONS


def test_scenario_names_are_unique() -> None:
    names = [s["name"] for s in DOC["scenarios"]]
    assert len(set(names)) == len(names)


@pytest.mark.parametrize(
    ("scenario", "mode"),
    [(s, m) for s in DOC["scenarios"] for m in MODES],
    ids=[f"{s['name']}-{m}" for s in DOC["scenarios"] for m in MODES],
)
@pytest.mark.asyncio
async def test_engine_scenario(scenario: dict[str, Any], mode: str) -> None:
    out = await _run(scenario, mode)
    _check_invariants(out, mode)
    _check_expectations(out, scenario["expect"][mode])
