"""The shared conformance fixtures every AgentChat SDK must pass.

``conformance/v1/`` holds the cases, byte for byte identical across the
TypeScript and Python repos. Tests load the files; nothing here restates
their content.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from agentchatme import RealtimeClient, gate
from agentchatme.types import AckFrame, InboxTray, WakeFrame

from ._texts import GATE_TEXTS
from .test_realtime import MockWebSocket, _ack_frames, _make_client, _settle

FIXTURES = Path(__file__).resolve().parent.parent / "conformance" / "v1"
def raw(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def load(name: str) -> Any:
    return json.loads(raw(name))


@pytest.fixture
def tray() -> dict[str, Any]:
    tray: dict[str, Any] = load("tray.sample.json")
    return tray


# ─────────────── Canonical form ───────────────


def test_expected_prompt_ends_with_exactly_one_newline() -> None:
    text = raw("gate-prompt.expected.txt")
    assert text.endswith("\n")
    assert not text.endswith("\n\n")


# ─────────────── tray.sample.json ───────────────


def test_tray_covers_a_direct_card_a_group_card_a_mention_and_a_deleted_group(
    tray: dict[str, Any],
) -> None:
    cards = tray["conversations"]
    assert any(c["type"] == "direct" for c in cards)
    assert any(c["type"] == "group" and c["group"]["deleted"] is None for c in cards)
    assert any(c["mentioned_you"] for c in cards)
    assert any(c["group"] is not None and c["group"]["deleted"] is not None for c in cards)
    # Every card carries the anchor a verdict reads through.
    for card in cards:
        assert card["preview"]["message_id"].startswith("msg_")


def test_tray_parses_into_the_typed_model_and_round_trips(tray: dict[str, Any]) -> None:
    model = InboxTray.model_validate(tray)
    assert [c.conversation_id for c in model.conversations] == [
        c["conversation_id"] for c in tray["conversations"]
    ]
    assert [c.anchor_message_id for c in model.conversations] == [
        c["preview"]["message_id"] for c in tray["conversations"]
    ]
    # Exactly the keys the server sent, nothing added, nothing lost.
    assert model.model_dump(mode="json", exclude_unset=True) == tray


# ─────────────── gate prompt ───────────────


def test_build_prompt_renders_the_sample_tray_to_the_expected_text(tray: dict[str, Any]) -> None:
    expected = raw("gate-prompt.expected.txt")
    assert gate.build_prompt(tray, texts=GATE_TEXTS) + "\n" == expected
    # The typed model renders the same bytes as the raw JSON.
    assert gate.build_prompt(InboxTray.model_validate(tray), texts=GATE_TEXTS) + "\n" == expected


def test_prompt_carries_the_server_question_verbatim_after_the_preamble(tray: dict[str, Any]) -> None:
    prompt = gate.build_prompt(tray, texts=GATE_TEXTS)
    assert prompt.startswith(GATE_TEXTS["gate_call_preamble"] + "\n\n")
    assert "\n\n" + tray["question"] + "\n\n" in prompt


# ─────────────── verdict parsing ───────────────

CASES: list[dict[str, Any]] = load("verdict-parse.cases.json")


def test_verdict_cases_have_unique_names() -> None:
    assert len({c["name"] for c in CASES}) == len(CASES)


@pytest.mark.parametrize("case", CASES, ids=[c["name"] for c in CASES])
def test_verdict_case(case: dict[str, Any], tray: dict[str, Any]) -> None:
    parsed = gate.parse_verdicts(case["text"], tray)
    got = (
        None
        if parsed is None
        else [
            {"conversation_id": v.conversation_id, "decision": v.decision, "category": v.category}
            for v in parsed
        ]
    )
    assert got == case["expected"]


# ─────────────── socket frames ───────────────


async def _connected(**opts: Any) -> tuple[RealtimeClient, MockWebSocket]:
    rt, ws = _make_client(**opts)
    await rt.connect()
    await _settle()
    await ws.push({"type": "hello.ok", "capabilities": ["ack"]})
    await _settle()
    return rt, ws


@pytest.mark.asyncio
async def test_wake_frame_reaches_wake_handlers_as_is_and_is_never_acked() -> None:
    frame = load("wake-frame.json")
    WakeFrame.model_validate(frame)  # the fixture is a valid typed frame
    rt, ws = await _connected()
    seen: list[dict[str, Any]] = []
    rt.on("wake", lambda m: seen.append(m))
    try:
        await ws.push(raw("wake-frame.json"))
        await _settle()
        assert seen == [frame]
        assert _ack_frames(ws) == []
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_ack_frame_is_what_the_client_sends_for_a_delivered_message() -> None:
    ack = load("ack-frame.json")
    AckFrame.model_validate(ack)
    rt, ws = await _connected(ack_on_receipt=True)
    rt.on("message.new", lambda m: None)
    try:
        await ws.push(
            {
                "type": "message.new",
                "payload": {
                    "id": ack["message_id"],
                    "conversation_id": "conv_direct0first01",
                    "seq": 1,
                },
            }
        )
        await _settle()
        sent = [json.loads(s) for s in ws.sent]
        assert ack in sent
        assert _ack_frames(ws) == [ack["message_id"]]
    finally:
        await rt.disconnect()
