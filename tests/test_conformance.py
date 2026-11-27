"""The shared conformance fixtures every AgentChat SDK must pass.

``conformance/v1/`` holds the cases, byte for byte identical across the
TypeScript and Python repos. Tests load the files; nothing here restates
their content.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from agentchatme import AgentChatClient, RealtimeClient, gate
from agentchatme.types import AckFrame, Agent, InboxTray, Reachability, TrayNotice, WakeFrame

from ._texts import GATE_TEXTS
from .test_realtime import MockWebSocket, _ack_frames, _flush, _make_client

FIXTURES = Path(__file__).resolve().parent.parent / "conformance" / "v1"
def raw(name: str) -> str:
    # Bytes, decoded: no newline translation, so the text is the file's.
    return (FIXTURES / name).read_bytes().decode("utf-8")


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
    # The legacy path (no served texts: a host written before contract v1)
    # renders the same bytes.
    assert gate.build_prompt(tray) + "\n" == expected


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
    # HELLO offers ack only while a message.new handler is attached.
    rt.on("message.new", lambda _m: None)
    await rt.connect()
    await _flush()
    await ws.push({"type": "hello.ok", "capabilities": ["ack"]})
    await _flush()
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
        await _flush()
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
        await _flush()
        sent = [json.loads(s) for s in ws.sent]
        assert ack in sent
        assert _ack_frames(ws) == [ack["message_id"]]
    finally:
        await rt.disconnect()


# ─────────────── reachability.v1.json ───────────────

_KNOWN_STATES = {"ready", "untested", "failing", "offline", "quiet", "when_run", "paused", "none"}
_KNOWN_VIAS = {"socket", "webhook", "wake_check", None}
_KNOWN_PROBLEMS = {"wake_failing", "woken_undelivered", "listener_not_ready", "listener_runs_failing"}
_REACHABILITY = load("reachability.v1.json")
_REACHABILITY_EXAMPLES = [
    (f"{group}.{name}", example)
    for group in ("agent", "connector", "integration", "owner")
    for name, example in _REACHABILITY[group].items()
]
_API = "https://api.test"


def test_reachability_is_version_1_with_an_example_of_every_state() -> None:
    assert _REACHABILITY["version"] == 1
    assert {example["state"] for _, example in _REACHABILITY_EXAMPLES} >= _KNOWN_STATES


@pytest.mark.parametrize(
    "example", [e for _, e in _REACHABILITY_EXAMPLES], ids=[n for n, _ in _REACHABILITY_EXAMPLES]
)
def test_every_reachability_example_parses_with_a_text_to_show(example: dict[str, Any]) -> None:
    model = Reachability.model_validate(example)
    assert model.version == 1
    assert model.state in _KNOWN_STATES
    assert model.via in _KNOWN_VIAS
    assert isinstance(model.text, str) and model.text
    if model.problem is not None:
        assert model.problem.kind in _KNOWN_PROBLEMS
        assert isinstance(model.problem.text, str) and model.problem.text
    # Exactly the keys the server sent, unknown ones (owner_text) included.
    assert model.model_dump(mode="json", exclude_unset=True) == example
    assert Agent.model_validate(_agent_with(example)).reachability == model


@pytest.mark.parametrize(
    "example", [e for _, e in _REACHABILITY_EXAMPLES], ids=[n for n, _ in _REACHABILITY_EXAMPLES]
)
def test_every_reachability_example_reaches_the_caller_as_sent(example: dict[str, Any]) -> None:
    done = {"agent": {"handle": "example-relay"}, "api_key": "ac_example-not-a-real-key", "next_step": "done", "reachability": example}
    with respx.mock:
        respx.get(f"{_API}/v1/agents/me").mock(return_value=httpx.Response(200, json=_agent_with(example)))
        respx.post(f"{_API}/v1/register/verify").mock(return_value=httpx.Response(201, json=done))
        assert AgentChatClient(api_key="ac_test", base_url=_API).get_me()["reachability"] == example
        result = AgentChatClient.verify("pnd_1", "123456", base_url=_API)
    assert result.reachability is not None
    assert result.reachability.model_dump(mode="json", exclude_unset=True) == example


def test_a_reachability_value_or_field_this_sdk_does_not_know_is_kept() -> None:
    base = _REACHABILITY["agent"]["failing_listener_not_ready"]
    later = {
        **base,
        "state": "later_state",
        "via": "later_via",
        "problem": {**base["problem"], "kind": "later_kind"},
        "later_field": 1,
    }
    model = Reachability.model_validate(later)
    assert (model.state, model.via, model.text) == ("later_state", "later_via", base["text"])
    assert model.model_dump(mode="json", exclude_unset=True) == later


def _agent_with(reachability: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": "agt_1",
        "handle": "example-relay",
        "email": "r***@example.com",
        "status": "active",
        "paused_by_owner": "none",
        "settings": {"inbox_mode": "open", "group_invite_policy": "open", "wake_mode": "wake"},
        "created_at": "2026-11-28T09:30:00.000Z",
        "updated_at": "2026-11-28T09:30:00.000Z",
        "reachability": reachability,
    }


# ─────────────── the notice fixtures ───────────────

_NOTICE_FILES = [
    "notice.wake-failing.v1.json",
    "notice.no-wake.v1.json",
    "notice.woken-undelivered.v1.json",
    "notice.no-wake-yet.v1.json",
    "notice.socket-offline.v1.json",
]
_NOTICES = [
    (f"{name} notices.{key}", notice)
    for name in _NOTICE_FILES
    for key, notice in load(name)["notices"].items()
] + [
    (f"{name} tray_example.notices[{i}]", notice)
    for name in _NOTICE_FILES
    for i, notice in enumerate(load(name)["tray_example"]["notices"])
]


@pytest.mark.parametrize("name", _NOTICE_FILES)
def test_every_notice_file_is_version_1_with_a_tray_example(name: str) -> None:
    file = load(name)
    assert file["version"] == 1
    assert file["tray_example"]["notices"]


@pytest.mark.parametrize("notice", [n for _, n in _NOTICES], ids=[i for i, _ in _NOTICES])
def test_every_notice_example_parses_with_a_text_to_show(notice: dict[str, Any]) -> None:
    model = TrayNotice.model_validate(notice)
    assert isinstance(model.kind, str) and model.kind
    assert isinstance(model.text, str) and model.text
    assert model.model_dump(mode="json", exclude_unset=True) == notice


def test_the_notices_name_the_two_new_kinds() -> None:
    kinds = {notice["kind"] for _, notice in _NOTICES}
    assert {"no_wake_yet", "socket_offline"} <= kinds


def test_a_tray_with_notices_reads_as_sent_and_its_notices_are_never_cards() -> None:
    v1 = load("tray.v1.sample.json")
    later = {"kind": "later_kind", "text": "A notice of a kind this SDK does not know.", "later_field": True}
    notices = [notice for _, notice in _NOTICES] + [later]
    model = InboxTray.model_validate({**v1, "notices": notices})
    assert model.notices is not None
    assert [n.model_dump(mode="json", exclude_unset=True) for n in model.notices] == notices
    assert [n.text for n in model.notices] == [n["text"] for n in notices]
    assert gate.build_prompt(model, texts=GATE_TEXTS) == gate.build_prompt(v1, texts=GATE_TEXTS)
