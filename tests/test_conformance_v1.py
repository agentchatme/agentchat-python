"""Contract v1 conformance fixtures (``conformance/v1/``): the byte-identical
copy proof (``manifest.sha256``) and the cases the integration kit, the
connect budget, the wake webhook verifier and the v1 frame shapes must pass.
The fixtures hold the cases; this file only interprets them."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from agentchatme import (
    WakeWebhookVerificationError,
    connect_budget_delay_ms,
    gate,
    verify_wake_webhook,
)
from agentchatme._connect_budget import BudgetClose, BudgetUpgrade
from agentchatme._realtime import wake_payload_problem
from agentchatme.integration import (
    AgentChatCallCount,
    TurnEnd,
    WakeCoalescer,
    anchor,
    invite_outcome,
    turn_outcome,
)
from agentchatme.types import HeartbeatFrame, InboxTray, Texts, WakeFrame

from .test_realtime import _settle
from .test_realtime_engine import connected

FIXTURES = Path(__file__).resolve().parent.parent / "conformance" / "v1"


def raw(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def load(name: str) -> Any:
    return json.loads(raw(name))


# ─────────────── the copy is byte-identical ───────────────


def test_ships_exactly_the_files_the_manifest_names_byte_for_byte() -> None:
    lines = raw("manifest.sha256").rstrip("\n").split("\n")
    entries: list[tuple[str, str]] = []
    for line in lines:
        match = re.fullmatch(r"([0-9a-f]{64}) {2}(\S+)", line)
        assert match is not None, f"manifest line: {line}"
        entries.append((match.group(1), match.group(2)))
    names = [name for _sha, name in entries]
    files = sorted(p.name for p in FIXTURES.iterdir() if p.name != "manifest.sha256")
    assert names == files
    assert names == sorted(names)
    for sha, name in entries:
        digest = hashlib.sha256((FIXTURES / name).read_bytes()).hexdigest()
        assert digest == sha, f"{name} differs from the manifest"


@pytest.mark.parametrize(
    "name", sorted(p.name for p in FIXTURES.iterdir() if p.name.endswith(".json"))
)
def test_json_fixture_is_canonical(name: str) -> None:
    text = raw(name)
    assert text == gate.canonical_json(json.loads(text)) + "\n"


# ─────────────── texts, tray v1, frames ───────────────


def test_texts_sample_is_a_v1_set_with_the_gate_call_texts() -> None:
    texts = Texts.model_validate(load("texts.sample.json"))
    assert texts.version == "1"
    for field in (
        "wake_instruction",
        "invite_prompt",
        "identity_line",
        "identity_line_template",
        "gate_call_preamble",
        "gate_call_answer_format",
    ):
        value = getattr(texts, field)
        assert isinstance(value, str) and value
    assert texts.manual.sha256 is not None and re.fullmatch(r"[0-9a-f]{64}", texts.manual.sha256)
    assert "<handle>" in texts.identity_line_template


def test_tray_sample_with_texts_sample_renders_the_expected_prompt() -> None:
    tray = load("tray.sample.json")
    texts = load("texts.sample.json")
    expected = raw("gate-prompt.expected.txt")
    assert gate.build_prompt(tray, texts=texts) + "\n" == expected
    assert gate.build_prompt(InboxTray.model_validate(tray), texts=Texts.model_validate(texts)) + "\n" == expected
    prompt = gate.build_prompt(tray, texts=texts)
    assert prompt.startswith(f"{texts['gate_call_preamble']}\n\n{tray['question']}\n\n")
    assert prompt.endswith(f"\n\n{texts['gate_call_answer_format']}")


def test_tray_v1_sample_parses_and_gates_like_any_other() -> None:
    data = load("tray.v1.sample.json")
    tray = InboxTray.model_validate(data)
    assert tray.version == 1
    assert tray.invites and tray.invites_truncated is False
    for invite in tray.invites:
        assert invite.invite_id.startswith("ginv_")
        assert invite.invited_by.startswith("@")
    assert any(card.muted is True for card in tray.conversations)
    assert any(card.reminder is not None and card.reminder.step == 2 for card in tray.conversations)
    assert tray.model_dump(mode="json", exclude_unset=True) == data
    prompt = gate.build_prompt(data, texts=load("texts.sample.json"))
    count = len(data["conversations"])
    assert f"Your tray, as the server built it: {count} conversations." in prompt
    assert "ginv_" not in prompt


def test_frames_parse_and_validate() -> None:
    unanswered = load("wake-frame.unanswered.json")
    assert wake_payload_problem(unanswered["payload"]) is None
    frame = WakeFrame.model_validate(unanswered)
    assert frame.payload.reason == "unanswered" and frame.payload.conversation_ids
    assert wake_payload_problem(load("wake-frame.json")["payload"]) is None
    assert HeartbeatFrame.model_validate(load("heartbeat-frame.json")).payload.at


@pytest.mark.asyncio
async def test_hello_ok_v1_negotiates_every_capability() -> None:
    hello = load("hello-ok.v1.json")
    rt, _ws = await connected(capabilities=hello["capabilities"])
    try:
        assert rt.capabilities == frozenset(hello["capabilities"])
        assert rt.ack_negotiated
    finally:
        await rt.disconnect()


@pytest.mark.asyncio
async def test_the_unanswered_wake_frame_reaches_wake_handlers_as_is() -> None:
    rt, ws = await connected(capabilities=["ack", "heartbeat", "rewake"])
    seen: list[dict[str, Any]] = []
    rt.on("wake", seen.append)
    try:
        await ws.push(raw("wake-frame.unanswered.json"))
        await ws.push(raw("heartbeat-frame.json"))
        await _settle()
        assert seen == [load("wake-frame.unanswered.json")]
    finally:
        await rt.disconnect()


# ─────────────── connect budget ───────────────

BUDGET = load("connect-budget.cases.json")


def _budget_event(event: dict[str, Any]) -> BudgetClose | BudgetUpgrade:
    if event["kind"] == "close":
        return BudgetClose(code=event["code"], reason=event["reason"])
    now = event.get("now")
    now_ms = (
        int(datetime.fromisoformat(now.replace("Z", "+00:00")).timestamp() * 1000)
        if isinstance(now, str)
        else None
    )
    return BudgetUpgrade(status=event["status"], retry_after=event["retry_after"], now_ms=now_ms)


def test_connect_budget_rules_are_the_ones_this_harness_knows() -> None:
    assert sorted(BUDGET["rules"]) == ["bounds", "close_1013", "jitter", "state", "upgrade_429"]
    assert BUDGET["default_delay_ms"] == 60_000
    assert BUDGET["min_delay_ms"] == 1_000
    assert BUDGET["max_delay_ms"] == 3_600_000


@pytest.mark.parametrize("case", BUDGET["cases"], ids=[c["name"] for c in BUDGET["cases"]])
def test_connect_budget_case(case: dict[str, Any]) -> None:
    assert case["expect"]["blocked"] is False
    assert connect_budget_delay_ms(_budget_event(case["event"])) == case["expect"]["delay_ms"]


# ─────────────── wake webhook verification ───────────────

WAKE = load("wake-webhook.cases.json")


@pytest.mark.parametrize("case", WAKE["cases"], ids=[c["name"] for c in WAKE["cases"]])
def test_wake_webhook_case(case: dict[str, Any]) -> None:
    secret = case.get("secret")
    if secret is None:
        secret = (
            WAKE["standard_webhooks_secret"]
            if case["dialect"] == "standard-webhooks"
            else WAKE["secret"]
        )
    body = case.get("body", WAKE["body"])

    def verify() -> Any:
        return verify_wake_webhook(
            dialect=case["dialect"],
            body=body,
            headers=case["headers"],
            secret=secret,
            tolerance_seconds=WAKE["tolerance_s"],
            now=lambda: float(WAKE["now"]),
        )

    if case["expect"]["ok"]:
        payload = verify()
        assert payload.model_dump(mode="json", exclude_unset=True) == WAKE["payload"]
    else:
        with pytest.raises(WakeWebhookVerificationError) as err:
            verify()
        assert err.value.reason == case["expect"]["reason"]
        assert err.value.reason in WAKE["reasons"]


# ─────────────── turn outcome ───────────────

OUTCOMES = load("turn-outcome.cases.json")


def _turn(raw_turn: dict[str, Any]) -> TurnEnd:
    calls = raw_turn.get("agentchat_calls")
    return TurnEnd(
        status=raw_turn["status"],
        agentchat_calls=(
            AgentChatCallCount(made=calls["made"], failed=calls["failed"])
            if isinstance(calls, dict)
            else None
        ),
    )


def test_turn_outcome_case_names_are_unique() -> None:
    names = [c["name"] for c in OUTCOMES["cases"] + OUTCOMES["invite_cases"]]
    assert len(set(names)) == len(names)


@pytest.mark.parametrize("case", OUTCOMES["cases"], ids=[c["name"] for c in OUTCOMES["cases"]])
def test_turn_outcome_case(case: dict[str, Any]) -> None:
    given = case["input"]
    outcome = turn_outcome(
        turn=_turn(given["turn"]),
        turn_started_at=given["turn_started_at"],
        gate_card=given["gate_card"],
        category=given.get("category"),
        fresh_tray=given["fresh_tray"],
        silent_before=given.get("silent_before", 0),
    )
    expect = case["expect"]
    assert outcome.action == expect["action"]
    assert outcome.reason == expect["reason"]
    assert outcome.conversation_id == given["gate_card"]["conversation_id"]
    if expect["action"] != "none":
        assert outcome.through_message_id == expect["through_message_id"]
        assert outcome.category == expect["category"]


@pytest.mark.parametrize(
    "case", OUTCOMES["invite_cases"], ids=[c["name"] for c in OUTCOMES["invite_cases"]]
)
def test_invite_outcome_case(case: dict[str, Any]) -> None:
    given = case["input"]
    outcome = invite_outcome(
        turn=_turn(given["turn"]),
        invite=given["invite"],
        fresh_tray=given["fresh_tray"],
        silent_before=given.get("silent_before", 0),
    )
    assert outcome.action == case["expect"]["action"]
    assert outcome.reason == case["expect"]["reason"]
    assert outcome.invite_id == case["expect"].get("invite_id", given["invite"]["invite_id"])


# ─────────────── anchor ───────────────

ANCHOR = load("anchor.cases.json")


def test_anchor_markers_are_the_kits() -> None:
    assert ANCHOR["markers"] == {
        "end": anchor.END_MARKER,
        "handle_line": anchor.handle_line("<handle>"),
        "legacy_end": anchor.LEGACY_END_MARKER,
        "legacy_start": anchor.LEGACY_START_MARKER,
        "start": anchor.START_MARKER,
    }
    assert sorted(ANCHOR["rules"]) == ["find", "remove", "render", "replace"]


@pytest.mark.parametrize("case", ANCHOR["render"], ids=[c["name"] for c in ANCHOR["render"]])
def test_anchor_render_case(case: dict[str, Any]) -> None:
    if case.get("error"):
        with pytest.raises(ValueError):
            anchor.render(case["texts"], case["handle"])
        return
    assert anchor.render(case["texts"], case["handle"]) == case["expect"]


@pytest.mark.parametrize("case", ANCHOR["find"], ids=[c["name"] for c in ANCHOR["find"]])
def test_anchor_find_case(case: dict[str, Any]) -> None:
    found = anchor.find(case["text"], template=case.get("template"))
    if case["expect"] is None:
        assert found is None
        return
    assert found is not None
    got = {"body": found.body, "handle": found.handle, "legacy": found.legacy, "marked": found.marked}
    assert got == {key: case["expect"][key] for key in got}


@pytest.mark.parametrize("case", ANCHOR["replace"], ids=[c["name"] for c in ANCHOR["replace"]])
def test_anchor_replace_case(case: dict[str, Any]) -> None:
    out = anchor.replace(case["text"], case["block"])
    assert out == case["expect"]
    assert anchor.replace(out, case["block"]) == out  # idempotent


@pytest.mark.parametrize("case", ANCHOR["remove"], ids=[c["name"] for c in ANCHOR["remove"]])
def test_anchor_remove_case(case: dict[str, Any]) -> None:
    assert anchor.remove(case["text"]) == case["expect"]


# ─────────────── coalescer ───────────────

COALESCER = load("coalescer.cases.json")


class _Clock:
    """Timers and cycle ends as entries in one queue, applied in time order
    with the fixture's rule for an instant: first what is due by then, then
    the events at it, then timers they armed for it."""

    def __init__(self) -> None:
        self.now = 0.0
        self._next = 0
        self.queue: list[tuple[float, int, Any]] = []

    def schedule(self, delay_ms: float, fn: Any) -> Any:
        self._next += 1
        entry = (self.now + max(0.0, delay_ms), self._next, fn)
        self.queue.append(entry)
        clock = self

        class _Handle:
            def cancel(self) -> None:
                if entry in clock.queue:
                    clock.queue.remove(entry)

        return _Handle()

    async def run_due(self, t: float) -> None:
        while True:
            self.queue.sort(key=lambda e: (e[0], e[1]))
            if not self.queue or self.queue[0][0] > t:
                return
            at, _id, fn = self.queue.pop(0)
            self.now = at
            fn()
            for _ in range(5):
                await asyncio.sleep(0)


async def _simulate(case: dict[str, Any]) -> list[float]:
    clock = _Clock()
    starts: list[float] = []
    durations = case["cycle_ms"] if isinstance(case["cycle_ms"], list) else [case["cycle_ms"]]
    loop = asyncio.get_running_loop()

    def on_cycle() -> asyncio.Future[None]:
        index = len(starts)
        starts.append(clock.now)
        done: asyncio.Future[None] = loop.create_future()
        clock.schedule(durations[min(index, len(durations) - 1)], lambda: done.set_result(None))
        return done

    coalescer = WakeCoalescer(
        on_cycle,
        quiet_ms=case["quiet_ms"],
        max_ms=case["max_ms"],
        now=lambda: clock.now,
        set_timer=clock.schedule,
    )
    for event in sorted(case["events"], key=lambda e: e["at"]):
        await clock.run_due(event["at"])
        clock.now = event["at"]
        if event["op"] == "wake":
            coalescer.wake()
        elif event["op"] == "stop":
            coalescer.stop()
        else:
            raise AssertionError(f"unknown op {event['op']}")
        for _ in range(5):
            await asyncio.sleep(0)
        await clock.run_due(event["at"])
    await clock.run_due(case["run_until"])
    return starts


def test_coalescer_rules_are_the_ones_this_harness_knows() -> None:
    assert sorted(COALESCER["rules"]) == ["during_cycle", "stop", "validation", "window"]
    names = [c["name"] for c in COALESCER["cases"]]
    assert len(set(names)) == len(names)


@pytest.mark.parametrize("case", COALESCER["cases"], ids=[c["name"] for c in COALESCER["cases"]])
@pytest.mark.asyncio
async def test_coalescer_case(case: dict[str, Any]) -> None:
    assert await _simulate(case) == case["expect_cycles"]


def test_coalescer_validation() -> None:
    with pytest.raises(ValueError):
        WakeCoalescer(lambda: None, quiet_ms=2000, max_ms=1000)
    with pytest.raises(ValueError):
        WakeCoalescer(lambda: None, quiet_ms=-1)
