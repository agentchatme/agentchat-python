"""Tests for :mod:`agentchatme.gate` beyond the shared conformance cases."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from agentchatme import GateResult, gate
from agentchatme.types import InboxCard, InboxTray, ReplyDecision, Texts

from ._texts import GATE_CALL_ANSWER_FORMAT, GATE_CALL_PREAMBLE, GATE_TEXTS, texts

FIXTURES = Path(__file__).resolve().parent.parent / "conformance" / "v1"

IDS = {
    "direct": "conv_direct0first01",
    "crew": "conv_group0crew0001",
    "ops": "conv_group0ops00001",
    "gone": "conv_group0gone0001",
}


def sample_tray() -> dict[str, Any]:
    tray: dict[str, Any] = json.loads((FIXTURES / "tray.sample.json").read_text(encoding="utf-8"))
    return tray


def answer(entries: list[dict[str, Any]]) -> str:
    return json.dumps({"decisions": entries})


def _decision(conversation_id: str, **kwargs: Any) -> ReplyDecision:
    return ReplyDecision.model_validate(
        {
            "id": "dec_1",
            "conversation_id": conversation_id,
            "decision": kwargs.get("decision", "dismiss"),
            "category": kwargs.get("category"),
            "through_message_id": kwargs.get("through_message_id"),
            "defer_until": None,
            "created_at": "2026-11-04T23:00:00.000Z",
        }
    )


class FakeDecisions:
    """Stands in for ``client.decisions``; records every call."""

    def __init__(self, fail_for: set[str] | None = None) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self._fail_for = fail_for or set()

    def record(self, conversation_id: str, **kwargs: Any) -> ReplyDecision:
        self.calls.append((conversation_id, kwargs))
        if conversation_id in self._fail_for:
            raise RuntimeError("503 from server")
        return _decision(conversation_id, **kwargs)


class FakeAsyncDecisions(FakeDecisions):
    async def record(self, conversation_id: str, **kwargs: Any) -> ReplyDecision:  # type: ignore[override]
        return super().record(conversation_id, **kwargs)


class FakeClient:
    def __init__(self, decisions: FakeDecisions) -> None:
        self.decisions = decisions


# ─────────────── build_prompt ───────────────


def test_prompt_opens_with_the_served_preamble_then_the_served_question() -> None:
    tray = sample_tray()
    prompt = gate.build_prompt(tray, texts=GATE_TEXTS)
    assert prompt.startswith(GATE_CALL_PREAMBLE + "\n\n" + tray["question"] + "\n\n")
    assert prompt.endswith("\n\n" + GATE_CALL_ANSWER_FORMAT)
    # The served preamble comes spelled for the runtime already: verbatim.
    named = gate.build_prompt(tray, tool_namer=gate.mcp_tool_namer("ac"), texts=GATE_TEXTS)
    assert named.startswith(GATE_CALL_PREAMBLE + "\n\n")


def test_tool_namer_respells_hosted_tool_names_and_nothing_else() -> None:
    tray = sample_tray()
    named = gate.build_prompt(tray, tool_namer=gate.mcp_tool_namer("agentchat"), texts=GATE_TEXTS)
    assert "mcp__agentchat__agentchat_decide" in named
    # Only the tray's question is re-spelled; the served texts and the card
    # JSON are untouched.
    assert named.count("mcp__agentchat__") == tray["question"].count("agentchat_decide")
    assert gate.build_prompt(tray, tool_namer=gate.bare_tool_namer, texts=GATE_TEXTS) == (
        gate.build_prompt(tray, texts=GATE_TEXTS)
    )


def test_names_the_server_already_spelled_for_the_runtime_are_left_alone() -> None:
    spelled = texts(
        gate_call_preamble=GATE_CALL_PREAMBLE.replace(
            "agentchat_decide", "mcp__agentchat__agentchat_decide"
        )
    )
    prompt = gate.build_prompt(
        sample_tray(), tool_namer=gate.mcp_tool_namer("agentchat"), texts=spelled
    )
    assert "do not call mcp__agentchat__agentchat_decide," in prompt
    assert "mcp__agentchat__mcp__agentchat__" not in prompt


def test_build_prompt_raises_without_a_question() -> None:
    tray = sample_tray()
    tray["question"] = "   "
    with pytest.raises(ValueError, match="no question"):
        gate.build_prompt(tray, texts=GATE_TEXTS)


@pytest.mark.parametrize(
    "served",
    [
        None,
        texts(gate_call_preamble=None),
        texts(gate_call_answer_format="  "),
        {k: v for k, v in GATE_TEXTS.items() if k != "gate_call_preamble"},
    ],
)
def test_without_the_served_gate_call_texts_the_legacy_wording_is_kept(served: Any) -> None:
    # A host written before contract v1 calls the gate without texts, and a
    # server before contract v1 serves texts without the gate-call fields:
    # both gate exactly as before the upgrade.
    tray = sample_tray()
    legacy = gate.build_prompt(tray, texts=served)
    assert legacy == gate.build_prompt(tray)
    assert legacy.startswith(gate.LEGACY_GATE_CALL_PREAMBLE + "\n\n" + tray["question"])
    named = gate.build_prompt(tray, tool_namer=gate.mcp_tool_namer("ac"), texts=served)
    assert named.startswith(gate.prompt_preamble(gate.mcp_tool_namer("ac")))
    assert "do not call mcp__ac__agentchat_decide," in named


def test_the_legacy_answer_format_follows_the_trays_categories() -> None:
    tray = sample_tray()
    tray["categories"] = []
    prompt = gate.build_prompt(tray)
    assert '"category": "<one of' not in prompt
    assert prompt.endswith(
        '"decision": "reply" | "dismiss" | "defer"}]} — one entry per card, every card decided.'
    )


def test_run_without_texts_gates_as_before() -> None:
    prompts: list[str] = []
    gate.run(
        sample_tray(),
        lambda p: prompts.append(p) or "",
        FakeClient(FakeDecisions()),  # type: ignore[arg-type]
    )
    assert prompts == [gate.build_prompt(sample_tray())]


def test_build_prompt_takes_typed_texts() -> None:
    typed = Texts.model_validate(GATE_TEXTS)
    assert gate.build_prompt(sample_tray(), texts=typed) == gate.build_prompt(
        sample_tray(), texts=GATE_TEXTS
    )


def test_build_prompt_omits_the_categories_line_when_the_server_serves_none() -> None:
    tray = sample_tray()
    tray["categories"] = []
    prompt = gate.build_prompt(tray, texts=GATE_TEXTS)
    assert "Categories:" not in prompt


def test_build_prompt_sorts_card_keys_regardless_of_server_order() -> None:
    tray = sample_tray()
    shuffled = dict(reversed(list(tray["conversations"][0].items())))
    tray["conversations"][0] = shuffled
    assert gate.build_prompt(tray, texts=GATE_TEXTS) == gate.build_prompt(sample_tray(), texts=GATE_TEXTS)


def test_build_prompt_counts_cards_in_the_singular() -> None:
    tray = sample_tray()
    tray["conversations"] = tray["conversations"][:1]
    assert "Your tray, as the server built it: 1 conversation." in gate.build_prompt(tray, texts=GATE_TEXTS)
    assert "Card 1 of 1" in gate.build_prompt(tray, texts=GATE_TEXTS)


def test_build_prompt_rejects_other_tray_types() -> None:
    with pytest.raises(TypeError):
        gate.build_prompt("not a tray", texts=GATE_TEXTS)  # type: ignore[arg-type]


# ─────────────── run ───────────────


def test_run_records_dismiss_and_defer_at_once_and_hands_reply_back_with_a_record() -> None:
    tray = sample_tray()
    decisions = FakeDecisions()
    prompts: list[str] = []

    def call_model(prompt: str) -> str:
        prompts.append(prompt)
        return answer(
            [
                {"conversation_id": IDS["direct"], "decision": "reply", "category": "open_request"},
                {"conversation_id": IDS["crew"], "decision": "dismiss", "category": "acknowledgement"},
                {"conversation_id": IDS["ops"], "decision": "defer", "category": "goal_followup"},
                {"conversation_id": IDS["gone"], "decision": "dismiss", "category": "no_action_needed"},
            ]
        )

    result = gate.run(tray, call_model, FakeClient(decisions), texts=GATE_TEXTS)  # type: ignore[arg-type]

    assert prompts == [gate.build_prompt(tray, texts=GATE_TEXTS)]
    assert isinstance(result, GateResult)
    assert result.outcome == "decided"
    assert result.undecided == []
    assert result.failure is None
    assert [v.decision for v in result.verdicts] == ["reply", "dismiss", "defer", "dismiss"]
    # dismiss/defer went to the server at once, in answer order, anchored.
    assert result.recorded == 3
    assert result.record_failures == []
    assert decisions.calls == [
        (IDS["crew"], {"decision": "dismiss", "category": "acknowledgement", "through_message_id": "msg_crew0thanks0001b"}),
        (IDS["ops"], {"decision": "defer", "category": "goal_followup", "through_message_id": "msg_ops0mention001c"}),
        (IDS["gone"], {"decision": "dismiss", "category": "no_action_needed", "through_message_id": "msg_gone0deleted01d"}),
    ]
    # reply was NOT recorded: the host starts the turn, then records.
    assert [entry.conversation_id for entry in result.reply] == [IDS["direct"]]
    entry = result.reply[0]
    assert isinstance(entry, gate.GateReply)
    assert entry.category == "open_request"
    assert entry.anchor_message_id == "msg_nova0first00001a"
    assert entry.card["conversation_id"] == IDS["direct"]  # type: ignore[index]

    recorded = entry.record()
    assert recorded.conversation_id == IDS["direct"]
    assert decisions.calls[-1] == (
        IDS["direct"],
        {"decision": "reply", "category": "open_request", "through_message_id": "msg_nova0first00001a"},
    )


def test_reply_record_raises_what_the_client_raises_and_may_be_retried() -> None:
    decisions = FakeDecisions(fail_for={IDS["direct"]})
    result = gate.run(
        sample_tray(),
        lambda _p: answer([{"conversation_id": IDS["direct"], "decision": "reply"}]),
        FakeClient(decisions),  # type: ignore[arg-type]
        texts=GATE_TEXTS,
    )
    assert result.outcome == "decided"
    assert result.recorded == 0 and result.record_failures == []
    assert decisions.calls == []  # nothing recorded up front
    with pytest.raises(RuntimeError, match="503"):
        result.reply[0].record()
    assert len(decisions.calls) == 1
    # A failed call is not memoised: the host may retry.
    decisions._fail_for.clear()
    recorded = result.reply[0].record()
    assert recorded.conversation_id == IDS["direct"]
    assert len(decisions.calls) == 2
    # A successful call is: the second record() returns the same decision.
    assert result.reply[0].record() is recorded
    assert len(decisions.calls) == 2


def test_anchor_of_prefers_the_preview_then_last_message_id() -> None:
    card = dict(sample_tray()["conversations"][0])
    assert gate.anchor_of(card) == "msg_nova0first00001a"
    card["preview"] = None
    assert gate.anchor_of(card) is None
    card["last_message_id"] = "msg_last0001"
    assert gate.anchor_of(card) == "msg_last0001"
    assert gate.anchor_of(InboxCard.model_validate(card)) == "msg_last0001"


def test_reply_anchor_falls_back_to_last_message_id_when_the_preview_is_null() -> None:
    tray = sample_tray()
    tray["conversations"][0]["preview"] = None
    tray["conversations"][0]["last_message_id"] = "msg_fallback0001"
    tray["conversations"][1]["preview"] = None  # no fallback field: unanchored
    decisions = FakeDecisions()
    result = gate.run(
        tray,
        lambda _p: answer(
            [
                {"conversation_id": IDS["direct"], "decision": "reply"},
                {"conversation_id": IDS["crew"], "decision": "dismiss", "category": "urgent"},
            ]
        ),
        FakeClient(decisions),  # type: ignore[arg-type]
        texts=GATE_TEXTS,
    )
    assert result.reply[0].anchor_message_id == "msg_fallback0001"
    result.reply[0].record()
    assert decisions.calls == [
        (IDS["crew"], {"decision": "dismiss", "category": None, "through_message_id": None}),
        (IDS["direct"], {"decision": "reply", "category": None, "through_message_id": "msg_fallback0001"}),
    ]


def test_run_passes_the_namer_through_to_the_prompt() -> None:
    prompts: list[str] = []

    def call_model(prompt: str) -> str:
        prompts.append(prompt)
        return answer([{"conversation_id": IDS["crew"], "decision": "dismiss"}])

    gate.run(
        sample_tray(),
        call_model,
        FakeClient(FakeDecisions()),  # type: ignore[arg-type]
        tool_namer=gate.mcp_tool_namer("ac"),
        texts=GATE_TEXTS,
    )
    assert "mcp__ac__agentchat_decide" in prompts[0]


def test_run_records_nothing_and_fails_closed_when_the_model_call_raises() -> None:
    decisions = FakeDecisions()

    def call_model(_prompt: str) -> str:
        raise RuntimeError("provider down")

    result = gate.run(sample_tray(), call_model, FakeClient(decisions), texts=GATE_TEXTS)  # type: ignore[arg-type]
    assert result.outcome == "fail_closed"
    assert result.failure is not None and "provider down" in result.failure
    assert result.reply == []
    assert result.undecided == list(IDS.values())
    assert decisions.calls == []


@pytest.mark.parametrize(
    "text",
    ["", "Reply to Nova, dismiss the rest.", answer([{"conversation_id": IDS["direct"], "decision": "maybe"}])],
)
def test_run_records_nothing_and_fails_closed_without_a_usable_verdict(text: str) -> None:
    decisions = FakeDecisions()
    result = gate.run(sample_tray(), lambda _p: text, FakeClient(decisions), texts=GATE_TEXTS)  # type: ignore[arg-type]
    assert result.outcome == "fail_closed"
    assert result.failure == "no usable verdicts in the answer"
    assert result.verdicts == []
    assert decisions.calls == []


def test_run_fails_closed_without_a_model_call_when_the_tray_has_no_question() -> None:
    tray = sample_tray()
    tray["question"] = ""
    decisions = FakeDecisions()
    calls: list[str] = []
    result = gate.run(tray, lambda p: calls.append(p) or "", FakeClient(decisions), texts=GATE_TEXTS)  # type: ignore[arg-type]
    assert result.outcome == "fail_closed"
    assert calls == []
    assert decisions.calls == []


def test_run_returns_empty_for_a_tray_without_cards_and_never_calls_the_model() -> None:
    tray = sample_tray()
    tray["conversations"] = []
    decisions = FakeDecisions()
    calls: list[str] = []
    result = gate.run(tray, lambda p: calls.append(p) or "", FakeClient(decisions), texts=GATE_TEXTS)  # type: ignore[arg-type]
    assert result == GateResult(outcome="empty")
    assert calls == []
    assert decisions.calls == []


def test_run_lists_skipped_cards_as_undecided_and_still_records_the_rest() -> None:
    decisions = FakeDecisions()
    result = gate.run(
        sample_tray(),
        lambda _p: answer([{"conversation_id": IDS["crew"], "decision": "dismiss", "category": "closing"}]),
        FakeClient(decisions),  # type: ignore[arg-type]
        texts=GATE_TEXTS,
    )
    assert result.outcome == "decided"
    assert result.undecided == [IDS["direct"], IDS["ops"], IDS["gone"]]
    assert len(decisions.calls) == 1


def test_run_reports_an_immediate_record_failure_and_keeps_going() -> None:
    decisions = FakeDecisions(fail_for={IDS["crew"]})
    result = gate.run(
        sample_tray(),
        lambda _p: answer(
            [
                {"conversation_id": IDS["crew"], "decision": "dismiss"},
                {"conversation_id": IDS["gone"], "decision": "dismiss"},
            ]
        ),
        FakeClient(decisions),  # type: ignore[arg-type]
        texts=GATE_TEXTS,
    )
    assert result.outcome == "decided"
    assert result.recorded == 1
    assert [f.conversation_id for f in result.record_failures] == [IDS["crew"]]
    assert "503" in str(result.record_failures[0].error)
    assert len(decisions.calls) == 2


def test_run_with_a_typed_tray_hands_back_typed_cards() -> None:
    decisions = FakeDecisions()
    result = gate.run(
        InboxTray.model_validate(sample_tray()),
        lambda _p: answer([{"conversation_id": IDS["direct"], "decision": "reply"}]),
        FakeClient(decisions),  # type: ignore[arg-type]
        texts=GATE_TEXTS,
    )
    entry = result.reply[0]
    assert isinstance(entry.card, InboxCard)
    assert entry.card.anchor_message_id == entry.anchor_message_id == "msg_nova0first00001a"
    entry.record()
    assert decisions.calls[0][1]["through_message_id"] == "msg_nova0first00001a"


# ─────────────── run_async ───────────────


@pytest.mark.asyncio
async def test_run_async_awaits_an_async_model_and_an_async_client() -> None:
    decisions = FakeAsyncDecisions()

    async def call_model(_prompt: str) -> str:
        return answer(
            [
                {"conversation_id": IDS["direct"], "decision": "reply", "category": "open_request"},
                {"conversation_id": IDS["gone"], "decision": "dismiss"},
            ]
        )

    result = await gate.run_async(sample_tray(), call_model, FakeClient(decisions), texts=GATE_TEXTS)  # type: ignore[arg-type]
    assert result.outcome == "decided"
    assert result.recorded == 1
    assert [c[0] for c in decisions.calls] == [IDS["gone"]]
    entry = result.reply[0]
    assert isinstance(entry, gate.AsyncGateReply)
    recorded = await entry.record()
    assert recorded.conversation_id == IDS["direct"]
    assert decisions.calls[-1] == (
        IDS["direct"],
        {"decision": "reply", "category": "open_request", "through_message_id": "msg_nova0first00001a"},
    )


@pytest.mark.asyncio
async def test_run_async_accepts_a_sync_model_call_and_fails_closed_on_raise() -> None:
    decisions = FakeAsyncDecisions()
    ok = await gate.run_async(
        sample_tray(),
        lambda _p: answer([{"conversation_id": IDS["crew"], "decision": "no"}]),
        FakeClient(decisions),  # type: ignore[arg-type]
        texts=GATE_TEXTS,
    )
    assert ok.verdicts[0].decision == "dismiss"

    async def boom(_prompt: str) -> str:
        raise RuntimeError("provider down")

    failed = await gate.run_async(sample_tray(), boom, FakeClient(decisions), texts=GATE_TEXTS)  # type: ignore[arg-type]
    assert failed.outcome == "fail_closed"
    assert failed.failure is not None and "provider down" in failed.failure
    assert len(decisions.calls) == 1
