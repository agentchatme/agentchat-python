"""ReceiveLoop, gate-call mode: the tray gated in chunks (contract v1, section
3.6) and the reply-turn prompt (texts ``reply_turn_template``)."""

from __future__ import annotations

import asyncio
import json
import re
import threading
from pathlib import Path
from typing import Any

import pytest

from agentchatme import gate
from agentchatme.integration import (
    AgentChatCallCount,
    ReceiveLoop,
    TextsCache,
    TrackedTurn,
    TurnAdmission,
    TurnEnd,
    TurnRequest,
    TurnTracker,
    reply_prompt,
)
from agentchatme.types import Texts

from ._texts import texts as texts_body
from .test_integration_kit import (
    _AsyncTextsClient,
    _card,
    _cycle,
    _eventually,
    _loop,
    _LoopClient,
    _tray,
    _Turns,
)

FIXTURES = Path(__file__).resolve().parent.parent / "conformance" / "v1"
TEMPLATE = (
    "Reply turn for ONE conversation, <conversation_id>: open it with agentchat_get_conversation."
    " Leave every other conversation alone (not <conversation_id>'s neighbours)."
)
_ID = re.compile(r'"conversation_id": "(c\d+)"')


def _cards(count: int) -> list[dict[str, Any]]:
    return [_card(f"c{i:02d}", f"m{i:02d}") for i in range(1, count + 1)]


def _ids(prompt: str) -> list[str]:
    """The cards a gate prompt asks about, in its order."""
    return _ID.findall(prompt)


def _decision(conversation_id: str, replies: frozenset[str]) -> str:
    if conversation_id in replies:
        return "reply"
    return "dismiss" if int(conversation_id[1:]) % 2 else "defer"


def _answer_for(prompt: str, replies: frozenset[str] = frozenset()) -> str:
    return json.dumps(
        {"decisions": [{"conversation_id": c, "decision": _decision(c, replies)} for c in _ids(prompt)]}
    )


def _cut(text: str) -> str:
    """An answer the runtime's output budget cut off half-way."""
    return text[: len(text) // 2]


def _recorded(client: _LoopClient) -> list[str]:
    return [entry["conversation_id"] for entry in client.decisions.recorded]


def _chunk_prompt(tray: dict[str, Any], cards: list[dict[str, Any]], texts: Any) -> str:
    """Section 3.6: a chunk's prompt is the gate prompt with the chunk as the tray's cards."""
    return gate.build_prompt({**tray, "conversations": cards}, texts=texts)


def _texts_with(**overrides: Any) -> Texts:
    return Texts.model_validate(texts_body(**overrides))


# ─────────────── chunks ───────────────


@pytest.mark.parametrize("kind", ["plain", "async"])
@pytest.mark.asyncio
async def test_a_50_card_tray_is_five_calls_of_10_and_a_cut_off_chunk_3_leaves_only_its_cards_undecided(
    kind: str,
) -> None:
    cards = _cards(50)
    tray = _tray(cards)
    client = _LoopClient([tray])
    prompts: list[str] = []
    events: list[dict[str, Any]] = []

    def answer(prompt: str) -> str:
        prompts.append(prompt)
        text = _answer_for(prompt)
        return _cut(text) if len(prompts) == 3 else text

    async def answer_async(prompt: str) -> str:
        return answer(prompt)

    loop = _loop(
        client,
        call_model=answer if kind == "plain" else answer_async,
        start_turn=_Turns(),
        on_event=events.append,
    )
    await loop.start()
    await _cycle(loop)

    texts = texts_body()
    assert len(prompts) == 5
    for index, prompt in enumerate(prompts):
        chunk = cards[index * 10 : index * 10 + 10]
        assert _ids(prompt) == [card["conversation_id"] for card in chunk]
        assert prompt == _chunk_prompt(tray, chunk, Texts.model_validate(texts))
        assert "Your tray, as the server built it: 10 conversations." in prompt
        assert "Card 10 of 10" in prompt and "Card 11" not in prompt
    # Chunks 1, 2, 4 and 5 are recorded; chunk 3's cards stay undecided.
    undecided = {f"c{i:02d}" for i in range(21, 31)}
    assert _recorded(client) == [c["conversation_id"] for c in cards if c["conversation_id"] not in undecided]
    for entry in client.decisions.recorded:
        cid = entry["conversation_id"]
        assert entry["decision"] == _decision(cid, frozenset())
        assert entry["through_message_id"] == "m" + cid[1:]
    summary = loop.status().last_cycle
    assert summary is not None
    assert summary.outcome == "decided"
    assert (summary.gated, summary.chunks_ok, summary.chunks_failed, summary.undecided) == (50, 4, 1, 10)
    assert (summary.dismiss, summary.defer, summary.reply) == (20, 20, 0)
    assert summary.failure == "1 of 5 gate chunks failed; chunk 3: no usable verdicts in the answer"
    reports = [{k: v for k, v in e.items() if k != "type"} for e in events if e["type"] == "gate_chunk"]
    assert reports == [
        {
            "index": i,
            "of": 5,
            "cards": 10,
            "outcome": "fail_closed" if i == 3 else "decided",
            "undecided": 10 if i == 3 else 0,
            "failure": "no usable verdicts in the answer" if i == 3 else None,
        }
        for i in range(1, 6)
    ]
    counters = loop.status().counters
    assert (counters["chunks_ok"], counters["chunks_failed"]) == (4, 1)
    await loop.stop()


@pytest.mark.asyncio
async def test_an_answer_budget_that_fits_10_answers_but_not_50_no_longer_leaves_the_tray_stuck() -> None:
    # The launch blocker: the runtime cuts every answer at a fixed budget.
    # Ten answers fit it, fifty do not.
    cards = _cards(50)
    client = _LoopClient([_tray(cards)])
    full = _answer_for(json.dumps([{"conversation_id": c["conversation_id"]} for c in cards], indent=1))
    budget = len(full) // 3
    assert len(_answer_for(json.dumps(cards[:10], indent=1))) < budget < len(full)
    loop = _loop(client, call_model=lambda prompt: _answer_for(prompt)[:budget], start_turn=_Turns())
    await loop.start()
    await _cycle(loop)
    assert sorted(_recorded(client)) == [c["conversation_id"] for c in cards]
    assert loop.status().last_cycle.outcome == "decided"  # type: ignore[union-attr]
    await loop.stop()


@pytest.mark.parametrize("count", [1, 4, 10])
@pytest.mark.asyncio
async def test_a_tray_of_10_cards_or_fewer_is_one_call_with_exactly_the_whole_trays_prompt(count: int) -> None:
    cards = _cards(count)
    tray = _tray(cards)
    client = _LoopClient([tray])
    prompts: list[str] = []
    loop = _loop(client, call_model=lambda p: prompts.append(p) or _answer_for(p), start_turn=_Turns())
    await loop.start()
    await _cycle(loop)
    assert prompts == [gate.build_prompt(tray, texts=Texts.model_validate(texts_body()))]
    summary = loop.status().last_cycle
    assert summary is not None and (summary.chunks_ok, summary.chunks_failed, summary.failure) == (1, 0, None)
    await loop.stop()


@pytest.mark.asyncio
async def test_the_sample_tray_through_the_loop_gives_gate_prompt_expected_byte_for_byte() -> None:
    tray = json.loads((FIXTURES / "tray.sample.json").read_bytes().decode("utf-8"))
    texts = Texts.model_validate_json((FIXTURES / "texts.sample.json").read_bytes())
    expected = (FIXTURES / "gate-prompt.expected.txt").read_bytes().decode("utf-8")
    client = _LoopClient([tray])
    prompts: list[str] = []
    loop = ReceiveLoop(
        client,
        texts,
        "gate_call",
        socket=None,
        quiet_ms=0,
        max_ms=0,
        call_model=lambda p: prompts.append(p) or "",
        start_turn=_Turns(),
    )
    await loop.start()
    await _cycle(loop)
    assert len(prompts) == 1 and prompts[0] + "\n" == expected
    await loop.stop()


@pytest.mark.asyncio
async def test_11_cards_are_two_calls_numbered_each_for_its_own_chunk_and_the_size_changes_the_split() -> None:
    cards = _cards(11)
    tray = _tray(cards)
    prompts: list[str] = []
    client = _LoopClient([tray])
    loop = _loop(client, call_model=lambda p: prompts.append(p) or _answer_for(p), start_turn=_Turns())
    await loop.start()
    await _cycle(loop)
    assert [len(_ids(p)) for p in prompts] == [10, 1]
    assert "Your tray, as the server built it: 1 conversation.\n\nCard 1 of 1\n" in prompts[1]
    assert prompts[1] == _chunk_prompt(tray, cards[10:], Texts.model_validate(texts_body()))
    await loop.stop()

    prompts.clear()
    client = _LoopClient([_tray(_cards(10))])
    loop = _loop(client, call_model=lambda p: prompts.append(p) or _answer_for(p), start_turn=_Turns(), gate_chunk_size=4)
    await loop.start()
    await _cycle(loop)
    assert [_ids(p) for p in prompts] == [
        ["c01", "c02", "c03", "c04"],
        ["c05", "c06", "c07", "c08"],
        ["c09", "c10"],
    ]
    assert len(client.decisions.recorded) == 10
    await loop.stop()

    prompts.clear()
    client = _LoopClient([_tray(_cards(50))])
    loop = _loop(client, call_model=lambda p: prompts.append(p) or _answer_for(p), start_turn=_Turns(), gate_chunk_size=50)
    await loop.start()
    await _cycle(loop)
    assert [len(_ids(p)) for p in prompts] == [50]  # 50: the largest tray in one call
    await loop.stop()


@pytest.mark.parametrize("value", [0, -1, 51, 1.5, 10.0, True, "10"])
def test_gate_chunk_size_must_be_an_int_from_1_to_50(value: Any) -> None:
    client = _LoopClient([_tray([])])
    with pytest.raises(ValueError, match="gate_chunk_size"):
        _loop(client, call_model=lambda _p: "", start_turn=_Turns(), gate_chunk_size=value)
    # Checked in single-run mode too: a wrong option is wrong in any mode.
    with pytest.raises(ValueError, match="gate_chunk_size"):
        _loop(client, mode="single_run", start_run=lambda _r: None, gate_chunk_size=value)


def test_gate_chunk_size_defaults_to_10_and_takes_1_to_50() -> None:
    client = _LoopClient([_tray([])])
    assert _loop(client, call_model=lambda _p: "", start_turn=_Turns())._gate_chunk_size == 10
    assert _loop(client, call_model=lambda _p: "", start_turn=_Turns(), gate_chunk_size=None)._gate_chunk_size == 10
    for value in (1, 7, 50):
        loop = _loop(client, call_model=lambda _p: "", start_turn=_Turns(), gate_chunk_size=value)
        assert loop._gate_chunk_size == value


@pytest.mark.asyncio
async def test_every_chunk_failing_is_fail_closed_and_records_nothing() -> None:
    client = _LoopClient([_tray(_cards(25))])
    calls: list[str] = []
    loop = _loop(client, call_model=lambda p: calls.append(p) or "", start_turn=_Turns())
    await loop.start()
    await _cycle(loop)
    assert len(calls) == 3 and client.decisions.recorded == []
    summary = loop.status().last_cycle
    assert summary is not None and summary.outcome == "fail_closed"
    assert (summary.chunks_ok, summary.chunks_failed, summary.undecided, summary.gated) == (0, 3, 25, 25)
    assert summary.failure == "3 of 3 gate chunks failed; chunk 1: no usable verdicts in the answer"
    await loop.stop()


@pytest.mark.asyncio
async def test_a_single_call_that_fails_keeps_its_own_reason() -> None:
    client = _LoopClient([_tray(_cards(3))])
    loop = _loop(client, call_model=lambda _p: "", start_turn=_Turns())
    await loop.start()
    await _cycle(loop)
    summary = loop.status().last_cycle
    assert summary is not None and summary.outcome == "fail_closed"
    assert summary.failure == "no usable verdicts in the answer"
    assert (summary.chunks_ok, summary.chunks_failed) == (0, 1)
    await loop.stop()


@pytest.mark.asyncio
async def test_replies_from_several_chunks_start_after_the_last_chunk_in_tray_order_with_their_chunks_card() -> None:
    cards = _cards(30)
    tray = _tray(cards)
    sent = _tray(
        [_card("c25", "m25", your_last_message_at="2026-11-08T09:05:00.000Z", you_owe_reply=False)]
    )
    client = _LoopClient([tray, sent])
    calls: list[list[str]] = []
    calls_at_start: list[int] = []
    turns = _Turns()

    def call_model(prompt: str) -> str:
        calls.append(_ids(prompt))
        return _answer_for(prompt, replies=frozenset({"c05", "c25"}))

    async def start_turn(request: TurnRequest) -> TurnAdmission:
        calls_at_start.append(len(calls))
        return await turns(request)

    loop = ReceiveLoop(
        client,
        _texts_with(reply_turn_template=TEMPLATE),
        "gate_call",
        socket=None,
        quiet_ms=0,
        max_ms=0,
        call_model=call_model,
        start_turn=start_turn,
        max_concurrent_turns=5,
    )
    await loop.start()
    await _cycle(loop)
    assert len(calls) == 3
    assert calls_at_start == [3, 3]  # no turn before every chunk had answered
    assert [r.conversation_id for r in turns.requests] == ["c05", "c25"]
    later = turns.requests[1]
    assert later.card == cards[24] and later.anchor == "m25"  # the card its chunk's gate saw
    assert later.prompt == TEMPLATE.replace("<conversation_id>", "c25")
    summary = loop.status().last_cycle
    assert summary is not None and (summary.reply, summary.turns_started) == (2, 2)
    # Its outcome reads the fresh tray against that card.
    turns.ends[1].set_result(TurnEnd(status="completed", agentchat_calls=AgentChatCallCount(made=3, failed=0)))
    await _eventually(lambda: len(client.decisions.recorded) == 29, "the reply verdict")
    assert client.decisions.recorded[-1] == {
        "conversation_id": "c25",
        "decision": "reply",
        "through_message_id": "m25",
    }
    await loop.stop()


@pytest.mark.asyncio
async def test_a_card_a_turn_took_while_an_earlier_chunk_was_gated_is_held_out_of_its_own_chunk() -> None:
    tracker = TurnTracker()
    client = _LoopClient([_tray(_cards(20))])
    calls: list[list[str]] = []

    async def call_model(prompt: str) -> str:
        calls.append(_ids(prompt))
        if len(calls) == 1:
            # The host starts its own turn for c15 (a shared tracker) while
            # chunk 1 is gated.
            assert tracker.begin(TrackedTurn.reply("c15", "m15")) is not None
        return _answer_for(prompt)

    loop = _loop(client, call_model=call_model, start_turn=_Turns(), turn_tracker=tracker)
    await loop.start()
    await _cycle(loop)
    assert len(calls) == 2 and "c15" not in calls[1] and len(calls[1]) == 9
    assert "c15" not in _recorded(client) and len(client.decisions.recorded) == 19
    summary = loop.status().last_cycle
    assert summary is not None and (summary.held_by_turns, summary.gated) == (1, 19)
    await loop.stop()


@pytest.mark.asyncio
async def test_a_truncated_tray_with_a_failed_chunk_still_reads_the_rest_at_once() -> None:
    client = _LoopClient([_tray(_cards(20), truncated=True)])
    events: list[dict[str, Any]] = []
    calls: list[str] = []

    def call_model(prompt: str) -> str:
        calls.append(prompt)
        return "" if len(calls) == 2 else _answer_for(prompt)

    loop = _loop(client, call_model=call_model, start_turn=_Turns(), on_event=events.append)
    await loop.start()
    loop.wake()
    await _eventually(lambda: any(e["type"] == "cycle" for e in events), "the first cycle")
    first = next(e["summary"] for e in events if e["type"] == "cycle")
    assert (first.outcome, first.chunks_ok, first.chunks_failed) == ("decided", 1, 1)
    # Progress (chunk 1 recorded): the rest is read at once, not after a backoff.
    await _eventually(lambda: any(e["type"] == "wake" and e["source"] == "more" for e in events), "a read of the rest")
    await loop.stop()


# ─────────────── deadlines and cancellation, per chunk ───────────────


@pytest.mark.asyncio
async def test_each_chunk_has_its_own_deadline_and_cancel_handle_async() -> None:
    client = _LoopClient([_tray(_cards(30))])
    handles: list[tuple[asyncio.Event, bool]] = []

    async def call_model(prompt: str, *, cancel: asyncio.Event) -> str:
        handles.append((cancel, cancel.is_set()))
        if len(handles) == 2:
            await cancel.wait()  # past its deadline: told, and it stops
            return ""
        return _answer_for(prompt)

    loop = _loop(client, call_model=call_model, start_turn=_Turns(), gate_call_timeout_ms=50)
    await loop.start()
    await _cycle(loop)
    await _eventually(lambda: loop.status().last_cycle is not None, "the cycle")
    assert len(handles) == 3
    assert [was_set for _h, was_set in handles] == [False, False, False]  # each call starts fresh
    assert handles[1][0].is_set() and not handles[0][0].is_set() and not handles[2][0].is_set()
    assert len({id(h) for h, _s in handles}) == 3
    assert _recorded(client) == [f"c{i:02d}" for i in [*range(1, 11), *range(21, 31)]]
    summary = loop.status().last_cycle
    assert summary is not None and summary.outcome == "decided"
    assert (summary.chunks_ok, summary.chunks_failed, summary.undecided) == (2, 1, 10)
    assert summary.failure == (
        "1 of 3 gate chunks failed; chunk 2: model call timed out: no answer within 50 ms"
    )
    await loop.stop()


@pytest.mark.asyncio
async def test_each_chunk_has_its_own_deadline_and_cancel_handle_plain() -> None:
    client = _LoopClient([_tray(_cards(30))])
    handles: list[tuple[threading.Event, bool]] = []
    lock = threading.Lock()

    def call_model(prompt: str, cancel: threading.Event) -> str:
        with lock:
            handles.append((cancel, cancel.is_set()))
            index = len(handles)
        if index == 2:
            cancel.wait(5.0)  # a blocking runtime call, told at the deadline
            return _answer_for(prompt)  # a late answer is not acted on
        return _answer_for(prompt)

    loop = _loop(client, call_model=call_model, start_turn=_Turns(), gate_call_timeout_ms=50)
    await loop.start()
    await _cycle(loop)
    await _eventually(lambda: loop.status().last_cycle is not None and len(handles) == 3, "the cycle")
    await loop.idle()
    assert [was_set for _h, was_set in handles] == [False, False, False]
    assert handles[1][0].is_set() and not handles[2][0].is_set()
    assert _recorded(client) == [f"c{i:02d}" for i in [*range(1, 11), *range(21, 31)]]
    summary = loop.status().last_cycle
    assert summary is not None and (summary.outcome, summary.chunks_failed) == ("decided", 1)
    assert "chunk 2: model call timed out" in (summary.failure or "")
    await loop.stop()


@pytest.mark.asyncio
async def test_time_inside_gate_calls_with_a_deadline_does_not_count_against_the_cycle() -> None:
    # Three chunks of 150 ms each against a 100 ms cycle deadline: each call
    # is bounded by its own deadline, and a cycle making progress is not
    # abandoned for taking k of them.
    client = _LoopClient([_tray(_cards(30))])

    async def call_model(prompt: str) -> str:
        await asyncio.sleep(0.15)
        return _answer_for(prompt)

    loop = _loop(
        client, call_model=call_model, start_turn=_Turns(), gate_call_timeout_ms=2_000, cycle_timeout_ms=100
    )
    await loop.start()
    loop.wake()
    await _eventually(lambda: loop.status().last_cycle is not None, "the cycle")
    summary = loop.status().last_cycle
    assert summary is not None and summary.outcome == "decided" and summary.chunks_ok == 3
    assert len(client.decisions.recorded) == 30
    await loop.stop()


@pytest.mark.asyncio
async def test_stop_during_a_chunk_tells_its_call_asks_no_later_chunk_and_starts_no_turn() -> None:
    client = _LoopClient([_tray(_cards(30))])
    turns = _Turns()
    handles: list[asyncio.Event] = []
    second = asyncio.Event()

    async def call_model(prompt: str, *, cancel: asyncio.Event) -> str:
        handles.append(cancel)
        if len(handles) == 2:
            second.set()
            await cancel.wait()
            return _answer_for(prompt)  # a late answer: never acted on
        return _answer_for(prompt, replies=frozenset({"c01"}))

    loop = _loop(client, call_model=call_model, start_turn=turns, max_concurrent_turns=5)
    await loop.start()
    loop.wake()
    await asyncio.wait_for(second.wait(), 5.0)
    await asyncio.wait_for(loop.stop(), 5.0)
    assert len(handles) == 2  # chunk 3 is never asked
    assert handles[1].is_set() and not handles[0].is_set()
    assert _recorded(client) == [f"c{i:02d}" for i in range(2, 11)]  # chunk 1's stand
    assert turns.requests == []  # chunk 1's reply never starts
    summary = loop.status().last_cycle
    assert summary is not None and summary.outcome == "abandoned"
    # The call the stop cut short is not a failed chunk.
    assert (summary.chunks_ok, summary.chunks_failed, summary.failure) == (1, 0, None)
    assert (summary.gated, summary.reply, summary.turns_started) == (10, 1, 0)


@pytest.mark.asyncio
async def test_a_watchdog_abandonment_with_the_gate_deadline_off_stops_between_chunks() -> None:
    client = _LoopClient([_tray(_cards(30))])
    turns = _Turns()
    handles: list[asyncio.Event] = []

    async def call_model(prompt: str, *, cancel: asyncio.Event) -> str:
        handles.append(cancel)
        if len(handles) == 2:
            await cancel.wait()  # hangs until told
            return _answer_for(prompt)
        return _answer_for(prompt, replies=frozenset({"c01"}))

    loop = _loop(
        client,
        call_model=call_model,
        start_turn=turns,
        gate_call_timeout_ms=0,
        cycle_timeout_ms=100,
        max_concurrent_turns=5,
    )
    await loop.start()
    loop.wake()
    await _eventually(lambda: loop.status().last_cycle is not None, "the abandoned cycle")
    await asyncio.sleep(0.05)
    assert loop.status().last_cycle.outcome == "abandoned"  # type: ignore[union-attr]
    assert len(handles) == 2 and handles[1].is_set()
    assert _recorded(client) == [f"c{i:02d}" for i in range(2, 11)]
    assert turns.requests == []
    await loop.stop()


# ─────────────── the reply-turn prompt ───────────────


def test_reply_prompt_fills_every_placeholder_of_the_served_template() -> None:
    texts = _texts_with(reply_turn_template=f"  {TEMPLATE}\n")
    expected = TEMPLATE.replace("<conversation_id>", "conv_1")
    assert reply_prompt(texts, "conv_1") == expected
    assert reply_prompt(texts.model_dump(mode="json"), "conv_1") == expected  # the raw JSON too
    # The id goes in as it is: no pattern syntax.
    assert reply_prompt(texts, r"conv_$&\1x") == TEMPLATE.replace("<conversation_id>", r"conv_$&\1x")
    assert expected.count("conv_1") == 2 and "<conversation_id>" not in expected


@pytest.mark.parametrize(
    "template",
    [None, "", "Reply to the conversation named above."],
    ids=["absent", "empty", "no placeholder"],
)
def test_reply_prompt_without_a_usable_template_is_the_wake_instruction_and_the_id(template: Any) -> None:
    body = texts_body()
    if template is not None:
        body["reply_turn_template"] = template
    expected = f"{body['wake_instruction']}\n\nconversation_id: conv_1"
    assert reply_prompt(Texts.model_validate(body), "conv_1") == expected
    assert reply_prompt(body, "conv_1") == expected


def test_texts_take_the_optional_reply_turn_template() -> None:
    assert Texts.model_validate(texts_body(reply_turn_template=TEMPLATE)).reply_turn_template == TEMPLATE
    assert Texts.model_validate(texts_body()).reply_turn_template is None


@pytest.mark.asyncio
async def test_a_reply_turn_starts_from_the_served_template_and_an_invite_turn_from_the_invite_prompt() -> None:
    texts = Texts.model_validate_json((FIXTURES / "texts.sample.json").read_bytes())
    assert texts.reply_turn_template is not None
    invite = {"invite_id": "ginv_1", "group_id": "grp_1", "invited_by": "@lead", "group_name": "Ops"}
    client = _LoopClient([_tray([_card("c1", "m1")], [invite])])
    turns = _Turns()
    loop = ReceiveLoop(
        client,
        texts,
        "gate_call",
        socket=None,
        quiet_ms=0,
        max_ms=0,
        call_model=lambda _p: json.dumps({"decisions": [{"conversation_id": "c1", "decision": "reply"}]}),
        start_turn=turns,
        max_concurrent_turns=2,
    )
    await loop.start()
    await _cycle(loop)
    by_kind = {r.kind: r for r in turns.requests}
    assert by_kind["reply"].prompt == texts.reply_turn_template.strip().replace("<conversation_id>", "c1")
    assert by_kind["invite"].prompt == f"{texts.invite_prompt.strip()}\n\ninvite_id: ginv_1"
    await loop.stop()


@pytest.mark.asyncio
async def test_a_reply_turn_from_texts_without_the_template_starts_as_before() -> None:
    client = _LoopClient([_tray([_card("c1", "m1")])])
    turns = _Turns()
    loop = _loop(
        client,
        call_model=lambda _p: json.dumps({"decisions": [{"conversation_id": "c1", "decision": "reply"}]}),
        start_turn=turns,
    )
    await loop.start()
    await _cycle(loop)
    assert turns.requests[0].prompt == f"{texts_body()['wake_instruction']}\n\nconversation_id: c1"
    await loop.stop()


@pytest.mark.asyncio
async def test_the_texts_cache_keeps_the_template_in_hand_in_the_file_and_from_the_file(tmp_path: Path) -> None:
    file = tmp_path / "texts.json"
    served = texts_body(runtime="hermes", reply_turn_template=TEMPLATE)
    online = TextsCache(_AsyncTextsClient([served]), runtime="hermes", file_path=file)
    await online.start()
    try:
        current = online.current()
        assert current is not None and current.reply_turn_template == TEMPLATE
        assert json.loads(file.read_text())["texts"]["reply_turn_template"] == TEMPLATE
    finally:
        online.stop()
    offline = TextsCache(_AsyncTextsClient([OSError("offline")]), runtime="hermes", file_path=file)
    await offline.start()
    try:
        current = offline.current()
        assert offline.source == "cache"
        assert current is not None and current.reply_turn_template == TEMPLATE
    finally:
        offline.stop()
    # Texts without it (an older server) are still a text set.
    older = TextsCache(_AsyncTextsClient([texts_body(runtime="hermes")]), runtime="hermes")
    await older.start()
    try:
        current = older.current()
        assert older.source == "server"
        assert current is not None and current.reply_turn_template is None
    finally:
        older.stop()
