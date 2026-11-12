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
    Readiness,
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


class _FakeTime:
    """The loop's clock (``now``) and timers (``set_timer``), moved only by
    the test: a deadline passes when the test says so, never because a
    machine was slow. A 0 ms timer (the coalescer's windows at
    ``quiet_ms=0``) runs on the event loop at once."""

    def __init__(self) -> None:
        self.now = 1_000_000.0
        self._seq = 0
        self.pending: list[tuple[float, int, Any]] = []

    def clock(self) -> float:
        return self.now

    def set_timer(self, delay_ms: float, fn: Any) -> Any:
        if delay_ms <= 0:
            return asyncio.get_running_loop().call_later(0, fn)
        self._seq += 1
        entry = (self.now + delay_ms, self._seq, fn)
        self.pending.append(entry)
        pending = self.pending

        class _Handle:
            def cancel(self) -> None:
                if entry in pending:
                    pending.remove(entry)

        return _Handle()

    def advance(self, ms: float) -> None:
        """Move the clock by ``ms``, firing every timer due by then, in order."""
        target = self.now + ms
        while True:
            due = [entry for entry in self.pending if entry[0] <= target]
            if not due:
                break
            entry = min(due, key=lambda e: (e[0], e[1]))
            self.pending.remove(entry)
            self.now = entry[0]
            entry[2]()
        self.now = target


def _fake_loop(client: _LoopClient, fake: _FakeTime, **kwargs: Any) -> ReceiveLoop:
    return _loop(client, now=fake.clock, set_timer=fake.set_timer, **kwargs)


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
    chunk_events = [e for e in events if e["type"] == "gate_chunk"]
    assert len({e["cycle"] for e in chunk_events}) == 1  # one cycle's chunks share its identifier
    reports = [{k: v for k, v in e.items() if k not in ("type", "cycle")} for e in chunk_events]
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


@pytest.mark.parametrize(
    "muted",
    [0, 3],
    ids=["owed cards only", "owed cards and muted ones"],
)
@pytest.mark.asyncio
async def test_every_owed_card_taken_by_a_turn_before_its_chunk_is_nothing_owed(muted: int) -> None:
    # Free when the tray was read, every owed card is taken by a host's turn
    # (a shared tracker) before its chunk is asked: no call, nothing_owed.
    owed = _cards(5)
    cards = owed + [_card(f"c{i}", f"m{i}", muted=True) for i in range(6, 6 + muted)]
    tracker = TurnTracker()
    calls: list[str] = []

    async def readiness() -> Readiness:
        for card in owed:
            assert tracker.begin(TrackedTurn.reply(card["conversation_id"], card["last_message_id"])) is not None
        return Readiness(ready=True)

    client = _LoopClient([_tray(cards)])
    loop = _loop(
        client,
        call_model=lambda p: calls.append(p) or _answer_for(p),
        start_turn=_Turns(),
        turn_tracker=tracker,
        readiness=readiness,
    )
    await loop.start()
    await _cycle(loop)
    assert calls == [] and client.decisions.recorded == []
    summary = loop.status().last_cycle
    assert summary is not None and summary.outcome == "nothing_owed"
    assert (summary.held_by_turns, summary.gated, summary.undecided) == (5, 0, 0)
    assert (summary.chunks_ok, summary.chunks_failed, summary.turns_started) == (0, 0, 0)
    await loop.stop()


@pytest.mark.asyncio
async def test_a_card_held_mid_gate_is_read_again_when_a_turn_of_the_loop_ends() -> None:
    # c15 is taken by a host's turn while chunk 1 is gated, so the cycle
    # leaves it out. When a turn of the loop's own ends (c01's), the cards
    # left out meanwhile are read again: c15 is gated in the next cycle.
    tracker = TurnTracker()
    tray = _tray(_cards(20))
    client = _LoopClient([tray, _tray([]), _tray([_card("c15", "m15")])])
    turns = _Turns()
    calls: list[list[str]] = []
    host: list[Any] = []

    async def call_model(prompt: str) -> str:
        calls.append(_ids(prompt))
        if len(calls) == 1:
            host.append(tracker.begin(TrackedTurn.reply("c15", "m15")))
        return _answer_for(prompt, replies=frozenset({"c01"}))

    loop = _loop(client, call_model=call_model, start_turn=turns, turn_tracker=tracker)
    await loop.start()
    await _cycle(loop)
    assert len(calls) == 2 and "c15" not in calls[1]
    assert [r.conversation_id for r in turns.requests] == ["c01"]
    tracker.end(host[0])  # the host's turn ends (the loop is not told)
    cycles = loop.status().coalescer["cycles"]
    turns.ends[0].set_result(TurnEnd(status="completed", agentchat_calls=AgentChatCallCount(made=2, failed=0)))
    await _eventually(lambda: loop.status().coalescer["cycles"] > cycles, "a cycle after c01's turn ends")
    await loop.idle()
    assert calls[-1] == ["c15"]
    assert client.decisions.recorded[-1]["conversation_id"] == "c15"
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
    fake = _FakeTime()
    client = _LoopClient([_tray(_cards(30))])
    handles: list[tuple[asyncio.Event, bool]] = []
    second = asyncio.Event()

    async def call_model(prompt: str, *, cancel: asyncio.Event) -> str:
        handles.append((cancel, cancel.is_set()))
        if len(handles) == 2:
            second.set()
            await cancel.wait()  # hangs until told
            return ""
        return _answer_for(prompt)

    loop = _fake_loop(client, fake, call_model=call_model, start_turn=_Turns(), gate_call_timeout_ms=60_000)
    await loop.start()
    loop.wake()
    await asyncio.wait_for(second.wait(), 5.0)
    fake.advance(59_999)
    for _ in range(20):
        await asyncio.sleep(0)
    assert len(handles) == 2 and not handles[1][0].is_set()  # not yet past its deadline
    fake.advance(1)  # chunk 2's own 60 s are up
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
        "1 of 3 gate chunks failed; chunk 2: model call timed out: no answer within 60000 ms"
    )
    await loop.stop()


@pytest.mark.asyncio
async def test_each_chunk_has_its_own_deadline_and_cancel_handle_plain() -> None:
    fake = _FakeTime()
    client = _LoopClient([_tray(_cards(30))])
    handles: list[tuple[threading.Event, bool]] = []
    lock = threading.Lock()

    def call_model(prompt: str, cancel: threading.Event) -> str:
        with lock:
            handles.append((cancel, cancel.is_set()))
            index = len(handles)
        if index == 2:
            cancel.wait(30.0)  # a blocking runtime call, told at the deadline
            return _answer_for(prompt)  # a late answer is not acted on
        return _answer_for(prompt)

    loop = _fake_loop(client, fake, call_model=call_model, start_turn=_Turns(), gate_call_timeout_ms=60_000)
    await loop.start()
    loop.wake()
    await _eventually(lambda: len(handles) == 2, "chunk 2's call to start in its worker thread")
    fake.advance(60_000)
    await _eventually(lambda: loop.status().last_cycle is not None, "the cycle")
    await loop.idle()
    assert len(handles) == 3
    assert [was_set for _h, was_set in handles] == [False, False, False]
    assert handles[1][0].is_set() and not handles[2][0].is_set()
    assert _recorded(client) == [f"c{i:02d}" for i in [*range(1, 11), *range(21, 31)]]
    summary = loop.status().last_cycle
    assert summary is not None and (summary.outcome, summary.chunks_failed) == ("decided", 1)
    assert "chunk 2: model call timed out" in (summary.failure or "")
    await loop.stop()


@pytest.mark.asyncio
async def test_time_inside_gate_calls_with_a_deadline_does_not_count_against_the_cycle() -> None:
    # Three calls of 150 ms each (of the loop's clock) against a 100 ms cycle
    # deadline: each is bounded by its own deadline, and a cycle making
    # progress is not abandoned for taking three of them.
    fake = _FakeTime()
    client = _LoopClient([_tray(_cards(30))])

    async def call_model(prompt: str) -> str:
        fake.advance(150)
        return _answer_for(prompt)

    loop = _fake_loop(
        client, fake, call_model=call_model, start_turn=_Turns(), gate_call_timeout_ms=1_000, cycle_timeout_ms=100
    )
    await loop.start()
    loop.wake()
    await _eventually(lambda: loop.status().last_cycle is not None, "the cycle")
    summary = loop.status().last_cycle
    assert summary is not None and summary.outcome == "decided" and summary.chunks_ok == 3
    assert len(client.decisions.recorded) == 30
    await loop.stop()


@pytest.mark.asyncio
async def test_a_gate_deadline_longer_than_the_cycle_deadline_is_not_cut_by_the_watchdog() -> None:
    fake = _FakeTime()
    client = _LoopClient([_tray(_cards(5))])
    calling = asyncio.Event()
    answer = asyncio.Event()

    async def call_model(prompt: str) -> str:
        calling.set()
        await answer.wait()
        return _answer_for(prompt)

    loop = _fake_loop(
        client, fake, call_model=call_model, start_turn=_Turns(), gate_call_timeout_ms=900_000, cycle_timeout_ms=600_000
    )
    await loop.start()
    loop.wake()
    await asyncio.wait_for(calling.wait(), 5.0)
    fake.advance(800_000)  # past the cycle's 10 min, inside the call's 15
    for _ in range(20):
        await asyncio.sleep(0)
    assert loop.status().last_cycle is None
    answer.set()
    await _eventually(lambda: loop.status().last_cycle is not None, "the cycle")
    assert loop.status().last_cycle.outcome == "decided"  # type: ignore[union-attr]
    await loop.stop()


@pytest.mark.asyncio
async def test_a_deadline_does_not_wait_for_a_call_that_ignores_its_cancellation() -> None:
    # A runtime call that swallows CancelledError and keeps waiting: past its
    # deadline the cycle no longer waits for it (cancel-then-await would hang
    # the cycle until the call chose to end).
    fake = _FakeTime()
    client = _LoopClient([_tray(_cards(3))])
    calling = asyncio.Event()
    release = asyncio.Event()
    swallowed: list[bool] = []

    async def call_model(prompt: str) -> str:
        calling.set()
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                swallowed.append(True)  # ignores the cancellation
        return _answer_for(prompt)

    loop = _fake_loop(client, fake, call_model=call_model, start_turn=_Turns(), gate_call_timeout_ms=60_000)
    await loop.start()
    try:
        loop.wake()
        await asyncio.wait_for(calling.wait(), 5.0)
        fake.advance(60_000)
        await _eventually(lambda: loop.status().last_cycle is not None, "the cycle to end at the deadline")
        summary = loop.status().last_cycle
        assert summary is not None and summary.outcome == "fail_closed"
        assert summary.failure == "model call timed out: no answer within 60000 ms"
        assert swallowed == [True]  # the call is still running, unheard
        release.set()  # its late answer is not acted on
        for _ in range(20):
            await asyncio.sleep(0)
        assert client.decisions.recorded == []
    finally:
        release.set()  # a regression that waits for the call must fail, not hang
        await asyncio.wait_for(loop.stop(), 5.0)


@pytest.mark.asyncio
async def test_a_large_deadline_is_printed_as_an_integer_in_the_failure() -> None:
    fake = _FakeTime()
    client = _LoopClient([_tray(_cards(2))])
    calling = asyncio.Event()

    async def call_model(_prompt: str, *, cancel: asyncio.Event) -> str:
        calling.set()
        await cancel.wait()
        return ""

    loop = _fake_loop(client, fake, call_model=call_model, start_turn=_Turns(), gate_call_timeout_ms=1_200_000)
    await loop.start()
    loop.wake()
    await asyncio.wait_for(calling.wait(), 5.0)
    fake.advance(1_200_000)
    await _eventually(lambda: loop.status().last_cycle is not None, "the cycle")
    assert loop.status().last_cycle.failure == "model call timed out: no answer within 1200000 ms"  # type: ignore[union-attr]
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
async def test_stop_while_a_chunk_records_counts_each_card_once() -> None:
    # The answer parsed; c05's verdict is on its way when stop() comes. c05
    # is recorded (never aborted); c06-c10 are not, and count as undecided
    # only: gated is the sum of the outcomes.
    cards = _cards(20)
    on_its_way = asyncio.Event()
    release = asyncio.Event()

    class HeldDecisions:
        def __init__(self) -> None:
            self.recorded: list[dict[str, Any]] = []

        async def record(self, conversation_id: str, **kwargs: Any) -> Any:
            if conversation_id == "c05":
                on_its_way.set()
                await release.wait()
            self.recorded.append({"conversation_id": conversation_id, **kwargs})
            return {"id": "d"}

    client = _LoopClient([_tray(cards)])
    client.decisions = HeldDecisions()  # type: ignore[assignment]
    calls: list[str] = []
    loop = _loop(client, call_model=lambda p: calls.append(p) or _answer_for(p), start_turn=_Turns())
    await loop.start()
    loop.wake()
    await asyncio.wait_for(on_its_way.wait(), 5.0)
    stopping = asyncio.ensure_future(loop.stop())
    await _eventually(lambda: loop.status().stopped)
    release.set()
    await asyncio.wait_for(stopping, 5.0)
    assert len(calls) == 1  # chunk 2 is never asked
    assert _recorded(client) == ["c01", "c02", "c03", "c04", "c05"]
    summary = loop.status().last_cycle
    assert summary is not None and summary.outcome == "abandoned"
    assert (summary.dismiss, summary.defer, summary.reply, summary.undecided) == (3, 2, 0, 5)
    assert summary.gated == summary.dismiss + summary.defer + summary.reply + summary.undecided == 10


@pytest.mark.asyncio
async def test_a_watchdog_abandonment_with_the_gate_deadline_off_stops_between_chunks() -> None:
    fake = _FakeTime()
    client = _LoopClient([_tray(_cards(30))])
    turns = _Turns()
    events: list[dict[str, Any]] = []
    handles: list[asyncio.Event] = []
    second = asyncio.Event()

    async def call_model(prompt: str, *, cancel: asyncio.Event) -> str:
        handles.append(cancel)
        if len(handles) == 2:
            second.set()
            await cancel.wait()  # hangs until told
            return _answer_for(prompt)  # a late answer: never acted on
        return _answer_for(prompt, replies=frozenset({"c01"}))

    loop = _fake_loop(
        client,
        fake,
        call_model=call_model,
        start_turn=turns,
        gate_call_timeout_ms=0,
        cycle_timeout_ms=100,
        max_concurrent_turns=5,
        on_event=events.append,
    )
    await loop.start()
    loop.wake()
    await asyncio.wait_for(second.wait(), 5.0)
    fake.advance(99)
    for _ in range(20):
        await asyncio.sleep(0)
    assert loop.status().last_cycle is None  # not yet
    fake.advance(1)  # the cycle's 100 ms are up, all of it counted
    await _eventually(lambda: loop.status().last_cycle is not None, "the abandoned cycle")
    for _ in range(20):
        await asyncio.sleep(0)
    assert loop.status().last_cycle.outcome == "abandoned"  # type: ignore[union-attr]
    assert len(handles) == 2 and handles[1].is_set()  # chunk 3 never asked
    assert _recorded(client) == [f"c{i:02d}" for i in range(2, 11)]
    assert turns.requests == []
    # The abandoned cycle reported chunk 1 and nothing after it.
    first = [e for e in events if e["type"] == "gate_chunk"]
    assert [(e["index"], e["outcome"]) for e in first] == [(1, "decided")]
    assert (loop.status().counters["chunks_ok"], loop.status().counters["chunks_failed"]) == (1, 0)
    # The next cycle's chunks carry an identifier of their own.
    loop.wake()
    await _eventually(lambda: len([e for e in events if e["type"] == "cycle"]) == 2, "the next cycle")
    later = [e for e in events if e["type"] == "gate_chunk"][1:]
    assert [e["index"] for e in later] == [1, 2, 3]
    assert len({e["cycle"] for e in later}) == 1 and later[0]["cycle"] != first[0]["cycle"]
    await loop.stop()


# ─────────────── muted cards ───────────────


@pytest.mark.asyncio
async def test_a_chunk_of_muted_cards_only_costs_no_model_call() -> None:
    owed = _cards(10)
    muted = [_card(f"c{i}", f"m{i}", muted=True) for i in range(11, 14)]
    client = _LoopClient([_tray(owed + muted)])
    calls: list[list[str]] = []
    events: list[dict[str, Any]] = []
    loop = _loop(
        client, call_model=lambda p: calls.append(_ids(p)) or _answer_for(p), start_turn=_Turns(), on_event=events.append
    )
    await loop.start()
    await _cycle(loop)
    assert calls == [[c["conversation_id"] for c in owed]]  # 10 owed + 3 muted: one call
    assert sorted(_recorded(client)) == [c["conversation_id"] for c in owed]
    summary = loop.status().last_cycle
    assert summary is not None and summary.outcome == "decided"
    # The muted cards are left as in a nothing_owed cycle: not gated, not undecided.
    assert (summary.gated, summary.undecided, summary.chunks_ok, summary.chunks_failed) == (10, 0, 1, 0)
    assert [(e["index"], e["of"]) for e in events if e["type"] == "gate_chunk"] == [(1, 2)]
    await loop.stop()


@pytest.mark.asyncio
async def test_one_call_made_gives_its_bare_reason_even_when_the_plan_had_more_chunks() -> None:
    # 10 owed + 3 muted: two chunks planned, one call made; it answers
    # nothing parsable.
    owed = _cards(10)
    muted = [_card(f"c{i}", f"m{i}", muted=True) for i in range(11, 14)]
    client = _LoopClient([_tray(owed + muted)])
    events: list[dict[str, Any]] = []
    loop = _loop(client, call_model=lambda _p: "", start_turn=_Turns(), on_event=events.append)
    await loop.start()
    await _cycle(loop)
    summary = loop.status().last_cycle
    assert summary is not None and summary.outcome == "fail_closed"
    assert summary.failure == "no usable verdicts in the answer"
    assert (summary.chunks_ok, summary.chunks_failed, summary.undecided, summary.gated) == (0, 1, 10, 10)
    assert [(e["index"], e["of"]) for e in events if e["type"] == "gate_chunk"] == [(1, 2)]
    await loop.stop()


@pytest.mark.asyncio
async def test_the_failure_counts_the_calls_made_and_names_the_chunk_by_its_planned_position() -> None:
    # Three chunks planned; the middle one is muted cards only (no call).
    # Of the two calls made, the second (chunk 3) answers nothing parsable.
    cards = (
        _cards(10)
        + [_card(f"c{i}", f"m{i}", muted=True) for i in range(11, 21)]
        + [_card(f"c{i}", f"m{i}") for i in range(21, 26)]
    )
    client = _LoopClient([_tray(cards)])
    events: list[dict[str, Any]] = []
    calls: list[str] = []

    def call_model(prompt: str) -> str:
        calls.append(prompt)
        return "" if len(calls) == 2 else _answer_for(prompt)

    loop = _loop(client, call_model=call_model, start_turn=_Turns(), on_event=events.append)
    await loop.start()
    await _cycle(loop)
    assert len(calls) == 2
    summary = loop.status().last_cycle
    assert summary is not None and summary.outcome == "decided"
    assert summary.failure == "1 of 2 gate chunks failed; chunk 3: no usable verdicts in the answer"
    reports = [(e["index"], e["of"], e["outcome"]) for e in events if e["type"] == "gate_chunk"]
    assert reports == [(1, 3, "decided"), (3, 3, "fail_closed")]
    await loop.stop()


@pytest.mark.asyncio
async def test_muted_cards_in_a_chunk_with_owed_ones_are_gated_with_them_as_before() -> None:
    cards = [_card("c01", "m01"), _card("c02", "m02", muted=True), _card("c03", "m03")]
    client = _LoopClient([_tray(cards)])
    calls: list[list[str]] = []
    loop = _loop(client, call_model=lambda p: calls.append(_ids(p)) or _answer_for(p), start_turn=_Turns())
    await loop.start()
    await _cycle(loop)
    assert calls == [["c01", "c02", "c03"]]
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
    [None, "", "  \n\t "],
    ids=["absent", "empty", "blank"],
)
def test_reply_prompt_without_the_template_is_the_wake_instruction_and_the_id(template: Any) -> None:
    body = texts_body()
    if template is not None:
        body["reply_turn_template"] = template
    expected = f"{body['wake_instruction']}\n\nconversation_id: conv_1"
    assert reply_prompt(Texts.model_validate(body), "conv_1") == expected
    assert reply_prompt(body, "conv_1") == expected


@pytest.mark.parametrize("value", [42, ["x <conversation_id>"], {"text": "<conversation_id>"}])
def test_reply_prompt_from_raw_texts_whose_template_is_not_a_string_is_the_wake_instruction(value: Any) -> None:
    body = {**texts_body(), "reply_turn_template": value}
    assert reply_prompt(body, "conv_1") == f"{body['wake_instruction']}\n\nconversation_id: conv_1"


def test_reply_prompt_from_a_template_without_the_placeholder_names_the_conversation_after_it() -> None:
    template = "  Reply to the one conversation this turn is for; leave the rest alone.\n"
    expected = f"{template.strip()}\n\nconversation_id: conv_1"
    assert reply_prompt(_texts_with(reply_turn_template=template), "conv_1") == expected
    assert reply_prompt({**texts_body(), "reply_turn_template": template}, "conv_1") == expected


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
