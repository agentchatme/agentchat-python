"""The engine surfaces on the client: ``inbox.tray``, ``decisions.record``,
``conversations.mark_read``, ``texts.get`` — URL, body, timeout and result
shape, sync and async."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

from agentchatme import AgentChatClient, AsyncAgentChatClient, CallOptions, NotFoundError
from agentchatme.types import (
    INBOX_TRAY_MAX_LIMIT,
    INBOX_WAIT_MAX_MS,
    InboxTray,
    ReplyDecision,
    Texts,
)

FIXTURES = Path(__file__).resolve().parent.parent / "conformance" / "v1"
BASE = "https://api.test"

SAMPLE_TRAY: dict[str, Any] = json.loads((FIXTURES / "tray.sample.json").read_text(encoding="utf-8"))
EMPTY_TRAY: dict[str, Any] = {
    **SAMPLE_TRAY,
    "conversations": [],
    "deferred": 0,
    "truncated": False,
    "waited_ms": None,
}
DECISION_ROW: dict[str, Any] = {
    "id": "dec_01",
    "conversation_id": "conv_direct0first01",
    "decision": "dismiss",
    "category": "acknowledgement",
    "through_message_id": "msg_nova0first00001a",
    "defer_until": None,
    "created_at": "2026-11-04T23:00:00.000Z",
}
TEXTS: dict[str, Any] = {
    "version": "2026-11-05",
    "manual": {"url": "https://agentchat.me/manual", "version": "2.1.2"},
    "wake_instruction": "Read your tray with agentchat_inbox.",
    "invite_prompt": "You have a pending group invite.",
    "identity_line": "You are @relay-bot on AgentChat.",
    "identity_line_template": "You are {handle} on AgentChat.",
}


def _capture(mock: respx.MockRouter, route: Any, status: int, body: Any) -> list[httpx.Request]:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(status, json=body)

    route.mock(side_effect=handler)
    return seen


def _read_timeout(request: httpx.Request) -> float | None:
    timeout = request.extensions.get("timeout")
    assert isinstance(timeout, dict)
    read = timeout.get("read")
    return float(read) if read is not None else None


# ─────────────── inbox.tray ───────────────


def test_tray_gets_inbox_with_no_query_for_a_plain_read() -> None:
    with respx.mock(base_url=BASE) as mock:
        seen = _capture(mock, mock.get("/v1/inbox"), 200, EMPTY_TRAY)
        with AgentChatClient(api_key="sk_test", base_url=BASE) as client:
            tray = client.inbox.tray()
    assert isinstance(tray, InboxTray)
    assert tray.conversations == []
    assert tray.you == "@relay-bot"
    assert str(seen[0].url) == f"{BASE}/v1/inbox"
    assert seen[0].method == "GET"
    assert _read_timeout(seen[0]) == 30.0  # the transport default, untouched


def test_tray_passes_wait_ms_and_limit_and_raises_the_timeout_to_cover_the_wait() -> None:
    with respx.mock(base_url=BASE) as mock:
        seen = _capture(
            mock, mock.get(url__regex=r".*/v1/inbox.*"), 200, {**SAMPLE_TRAY, "waited_ms": 1200}
        )
        with AgentChatClient(api_key="sk_test", base_url=BASE) as client:
            tray = client.inbox.tray(wait_ms=25_000, limit=5)
    assert tray.waited_ms == 1200
    assert len(tray.conversations) == 4
    assert str(seen[0].url) == f"{BASE}/v1/inbox?wait_ms=25000&limit=5"
    assert _read_timeout(seen[0]) == 35.0  # wait + 10 s grace


def test_tray_honours_an_explicit_timeout_over_the_derived_one() -> None:
    with respx.mock(base_url=BASE) as mock:
        seen = _capture(mock, mock.get(url__regex=r".*/v1/inbox.*"), 200, EMPTY_TRAY)
        with AgentChatClient(api_key="sk_test", base_url=BASE) as client:
            client.inbox.tray(wait_ms=INBOX_WAIT_MAX_MS, opts=CallOptions(timeout_ms=2_000))
    assert _read_timeout(seen[0]) == 2.0


@pytest.mark.parametrize(
    ("kwargs", "error"),
    [
        ({"wait_ms": INBOX_WAIT_MAX_MS + 1}, ValueError),
        ({"wait_ms": -1}, ValueError),
        ({"wait_ms": 1.5}, TypeError),
        ({"limit": 0}, ValueError),
        ({"limit": INBOX_TRAY_MAX_LIMIT + 1}, ValueError),
    ],
)
def test_tray_rejects_out_of_range_arguments_before_the_wire(
    kwargs: dict[str, Any], error: type[Exception]
) -> None:
    with respx.mock(base_url=BASE, assert_all_called=False) as mock:
        route = mock.get(url__regex=r".*/v1/inbox.*").mock(return_value=httpx.Response(200, json=EMPTY_TRAY))
        with AgentChatClient(api_key="sk_test", base_url=BASE) as client, pytest.raises(error):
            client.inbox.tray(**kwargs)
        assert route.call_count == 0


def test_tray_card_fields_are_typed() -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.get("/v1/inbox").mock(return_value=httpx.Response(200, json=SAMPLE_TRAY))
        with AgentChatClient(api_key="sk_test", base_url=BASE) as client:
            tray = client.inbox.tray()
    first, crew = tray.conversations[0], tray.conversations[1]
    assert first.counterparty is not None and first.counterparty.handle == "@nova-scout"
    assert first.anchor_message_id == "msg_nova0first00001a"
    assert first.pace.last_10m == 1
    assert crew.group is not None and crew.group.name == "launch-crew"
    assert crew.your_last_decision is not None and crew.your_last_decision.decision == "dismiss"
    gone = tray.conversations[3]
    assert gone.group is not None and gone.group.deleted is not None
    assert gone.group.deleted.by == SAMPLE_TRAY["conversations"][-1]["group"]["deleted"]["by"]


def test_tray_tolerates_server_additive_fields() -> None:
    tray = dict(SAMPLE_TRAY)
    card = dict(tray["conversations"][0])
    card["muted"] = True
    card["reminder"] = {"step": 2, "fired_at": "2026-11-05T10:00:00.000Z"}
    card["brand_new_field"] = {"x": 1}
    tray["conversations"] = [card]
    tray["another_new_top_level"] = 7
    with respx.mock(base_url=BASE) as mock:
        mock.get("/v1/inbox").mock(return_value=httpx.Response(200, json=tray))
        with AgentChatClient(api_key="sk_test", base_url=BASE) as client:
            parsed = client.inbox.tray()
    only = parsed.conversations[0]
    assert only.muted is True
    assert only.reminder is not None and only.reminder.step == 2
    assert parsed.model_dump(mode="json", exclude_unset=True) == tray


# ─────────────── decisions.record ───────────────


def test_record_posts_the_verdict_with_the_anchor_and_unwraps_the_decision() -> None:
    with respx.mock(base_url=BASE) as mock:
        seen = _capture(
            mock,
            mock.post("/v1/conversations/conv_direct0first01/decisions"),
            201,
            {"decision": DECISION_ROW},
        )
        with AgentChatClient(api_key="sk_test", base_url=BASE) as client:
            decision = client.decisions.record(
                "conv_direct0first01",
                decision="dismiss",
                category="acknowledgement",
                through_message_id="msg_nova0first00001a",
            )
    assert isinstance(decision, ReplyDecision)
    assert decision.id == "dec_01"
    assert decision.through_message_id == "msg_nova0first00001a"
    assert json.loads(seen[0].content) == {
        "decision": "dismiss",
        "category": "acknowledgement",
        "through_message_id": "msg_nova0first00001a",
    }


def test_record_leaves_optional_fields_off_the_wire_when_unset() -> None:
    with respx.mock(base_url=BASE) as mock:
        seen = _capture(
            mock, mock.post(url__regex=r".*/decisions$"), 201, {"decision": DECISION_ROW}
        )
        with AgentChatClient(api_key="sk_test", base_url=BASE) as client:
            client.decisions.record("conv_direct0first01", decision="reply")
    assert json.loads(seen[0].content) == {"decision": "reply"}


def test_record_url_encodes_the_conversation_id_and_rejects_unknown_verdicts() -> None:
    with respx.mock(base_url=BASE) as mock:
        seen = _capture(mock, mock.post(url__regex=r".*/decisions$"), 201, {"decision": DECISION_ROW})
        with AgentChatClient(api_key="sk_test", base_url=BASE) as client:
            client.decisions.record("conv/odd id", decision="defer")
            with pytest.raises(ValueError, match="decision must be one of"):
                client.decisions.record("conv_x", decision="maybe")  # type: ignore[arg-type]
    assert str(seen[0].url) == f"{BASE}/v1/conversations/conv%2Fodd%20id/decisions"
    assert len(seen) == 1


# ─────────────── conversations.mark_read ───────────────


def test_mark_read_posts_the_anchor_and_returns_the_count() -> None:
    with respx.mock(base_url=BASE) as mock:
        seen = _capture(
            mock, mock.post("/v1/conversations/conv_direct0first01/read"), 200, {"read": 1}
        )
        with AgentChatClient(api_key="sk_test", base_url=BASE) as client:
            read = client.conversations.mark_read("conv_direct0first01", "msg_nova0first00001a")
    assert read == 1
    assert json.loads(seen[0].content) == {"through_message_id": "msg_nova0first00001a"}


def test_mark_read_surfaces_a_foreign_anchor_as_not_found() -> None:
    with respx.mock(base_url=BASE) as mock:
        mock.post(url__regex=r".*/read$").mock(
            return_value=httpx.Response(
                404, json={"code": "MESSAGE_NOT_FOUND", "message": "Message not found"}
            )
        )
        with AgentChatClient(api_key="sk_test", base_url=BASE) as client:
            with pytest.raises(NotFoundError):
                client.conversations.mark_read("conv_direct0first01", "msg_doesnotexist00")
            with pytest.raises(ValueError):
                client.conversations.mark_read("conv_direct0first01", "")


# ─────────────── texts.get ───────────────


def test_texts_get_fetches_for_the_runtime() -> None:
    with respx.mock(base_url=BASE) as mock:
        seen = _capture(mock, mock.get(url__regex=r".*/v1/texts.*"), 200, TEXTS)
        with AgentChatClient(api_key="sk_test", base_url=BASE) as client:
            texts = client.texts.get(runtime="hermes")
            with pytest.raises(ValueError):
                client.texts.get(runtime="")
    assert isinstance(texts, Texts)
    assert texts.version == "2026-11-05"
    assert texts.manual.version == "2.1.2"
    assert texts.identity_line_template.startswith("You are {handle}")
    assert str(seen[0].url) == f"{BASE}/v1/texts?runtime=hermes"


# ─────────────── async counterparts ───────────────


@pytest.mark.asyncio
async def test_async_engine_surfaces_speak_the_same_wire() -> None:
    with respx.mock(base_url=BASE) as mock:
        tray_seen = _capture(
            mock, mock.get(url__regex=r".*/v1/inbox.*"), 200, {**SAMPLE_TRAY, "waited_ms": 400}
        )
        rec_seen = _capture(
            mock, mock.post(url__regex=r".*/decisions$"), 201, {"decision": DECISION_ROW}
        )
        read_seen = _capture(mock, mock.post(url__regex=r".*/read$"), 200, {"read": 2})
        texts_seen = _capture(mock, mock.get(url__regex=r".*/v1/texts.*"), 200, TEXTS)
        async with AsyncAgentChatClient(api_key="sk_test", base_url=BASE) as client:
            tray = await client.inbox.tray(wait_ms=30_000, limit=10)
            decision = await client.decisions.record(
                "conv_direct0first01",
                decision="reply",
                category="open_request",
                through_message_id=tray.conversations[0].anchor_message_id,
            )
            read = await client.conversations.mark_read("conv_group0crew0001", "msg_crew0thanks0001b")
            texts = await client.texts.get(runtime="openclaw")

    assert str(tray_seen[0].url) == f"{BASE}/v1/inbox?wait_ms=30000&limit=10"
    assert _read_timeout(tray_seen[0]) == 40.0
    assert tray.waited_ms == 400
    assert json.loads(rec_seen[0].content) == {
        "decision": "reply",
        "category": "open_request",
        "through_message_id": "msg_nova0first00001a",
    }
    assert decision.conversation_id == "conv_direct0first01"
    assert json.loads(read_seen[0].content) == {"through_message_id": "msg_crew0thanks0001b"}
    assert read == 2
    assert str(texts_seen[0].url) == f"{BASE}/v1/texts?runtime=openclaw"
    assert texts.wake_instruction == TEXTS["wake_instruction"]
