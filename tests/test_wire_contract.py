"""Wire-contract tests for ``/v1/messages/sync`` + ``/v1/messages/sync/ack``.

These lock the SDK to the shape production actually speaks (verified
live-fire 2026-07-12, mirrored from the TypeScript reference wire client):

- ``GET /v1/messages/sync`` → **bare JSON array** of message rows, oldest
  first. ``delivery_id`` is an **opaque, nullable string** (``del_<32hex>``)
  — positional cursor semantics, never compared numerically.
- ``POST /v1/messages/sync/ack`` ``{"last_delivery_id": "<str>"}`` →
  ``{"acked": <int>}``.

SDK v1.0.2/1.0.3 typed this path as ``{"envelopes": [...]}`` with numeric
ids — a silent zero-row drain. These tests exist so that shape can never
come back.
"""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx
import pytest
import respx

from agentchatme import (
    AgentChatClient,
    AsyncAgentChatClient,
    SyncRow,
    last_sync_delivery_id,
)

# A realistic production batch: full row, minimal row, null-delivery row,
# and a server-additive field the SDK has never heard of.
WIRE_SYNC_BATCH: list[dict[str, Any]] = [
    {
        "id": "msg_01J2ZK3V8Q4N5P6R7S8T9U0V1W",
        "conversation_id": "conv_7f3a9b2c4d5e",
        "delivery_id": "del_9f86d081884c7d659a2feaa0c55ad015",
        "sender": "mike-asst",
        "type": "text",
        "content": {"text": "hey — did the deploy land?"},
        "created_at": "2026-07-12T18:04:11.482Z",
        "seq": 41,
        "status": "stored",
    },
    {
        "id": "msg_01J2ZK4W9R5P6Q7S8T9U0V1W2X",
        "conversation_id": "conv_7f3a9b2c4d5e",
        "delivery_id": "del_ab56b4d92b40713acc5af89985d4b786",
        "sender": "vellum-noir",
        "type": "structured",
        "content": {"data": {"kind": "deploy_report", "ok": True}},
        "created_at": "2026-07-12T18:05:02.017Z",
        "seq": 42,
        # Server-additive field this SDK release doesn't know about —
        # must pass through untouched, never break parsing.
        "delivery_attempts": 2,
    },
    {
        "id": "msg_01J2ZK5X0S6Q7R8T9U0V1W2X3Y",
        "conversation_id": "grp_O6EB0CSFpmOuCOQD",
        "delivery_id": None,
        "sender": "tessera-rho",
        "type": "text",
        "content": {"text": "group fanout row without an envelope"},
        "created_at": "2026-07-12T18:05:40.900Z",
        "seq": 7,
    },
]


# ─────────────── GET /v1/messages/sync ───────────────


def test_sync_returns_bare_array_rows_untouched() -> None:
    with respx.mock(base_url="https://api.test") as mock:
        mock.get(url__regex=r".*/v1/messages/sync.*").mock(
            return_value=httpx.Response(200, json=WIRE_SYNC_BATCH)
        )
        client = AgentChatClient(api_key="sk_test", base_url="https://api.test")
        try:
            rows = client.sync()
        finally:
            client.close()

    assert isinstance(rows, list)
    assert rows == WIRE_SYNC_BATCH
    # Unknown fields tolerated, string cursor preserved, null preserved.
    assert rows[1]["delivery_attempts"] == 2  # type: ignore[typeddict-item]
    assert rows[0]["delivery_id"] == "del_9f86d081884c7d659a2feaa0c55ad015"
    assert rows[2]["delivery_id"] is None


def test_sync_passes_string_after_cursor_and_limit() -> None:
    captured_url: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured_url.append(str(request.url))
        return httpx.Response(200, json=[])

    with respx.mock(base_url="https://api.test") as mock:
        mock.get(url__regex=r".*/v1/messages/sync.*").mock(side_effect=handler)
        client = AgentChatClient(api_key="sk_test", base_url="https://api.test")
        try:
            rows = client.sync(limit=100, after="del_9f86d081884c7d659a2feaa0c55ad015")
        finally:
            client.close()

    assert rows == []
    url = captured_url[0]
    assert "limit=100" in url
    assert "after=del_9f86d081884c7d659a2feaa0c55ad015" in url


def test_sync_non_array_payload_is_empty_batch_with_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # The pre-1.0.31 envelope-object shape (or any other non-array body) is
    # a contract violation: warn + empty so drain loops terminate instead
    # of spinning or crashing.
    with respx.mock(base_url="https://api.test") as mock:
        mock.get(url__regex=r".*/v1/messages/sync.*").mock(
            return_value=httpx.Response(200, json={"envelopes": []})
        )
        client = AgentChatClient(api_key="sk_test", base_url="https://api.test")
        try:
            with caplog.at_level(logging.WARNING, logger="agentchat.client"):
                rows = client.sync()
        finally:
            client.close()

    assert rows == []
    assert any("non-array" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_async_sync_returns_bare_array() -> None:
    with respx.mock(base_url="https://api.test") as mock:
        mock.get(url__regex=r".*/v1/messages/sync.*").mock(
            return_value=httpx.Response(200, json=WIRE_SYNC_BATCH)
        )
        async with AsyncAgentChatClient(
            api_key="sk_test", base_url="https://api.test"
        ) as client:
            rows = await client.sync(limit=100)

    assert rows == WIRE_SYNC_BATCH


# ─────────────── POST /v1/messages/sync/ack ───────────────


def test_sync_ack_posts_string_cursor_and_parses_acked_count() -> None:
    captured_body: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured_body.append(json.loads(request.content.decode()))
        return httpx.Response(200, json={"acked": 2})

    with respx.mock(base_url="https://api.test") as mock:
        mock.post("/v1/messages/sync/ack").mock(side_effect=handler)
        client = AgentChatClient(api_key="sk_test", base_url="https://api.test")
        try:
            result = client.sync_ack("del_ab56b4d92b40713acc5af89985d4b786")
        finally:
            client.close()

    assert captured_body == [
        {"last_delivery_id": "del_ab56b4d92b40713acc5af89985d4b786"}
    ]
    assert result == {"acked": 2}


@pytest.mark.asyncio
async def test_async_sync_ack_posts_string_cursor() -> None:
    captured_body: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured_body.append(json.loads(request.content.decode()))
        return httpx.Response(200, json={"acked": 1})

    with respx.mock(base_url="https://api.test") as mock:
        mock.post("/v1/messages/sync/ack").mock(side_effect=handler)
        async with AsyncAgentChatClient(
            api_key="sk_test", base_url="https://api.test"
        ) as client:
            result = await client.sync_ack("del_9f86d081884c7d659a2feaa0c55ad015")

    assert captured_body[0]["last_delivery_id"] == (
        "del_9f86d081884c7d659a2feaa0c55ad015"
    )
    assert result["acked"] == 1


@pytest.mark.parametrize("bad_cursor", [42, "", None, 41.0])
def test_sync_ack_rejects_non_string_cursors(bad_cursor: Any) -> None:
    # Legacy (<=1.0.3) callers passed numeric ids — fail fast client-side
    # with a migration hint rather than a server-side VALIDATION_ERROR.
    client = AgentChatClient(api_key="sk_test", base_url="https://api.test")
    try:
        with pytest.raises(TypeError, match="non-empty string cursor"):
            client.sync_ack(bad_cursor)
    finally:
        client.close()


@pytest.mark.asyncio
async def test_async_sync_ack_rejects_non_string_cursors() -> None:
    async with AsyncAgentChatClient(
        api_key="sk_test", base_url="https://api.test"
    ) as client:
        with pytest.raises(TypeError, match="non-empty string cursor"):
            await client.sync_ack(42)  # type: ignore[arg-type]


# ─────────────── Realtime drain ↔ HTTP client integration ───────────────


@pytest.mark.asyncio
async def test_realtime_drain_speaks_real_wire_end_to_end() -> None:
    """Drain through a REAL ``AsyncAgentChatClient`` against the mocked
    production wire — catches signature/shape drift between the realtime
    drain and the HTTP client, the exact bug class that shipped in 1.0.2
    (drain unwrapped ``batch["envelopes"]`` that the wire never sends).
    """
    from agentchatme import RealtimeClient

    sync_urls: list[str] = []
    ack_bodies: list[dict[str, Any]] = []

    def sync_handler(request: httpx.Request) -> httpx.Response:
        sync_urls.append(str(request.url))
        return httpx.Response(200, json=WIRE_SYNC_BATCH)

    def ack_handler(request: httpx.Request) -> httpx.Response:
        ack_bodies.append(json.loads(request.content.decode()))
        return httpx.Response(200, json={"acked": 3})

    with respx.mock(base_url="https://api.test") as mock:
        mock.get(url__regex=r".*/v1/messages/sync(\?.*)?$").mock(
            side_effect=sync_handler
        )
        mock.post("/v1/messages/sync/ack").mock(side_effect=ack_handler)

        async with AsyncAgentChatClient(
            api_key="sk_test", base_url="https://api.test"
        ) as client:
            rt = RealtimeClient(
                api_key="sk_test",
                client=client,
                auto_drain_on_connect=False,
                reconnect=False,
            )
            handled: list[str] = []
            rt.on("message.new", lambda m: handled.append(m["payload"]["id"]))
            await rt.drain_offline_envelopes()
            await rt.disconnect()

    # Every wire row reached the handlers, through the real HTTP client.
    assert handled == [row["id"] for row in WIRE_SYNC_BATCH]
    # One page (3 rows < the drain's requested limit of 100 → short page).
    assert len(sync_urls) == 1
    assert "limit=100" in sync_urls[0]
    # Acked once, with the positional cursor: the LAST NON-NULL delivery_id
    # (row 3's null is skipped), as a string.
    assert ack_bodies == [
        {"last_delivery_id": "del_ab56b4d92b40713acc5af89985d4b786"}
    ]


# ─────────────── Cursor helper ───────────────


def test_last_sync_delivery_id_is_positional() -> None:
    rows: list[SyncRow] = [
        {"id": "m1", "conversation_id": "c1", "delivery_id": "del_zzz"},
        {"id": "m2", "conversation_id": "c1", "delivery_id": "del_aaa"},
        {"id": "m3", "conversation_id": "c1", "delivery_id": None},
    ]
    # Last non-null wins by POSITION — "del_aaa" despite sorting before
    # "del_zzz"; the trailing null is skipped, not treated as a reset.
    assert last_sync_delivery_id(rows) == "del_aaa"


def test_last_sync_delivery_id_handles_unackable_batches() -> None:
    assert last_sync_delivery_id([]) is None
    all_null: list[SyncRow] = [
        {"id": "m1", "conversation_id": "c1", "delivery_id": None},
        {"id": "m2", "conversation_id": "c1", "delivery_id": ""},
    ]
    # Nulls and empty strings are both unackable.
    assert last_sync_delivery_id(all_null) is None
