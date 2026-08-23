"""Smoke tests for :class:`AgentChatClient` / :class:`AsyncAgentChatClient`.

The HTTP transport and error mapping are already thoroughly tested
elsewhere. These tests just verify the high-level client dispatches the
right method + URL + body for a representative subset of the API surface,
and that backlog warnings from ``X-Backlog-Warning`` are parsed correctly.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
import respx

from agentchatme import (
    VERSION,
    AgentChatClient,
    AgentChatError,
    AsyncAgentChatClient,
    EmailExhaustedError,
    EmailLimitReachedError,
    HandleRequiredError,
    RecipientBackloggedError,
    SystemAgentProtectedError,
)


def test_send_message_posts_to_messages() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["url"] = str(request.url)
        captured["body"] = json.loads(request.content.decode())
        return httpx.Response(200, json={"id": "msg_1"})

    with respx.mock(base_url="https://api.test") as mock:
        mock.post("/v1/messages").mock(side_effect=handler)
        client = AgentChatClient(api_key="sk_test", base_url="https://api.test")
        try:
            result = client.send_message(to="@alice", content="hello")
        finally:
            client.close()

    assert captured["method"] == "POST"
    assert captured["body"]["to"] == "@alice"
    assert captured["body"]["content"] == "hello"
    # send_message auto-generates a client_msg_id for dedup.
    assert "client_msg_id" in captured["body"]
    assert result.message == {"id": "msg_1"}


def test_default_client_identity_headers() -> None:
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["client"] = request.headers.get("x-agentchat-client", "")
        captured["version"] = request.headers.get(
            "x-agentchat-client-version", ""
        )
        return httpx.Response(200, json=[])

    with respx.mock(base_url="https://api.test") as mock:
        mock.get("/v1/conversations").mock(side_effect=handler)
        client = AgentChatClient(api_key="sk_test", base_url="https://api.test")
        try:
            client.list_conversations()
        finally:
            client.close()

    assert captured == {"client": "python_sdk", "version": VERSION}


def test_integration_can_override_client_identity() -> None:
    from agentchatme import AgentChatClientIdentity

    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["client"] = request.headers.get("x-agentchat-client", "")
        captured["version"] = request.headers.get(
            "x-agentchat-client-version", ""
        )
        return httpx.Response(200, json=[])

    with respx.mock(base_url="https://api.test") as mock:
        mock.get("/v1/conversations").mock(side_effect=handler)
        client = AgentChatClient(
            api_key="sk_test",
            base_url="https://api.test",
            client_identity=AgentChatClientIdentity(
                name="hermes",
                version="4.2.0",
            ),
        )
        try:
            client.list_conversations()
        finally:
            client.close()

    assert captured == {"client": "hermes", "version": "4.2.0"}


def test_get_agent_hits_correct_url() -> None:
    with respx.mock(base_url="https://api.test") as mock:
        route = mock.get("/v1/agents/@alice").mock(
            return_value=httpx.Response(200, json={"handle": "@alice", "status": "active"})
        )
        client = AgentChatClient(api_key="sk_test", base_url="https://api.test")
        try:
            agent = client.get_agent("@alice")
        finally:
            client.close()
        assert agent["handle"] == "@alice"
        assert route.called


def test_get_messages_query_string() -> None:
    captured_url: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured_url.append(str(request.url))
        return httpx.Response(200, json=[])

    with respx.mock(base_url="https://api.test") as mock:
        mock.get(url__regex=r".*/v1/messages/c1.*").mock(side_effect=handler)
        client = AgentChatClient(api_key="sk_test", base_url="https://api.test")
        try:
            client.get_messages("c1", limit=25, after_seq=7)
        finally:
            client.close()
    url = captured_url[0]
    assert "limit=25" in url
    assert "after_seq=7" in url


def test_mute_status_missing_returns_none() -> None:
    with respx.mock(base_url="https://api.test") as mock:
        mock.get(url__regex=r".*/v1/mutes/agent/%40alice$").mock(
            return_value=httpx.Response(
                404, json={"code": "AGENT_NOT_FOUND", "message": "not muted"}
            )
        )
        client = AgentChatClient(api_key="sk_test", base_url="https://api.test")
        try:
            mute = client.get_agent_mute_status("@alice")
        finally:
            client.close()
    # 404 → None rather than raising (the "not muted" signal).
    assert mute is None


def test_recipient_backlogged_error_has_typed_details() -> None:
    with respx.mock(base_url="https://api.test") as mock:
        mock.post("/v1/messages").mock(
            return_value=httpx.Response(
                429,
                json={
                    "code": "RECIPIENT_BACKLOGGED",
                    "message": "full",
                    "details": {
                        "recipient_handle": "@alice",
                        "undelivered_count": 9999,
                    },
                },
            )
        )
        client = AgentChatClient(api_key="sk_test", base_url="https://api.test")
        try:
            with pytest.raises(RecipientBackloggedError) as exc_info:
                client.send_message(to="@alice", content="hi")
        finally:
            client.close()
        assert exc_info.value.recipient_handle == "@alice"
        assert exc_info.value.undelivered_count == 9999


def test_get_me_hits_self_endpoint() -> None:
    with respx.mock(base_url="https://api.test") as mock:
        route = mock.get("/v1/agents/me").mock(
            return_value=httpx.Response(
                200,
                json={
                    "id": "agt_1",
                    "handle": "alice",
                    "email": "alice@example.com",
                    "status": "active",
                    "paused_by_owner": "none",
                    "settings": {
                        "inbox_mode": "open",
                        "group_invite_policy": "open",
                    },
                    "is_system": False,
                    "created_at": "2026-01-01T00:00:00Z",
                    "updated_at": "2026-01-01T00:00:00Z",
                },
            )
        )
        client = AgentChatClient(api_key="sk_test", base_url="https://api.test")
        try:
            me = client.get_me()
        finally:
            client.close()
    assert route.called
    assert me["handle"] == "alice"
    assert me["is_system"] is False


def test_mark_as_read_posts() -> None:
    with respx.mock(base_url="https://api.test") as mock:
        route = mock.post("/v1/messages/msg_1/read").mock(
            return_value=httpx.Response(200, json={"ok": True})
        )
        client = AgentChatClient(api_key="sk_test", base_url="https://api.test")
        try:
            client.mark_as_read("msg_1")
        finally:
            client.close()
    assert route.called


def test_get_conversation_participants() -> None:
    with respx.mock(base_url="https://api.test") as mock:
        route = mock.get("/v1/conversations/c1/participants").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {"handle": "alice", "display_name": "Alice"},
                    {"handle": "bob", "display_name": None},
                ],
            )
        )
        client = AgentChatClient(api_key="sk_test", base_url="https://api.test")
        try:
            participants = client.get_conversation_participants("c1")
        finally:
            client.close()
    assert route.called
    assert [p["handle"] for p in participants] == ["alice", "bob"]


def test_hide_conversation_deletes() -> None:
    with respx.mock(base_url="https://api.test") as mock:
        route = mock.delete("/v1/conversations/c1").mock(
            return_value=httpx.Response(200, json={"ok": True})
        )
        client = AgentChatClient(api_key="sk_test", base_url="https://api.test")
        try:
            client.hide_conversation("c1")
        finally:
            client.close()
    assert route.called


def test_set_group_avatar_uploads_raw_bytes() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["method"] = request.method
        captured["body"] = request.content
        captured["content_type"] = request.headers.get("content-type")
        return httpx.Response(
            200, json={"avatar_key": "k1", "avatar_url": "https://cdn.test/k1"}
        )

    with respx.mock(base_url="https://api.test") as mock:
        mock.put("/v1/groups/grp_1/avatar").mock(side_effect=handler)
        client = AgentChatClient(api_key="sk_test", base_url="https://api.test")
        try:
            client.set_group_avatar("grp_1", b"\x89PNG\r\n", content_type="image/png")
        finally:
            client.close()
    assert captured["method"] == "PUT"
    assert captured["body"] == b"\x89PNG\r\n"
    assert captured["content_type"] == "image/png"


def test_remove_group_avatar_deletes() -> None:
    with respx.mock(base_url="https://api.test") as mock:
        route = mock.delete("/v1/groups/grp_1/avatar").mock(
            return_value=httpx.Response(200, json={"ok": True})
        )
        client = AgentChatClient(api_key="sk_test", base_url="https://api.test")
        try:
            client.remove_group_avatar("grp_1")
        finally:
            client.close()
    assert route.called


def test_get_attachment_download_url_captures_location_without_following() -> None:
    """The 302 must NOT be followed — Bearer would leak to the storage URL."""
    storage_url = "https://storage.test/signed/key123?sig=abc"
    captured_calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured_calls.append(str(request.url))
        return httpx.Response(302, headers={"location": storage_url})

    with respx.mock(base_url="https://api.test") as mock:
        mock.get("/v1/attachments/att_1").mock(side_effect=handler)
        client = AgentChatClient(api_key="sk_test", base_url="https://api.test")
        try:
            url = client.get_attachment_download_url("att_1")
        finally:
            client.close()

    assert url == storage_url
    # Exactly one call — the redirect was captured, not chased to the storage URL.
    assert len(captured_calls) == 1
    assert captured_calls[0].endswith("/v1/attachments/att_1")


def test_get_attachment_download_url_raises_when_no_location() -> None:
    with respx.mock(base_url="https://api.test") as mock:
        mock.get("/v1/attachments/att_1").mock(
            return_value=httpx.Response(204)  # 2xx but no location → must raise
        )
        client = AgentChatClient(api_key="sk_test", base_url="https://api.test")
        try:
            with pytest.raises(AgentChatError):
                client.get_attachment_download_url("att_1")
        finally:
            client.close()


def test_sync_passes_after_in_query() -> None:
    captured_url: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured_url.append(str(request.url))
        return httpx.Response(200, json={"envelopes": []})

    with respx.mock(base_url="https://api.test") as mock:
        mock.get(url__regex=r".*/v1/messages/sync.*").mock(side_effect=handler)
        client = AgentChatClient(api_key="sk_test", base_url="https://api.test")
        try:
            client.sync(limit=50, after=12345)
        finally:
            client.close()
    url = captured_url[0]
    assert "limit=50" in url
    assert "after=12345" in url


def test_system_agent_protected_error_via_block() -> None:
    """A 409 SYSTEM_AGENT_PROTECTED should surface as the typed subclass."""
    with respx.mock(base_url="https://api.test") as mock:
        mock.post("/v1/contacts/chatfather/block").mock(
            return_value=httpx.Response(
                409,
                json={
                    "code": "SYSTEM_AGENT_PROTECTED",
                    "message": "cannot block a system agent",
                },
            )
        )
        client = AgentChatClient(api_key="sk_test", base_url="https://api.test")
        try:
            with pytest.raises(SystemAgentProtectedError) as exc_info:
                client.block_agent("chatfather")
        finally:
            client.close()
    assert exc_info.value.code == "SYSTEM_AGENT_PROTECTED"
    assert exc_info.value.status == 409


# ─────────────── Registration: email policy ───────────────


def test_register_email_limit_reached_is_typed() -> None:
    """409 EMAIL_LIMIT_REACHED from /v1/register → EmailLimitReachedError with the cap."""
    with respx.mock(base_url="https://api.test") as mock:
        mock.post("/v1/register").mock(
            return_value=httpx.Response(
                409,
                json={
                    "code": "EMAIL_LIMIT_REACHED",
                    "message": "This email already backs 10 active agents.",
                    "details": {"limit": 10},
                },
            )
        )
        with pytest.raises(EmailLimitReachedError) as exc_info:
            AgentChatClient.register(
                email="you@example.com", handle="my-agent", base_url="https://api.test"
            )
    assert exc_info.value.status == 409
    assert exc_info.value.limit == 10


def test_register_email_exhausted_is_typed() -> None:
    with respx.mock(base_url="https://api.test") as mock:
        mock.post("/v1/register").mock(
            return_value=httpx.Response(
                409,
                json={
                    "code": "EMAIL_EXHAUSTED",
                    "message": "This email has reached the maximum of 30 account registrations.",
                    "details": {"limit": 30},
                },
            )
        )
        with pytest.raises(EmailExhaustedError) as exc_info:
            AgentChatClient.register(
                email="you@example.com", handle="my-agent", base_url="https://api.test"
            )
    assert exc_info.value.limit == 30


# ─────────────── Recovery: handle + email ───────────────


def _capture_post(mock: respx.MockRouter, path: str, response: dict[str, Any]) -> dict[str, Any]:
    """Route ``POST path`` to a 200 ``response`` and return the captured JSON body."""
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.update(json.loads(request.content.decode()))
        return httpx.Response(200, json=response)

    mock.post(path).mock(side_effect=handler)
    return captured


def test_recover_sends_handle_with_email() -> None:
    with respx.mock(base_url="https://api.test") as mock:
        body = _capture_post(
            mock, "/v1/agents/recover", {"pending_id": "pnd_1", "message": "If it exists…"}
        )
        result = AgentChatClient.recover(
            "you@example.com", handle="my-agent", base_url="https://api.test"
        )
    assert body == {"email": "you@example.com", "handle": "my-agent"}
    assert result == {"pending_id": "pnd_1", "message": "If it exists…"}


def test_recover_omits_handle_key_when_not_given() -> None:
    """Legacy email-only call: the key must be absent, not ``null`` — the
    server schema is optional, not nullable."""
    with respx.mock(base_url="https://api.test") as mock:
        body = _capture_post(
            mock, "/v1/agents/recover", {"pending_id": "pnd_1", "message": "If it exists…"}
        )
        result = AgentChatClient.recover("you@example.com", base_url="https://api.test")
    assert body == {"email": "you@example.com"}
    assert "handle" not in body
    assert result["pending_id"] == "pnd_1"


def test_recover_handle_is_keyword_only() -> None:
    # Locks the signature: a positional second argument must not silently
    # become the handle (or anything else).
    with pytest.raises(TypeError):
        AgentChatClient.recover("you@example.com", "my-agent")  # type: ignore[misc]
    with pytest.raises(TypeError):
        AsyncAgentChatClient.recover("you@example.com", "my-agent")  # type: ignore[misc]


def test_recover_verify_handle_required_is_typed() -> None:
    """409 HANDLE_REQUIRED → HandleRequiredError listing the live sibling handles."""
    with respx.mock(base_url="https://api.test") as mock:
        mock.post("/v1/agents/recover/verify").mock(
            return_value=httpx.Response(
                409,
                json={
                    "code": "HANDLE_REQUIRED",
                    "message": "This email backs more than one agent.",
                    "details": {"handles": ["alpha-bot", "beta-bot"]},
                },
            )
        )
        with pytest.raises(HandleRequiredError) as exc_info:
            AgentChatClient.recover_verify("pnd_1", "123456", base_url="https://api.test")
    assert exc_info.value.status == 409
    assert exc_info.value.handles == ["alpha-bot", "beta-bot"]


def test_recover_verify_success_returns_handle_key_client() -> None:
    with respx.mock(base_url="https://api.test") as mock:
        mock.post("/v1/agents/recover/verify").mock(
            return_value=httpx.Response(
                200, json={"handle": "my-agent", "api_key": "ac_new", "message": "ok"}
            )
        )
        handle, api_key, client = AgentChatClient.recover_verify(
            "pnd_1", "123456", base_url="https://api.test"
        )
        client.close()
    assert (handle, api_key) == ("my-agent", "ac_new")
    assert client.base_url == "https://api.test"


# ─────────────── Async counterpart ───────────────


@pytest.mark.asyncio
async def test_async_recover_sends_handle_with_email() -> None:
    with respx.mock(base_url="https://api.test") as mock:
        body = _capture_post(
            mock, "/v1/agents/recover", {"pending_id": "pnd_1", "message": "If it exists…"}
        )
        result = await AsyncAgentChatClient.recover(
            "you@example.com", handle="my-agent", base_url="https://api.test"
        )
    assert body == {"email": "you@example.com", "handle": "my-agent"}
    assert result["pending_id"] == "pnd_1"


@pytest.mark.asyncio
async def test_async_recover_omits_handle_key_when_not_given() -> None:
    with respx.mock(base_url="https://api.test") as mock:
        body = _capture_post(
            mock, "/v1/agents/recover", {"pending_id": "pnd_1", "message": "If it exists…"}
        )
        await AsyncAgentChatClient.recover("you@example.com", base_url="https://api.test")
    assert body == {"email": "you@example.com"}


@pytest.mark.asyncio
async def test_async_recover_verify_handle_required_is_typed() -> None:
    with respx.mock(base_url="https://api.test") as mock:
        mock.post("/v1/agents/recover/verify").mock(
            return_value=httpx.Response(
                409,
                json={
                    "code": "HANDLE_REQUIRED",
                    "message": "This email backs more than one agent.",
                    "details": {"handles": ["alpha-bot", "beta-bot"]},
                },
            )
        )
        with pytest.raises(HandleRequiredError) as exc_info:
            await AsyncAgentChatClient.recover_verify(
                "pnd_1", "123456", base_url="https://api.test"
            )
    assert exc_info.value.handles == ["alpha-bot", "beta-bot"]


@pytest.mark.asyncio
async def test_async_send_message_and_context_manager() -> None:
    with respx.mock(base_url="https://api.test") as mock:
        mock.post("/v1/messages").mock(return_value=httpx.Response(200, json={"id": "msg_1"}))
        async with AsyncAgentChatClient(
            api_key="sk_test", base_url="https://api.test"
        ) as client:
            result = await client.send_message(to="@alice", content="hello")
        assert result.message == {"id": "msg_1"}


@pytest.mark.asyncio
async def test_async_get_attachment_download_url() -> None:
    storage_url = "https://storage.test/signed/key456?sig=xyz"

    with respx.mock(base_url="https://api.test") as mock:
        mock.get("/v1/attachments/att_2").mock(
            return_value=httpx.Response(302, headers={"location": storage_url})
        )
        async with AsyncAgentChatClient(
            api_key="sk_test", base_url="https://api.test"
        ) as client:
            url = await client.get_attachment_download_url("att_2")
    assert url == storage_url


@pytest.mark.asyncio
async def test_async_get_me() -> None:
    with respx.mock(base_url="https://api.test") as mock:
        mock.get("/v1/agents/me").mock(
            return_value=httpx.Response(
                200,
                json={
                    "id": "agt_1",
                    "handle": "alice",
                    "email": "alice@example.com",
                    "status": "active",
                    "paused_by_owner": "none",
                    "settings": {
                        "inbox_mode": "open",
                        "group_invite_policy": "open",
                    },
                    "is_system": False,
                    "created_at": "2026-01-01T00:00:00Z",
                    "updated_at": "2026-01-01T00:00:00Z",
                },
            )
        )
        async with AsyncAgentChatClient(
            api_key="sk_test", base_url="https://api.test"
        ) as client:
            me = await client.get_me()
    assert me["handle"] == "alice"


@pytest.mark.asyncio
async def test_async_list_conversations() -> None:
    with respx.mock(base_url="https://api.test") as mock:
        mock.get("/v1/conversations").mock(
            return_value=httpx.Response(
                200, json=[{"id": "c1"}, {"id": "c2"}]
            )
        )
        async with AsyncAgentChatClient(
            api_key="sk_test", base_url="https://api.test"
        ) as client:
            convs = await client.list_conversations()
        assert [c["id"] for c in convs] == ["c1", "c2"]
