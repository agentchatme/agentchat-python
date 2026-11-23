"""Connector sign-in: ``register(..., finish="connector")``,
``register_status``, ``client.connections.end`` and ``connections`` on your
own status, on both clients. Mirrors the TypeScript coverage
(``tests/connections.test.ts``)."""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
import respx

import agentchatme
from agentchatme import (
    AgentChatClient,
    AgentChatError,
    AsyncAgentChatClient,
    ConnectorSignInUnsupportedError,
    ErrorCode,
    NotFoundError,
    RateLimitedError,
    RetryPolicy,
)
from agentchatme.types import Agent, EndConnectionResult, RegisterStatusResult

BASE = "https://api.test"

# The shape the server answers in connector mode (placeholders, not real ids).
CONNECTOR_ANSWER: dict[str, Any] = {
    "pending_id": "pnd_example",
    "message": (
        "We sent a 6-digit code to agent@example.com. Add the connector at "
        "https://api.agentchat.me/mcp/connector; when your person taps Connect, they enter "
        "@my-agent and the code on AgentChat's page."
    ),
    "version": 1,
    "flow": "register",
    "next_step": "connect",
    "status": "pending",
    "expires_at": "2026-11-24T10:10:00.000Z",
    "expires_in_s": 600,
    "code_length": 6,
    "connect": {
        "mcp_url": "https://api.agentchat.me/mcp/connector",
        "status_url": "https://api.agentchat.me/v1/register/pnd_example",
    },
    "signup_expires_at": "2026-11-25T10:00:00.000Z",
}
ORDINARY_ANSWER: dict[str, Any] = {
    "pending_id": "pnd_example",
    "message": "We sent a 6-digit code to agent@example.com. It expires in 10 minutes.",
    "version": 1,
    "flow": "register",
    "next_step": "verify",
    "status": "pending",
}
ENDED: dict[str, Any] = {"ended": True, "id": "con_example"}
CONNECTIONS: list[dict[str, Any]] = [
    {
        "id": "con_example",
        "client_name": "Example runtime",
        "redirect_host": "runtime.example.com",
        "created_at": "2026-11-24T10:00:00.000Z",
        "last_used_at": None,
        "current": True,
    }
]
ME: dict[str, Any] = {
    "id": "agt_example",
    "handle": "my-agent",
    "email": "agent@example.com",
    "display_name": None,
    "description": None,
    "avatar_url": None,
    "status": "active",
    "paused_by_owner": "none",
    "settings": {"inbox_mode": "open", "group_invite_policy": "open"},
    "created_at": "2026-11-24T10:00:00.000Z",
    "updated_at": "2026-11-24T10:00:00.000Z",
}


def _capture(route: Any, status: int, body: Any, headers: dict[str, str] | None = None) -> list[httpx.Request]:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(status, json=body, headers=headers)

    route.mock(side_effect=handler)
    return seen


def _client(**kwargs: Any) -> AgentChatClient:
    return AgentChatClient(api_key="ac_test", base_url=BASE, **kwargs)


# ─────────────── register(finish="connector") ───────────────


@respx.mock
def test_register_connector_sends_finish_and_returns_the_answer_with_no_key() -> None:
    seen = _capture(respx.post(f"{BASE}/v1/register"), 201, CONNECTOR_ANSWER)
    res = AgentChatClient.register(
        email="agent@example.com", handle="my-agent", finish="connector", base_url=BASE
    )
    assert json.loads(seen[0].content) == {
        "email": "agent@example.com",
        "handle": "my-agent",
        "finish": "connector",
    }
    assert res["next_step"] == "connect"
    assert res["connect"]["mcp_url"] == "https://api.agentchat.me/mcp/connector"
    assert res["connect"]["status_url"] == "https://api.agentchat.me/v1/register/pnd_example"
    assert res["signup_expires_at"] == "2026-11-25T10:00:00.000Z"
    assert res["expires_at"] == "2026-11-24T10:10:00.000Z"
    assert "api_key" not in res


@respx.mock
def test_register_leaves_finish_out_of_an_ordinary_body() -> None:
    seen = _capture(respx.post(f"{BASE}/v1/register"), 201, ORDINARY_ANSWER)
    AgentChatClient.register(email="agent@example.com", handle="my-agent", base_url=BASE)
    assert json.loads(seen[0].content) == {"email": "agent@example.com", "handle": "my-agent"}


@respx.mock
def test_register_connector_refuses_an_ordinary_answer() -> None:
    _capture(respx.post(f"{BASE}/v1/register"), 201, ORDINARY_ANSWER)
    with pytest.raises(ConnectorSignInUnsupportedError) as caught:
        AgentChatClient.register(
            email="agent@example.com", handle="my-agent", finish="connector", base_url=BASE
        )
    assert caught.value.pending_id == "pnd_example"
    assert caught.value.server_message == ORDINARY_ANSWER["message"]


@respx.mock
def test_register_connector_refuses_an_answer_that_carries_a_key() -> None:
    _capture(
        respx.post(f"{BASE}/v1/register"), 201, {**CONNECTOR_ANSWER, "api_key": "ac_not_a_real_key"}
    )
    with pytest.raises(ConnectorSignInUnsupportedError):
        AgentChatClient.register(
            email="agent@example.com", handle="my-agent", finish="connector", base_url=BASE
        )


@respx.mock
def test_register_connector_surfaces_the_404_of_a_server_where_it_is_off() -> None:
    _capture(
        respx.post(f"{BASE}/v1/register"),
        404,
        {"code": "NOT_FOUND", "message": "Connector sign-in is not available on this server."},
    )
    with pytest.raises(NotFoundError):
        AgentChatClient.register(
            email="agent@example.com", handle="my-agent", finish="connector", base_url=BASE
        )


@respx.mock
def test_verify_on_a_connector_signup_is_finish_on_page() -> None:
    _capture(
        respx.post(f"{BASE}/v1/register/verify"),
        409,
        {
            "code": "FINISH_ON_PAGE",
            "message": "This sign-up finishes on AgentChat's page when your person approves the connector.",
            "details": {"version": 1, "flow": "register", "next_step": "connect"},
        },
    )
    with pytest.raises(AgentChatError) as caught:
        AgentChatClient.verify("pnd_example", "123456", base_url=BASE)
    assert caught.value.code == ErrorCode.FINISH_ON_PAGE == "FINISH_ON_PAGE"
    assert caught.value.status == 409


@respx.mock
async def test_async_register_connector_and_its_refusal() -> None:
    route = respx.post(f"{BASE}/v1/register")
    seen = _capture(route, 201, CONNECTOR_ANSWER)
    res = await AsyncAgentChatClient.register(
        email="agent@example.com", handle="my-agent", finish="connector", base_url=BASE
    )
    assert json.loads(seen[0].content)["finish"] == "connector"
    assert res["next_step"] == "connect"
    _capture(route, 201, ORDINARY_ANSWER)
    with pytest.raises(ConnectorSignInUnsupportedError):
        await AsyncAgentChatClient.register(
            email="agent@example.com", handle="my-agent", finish="connector", base_url=BASE
        )


# ─────────────── register_status ───────────────

APPROVED: dict[str, Any] = {
    "status": "approved",
    "handle": "my-agent",
    "next_step": "done",
    "message": "Your person approved the connector. Call agentchat_get_my_status through it.",
}


@respx.mock
def test_register_status_gets_the_encoded_path_without_a_key() -> None:
    seen = _capture(respx.get(f"{BASE}/v1/register/pnd_a%2Fb"), 200, APPROVED)
    res = AgentChatClient.register_status("pnd_a/b", base_url=BASE)
    assert isinstance(res, RegisterStatusResult)
    assert (res.status, res.handle, res.next_step) == ("approved", "my-agent", "done")
    assert "authorization" not in seen[0].headers


@respx.mock
def test_register_status_passes_a_failure_reason_through() -> None:
    failed = {"status": "failed", "handle": "my-agent", "reason": "handle_taken", "next_step": "register", "message": "x"}
    _capture(respx.get(f"{BASE}/v1/register/pnd_example"), 200, failed)
    res = AgentChatClient.register_status("pnd_example", base_url=BASE)
    assert (res.status, res.reason) == ("failed", "handle_taken")


@respx.mock
def test_register_status_unknown_id_is_not_found() -> None:
    _capture(respx.get(f"{BASE}/v1/register/pnd_unknown"), 404, {"code": "NOT_FOUND", "message": "No such sign-up."})
    with pytest.raises(NotFoundError):
        AgentChatClient.register_status("pnd_unknown", base_url=BASE)


def test_register_status_refuses_an_empty_id() -> None:
    with pytest.raises(ValueError):
        AgentChatClient.register_status("", base_url=BASE)


@respx.mock
async def test_async_register_status() -> None:
    _capture(respx.get(f"{BASE}/v1/register/pnd_example"), 200, APPROVED)
    res = await AsyncAgentChatClient.register_status("pnd_example", base_url=BASE)
    assert res.status == "approved"


# ─────────────── client.connections.end ───────────────


@respx.mock
def test_end_deletes_the_connection_with_the_key() -> None:
    seen = _capture(respx.delete(f"{BASE}/v1/agents/me/connections/con_example"), 200, ENDED)
    res = _client().connections.end("con_example")
    assert isinstance(res, EndConnectionResult)
    assert (res.ended, res.id, res.message) == (True, "con_example", None)
    assert len(seen) == 1
    assert seen[0].headers["authorization"] == "Bearer ac_test"


@respx.mock
def test_end_encodes_the_id() -> None:
    seen = _capture(respx.delete(url__regex=rf"{BASE}/v1/agents/me/connections/.*"), 200, ENDED)
    _client().connections.end("con/../x")
    assert seen[0].url.raw_path.decode() == "/v1/agents/me/connections/con%2F..%2Fx"


def test_end_refuses_an_empty_id() -> None:
    with pytest.raises(ValueError):
        _client().connections.end("")


@respx.mock
def test_end_maps_connection_not_found() -> None:
    _capture(
        respx.delete(f"{BASE}/v1/agents/me/connections/con_x"),
        404,
        {"code": "CONNECTION_NOT_FOUND", "message": "No such connection."},
    )
    with pytest.raises(NotFoundError) as caught:
        _client().connections.end("con_x")
    assert caught.value.code == ErrorCode.CONNECTION_NOT_FOUND == "CONNECTION_NOT_FOUND"


@respx.mock
def test_end_maps_rate_limited_with_the_wait() -> None:
    _capture(
        respx.delete(f"{BASE}/v1/agents/me/connections/con_x"),
        429,
        {"code": "RATE_LIMITED", "message": "Slow down."},
        headers={"Retry-After": "7"},
    )
    with pytest.raises(RateLimitedError) as caught:
        _client(retry=RetryPolicy(max_retries=0)).connections.end("con_x")
    assert caught.value.retry_after_ms == 7000


@respx.mock
async def test_async_end() -> None:
    _capture(respx.delete(f"{BASE}/v1/agents/me/connections/con_example"), 200, ENDED)
    async with AsyncAgentChatClient(api_key="ac_test", base_url=BASE) as client:
        res = await client.connections.end("con_example")
    assert res.ended is True


# ─────────────── connections in your own status ───────────────


def test_agent_model_reads_connections_and_tolerates_their_absence() -> None:
    agent = Agent.model_validate({**ME, "connections": CONNECTIONS})
    assert agent.connections is not None
    assert agent.connections[0].id == "con_example"
    assert agent.connections[0].current is True
    assert agent.connections[0].last_used_at is None
    assert Agent.model_validate(ME).connections is None


@respx.mock
def test_get_me_passes_connections_through() -> None:
    _capture(respx.get(f"{BASE}/v1/agents/me"), 200, {**ME, "connections": CONNECTIONS})
    assert _client().get_me()["connections"] == CONNECTIONS


def test_public_surface() -> None:
    client = _client()
    assert callable(client.connections.end)
    assert callable(AgentChatClient.register_status)
    assert callable(AsyncAgentChatClient.register_status)
    assert agentchatme.ConnectorSignInUnsupportedError is ConnectorSignInUnsupportedError
    assert "ConnectorSignInUnsupportedError" in agentchatme.__all__
