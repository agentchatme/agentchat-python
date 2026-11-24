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
    ErrorCode,
    NotFoundError,
    RateLimitedError,
    RetryPolicy,
    ServerError,
)
from agentchatme.types import Agent, AgentConnection, EndConnectionResult, RegisterStatusResult

BASE = "https://api.test"

ORDINARY_ANSWER: dict[str, Any] = {
    "pending_id": "pnd_example",
    "message": "We sent a 6-digit code to agent@example.com. It expires in 10 minutes.",
    "version": 1,
    "flow": "register",
    "next_step": "verify",
    "status": "pending",
}
ENDED: dict[str, Any] = {"ended": True, "id": "4f6c2a8e-1b3d-4c5e-9f7a-2b8d6e0c1a34"}
CONNECTIONS: list[dict[str, Any]] = [
    {
        "id": "4f6c2a8e-1b3d-4c5e-9f7a-2b8d6e0c1a34",
        "client_name": "Example runtime",
        "redirect_host": "runtime.example.com",
        "created_at": "2026-11-24T10:00:00.000Z",
        "last_used_at": None,
        "current": True,
    },
    {
        "id": "0a1b2c3d-4e5f-4a6b-8c7d-9e0f1a2b3c4d",
        "client_name": None,
        "redirect_host": "other.example.com",
        "created_at": "2026-11-24T11:00:00.000Z",
        "last_used_at": "2026-11-24T11:05:00.000Z",
        "current": False,
    },
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
# The connector answers themselves, and the 404 where it is off, are the
# contract fixture's cases (test_conformance_signup_flow.py).


@respx.mock
def test_register_leaves_finish_out_of_an_ordinary_body() -> None:
    seen = _capture(respx.post(f"{BASE}/v1/register"), 201, ORDINARY_ANSWER)
    AgentChatClient.register(email="agent@example.com", handle="my-agent", base_url=BASE)
    assert json.loads(seen[0].content) == {"email": "agent@example.com", "handle": "my-agent"}


@pytest.mark.parametrize(
    ("status", "code", "cls"),
    [(409, "HANDLE_TAKEN", AgentChatError), (500, "OTP_FAILED", ServerError)],
)
@respx.mock
def test_register_connector_errors_are_one_attempt_with_their_code(status: int, code: str, cls: type) -> None:
    seen = _capture(
        respx.post(f"{BASE}/v1/register"), status, {"code": code, "message": "x", "details": {"param": "handle"}}
    )
    with pytest.raises(cls) as caught:
        AgentChatClient.register(email="agent@example.com", handle="my-agent", finish="connector", base_url=BASE)
    assert caught.value.code == code
    assert len(seen) == 1


@respx.mock
async def test_async_register_connector_sends_finish() -> None:
    seen = _capture(respx.post(f"{BASE}/v1/register"), 404, {"code": "NOT_FOUND", "message": "x"})
    with pytest.raises(NotFoundError):
        await AsyncAgentChatClient.register(
            email="agent@example.com", handle="my-agent", finish="connector", base_url=BASE
        )
    assert json.loads(seen[0].content)["finish"] == "connector"


# ─────────────── register_status ───────────────

STATUS_URL = "https://status.agentchat.test/v1/register/pnd_example"
APPROVED: dict[str, Any] = {
    "status": "approved",
    "handle": "my-agent",
    "next_step": "get_my_status",
    "signup_expires_at": "2026-11-25T10:00:00.000Z",
    "message": "Done: your connector is signed in as @my-agent.",
}


@respx.mock
def test_register_status_gets_the_status_url_as_given_without_a_key() -> None:
    seen = _capture(respx.get(STATUS_URL), 200, APPROVED)
    res = AgentChatClient.register_status(STATUS_URL)
    assert isinstance(res, RegisterStatusResult)
    assert (res.status, res.handle, res.next_step) == ("approved", "my-agent", "get_my_status")
    assert len(seen) == 1
    assert "authorization" not in seen[0].headers


@pytest.mark.parametrize(
    "status_url", ["", "pnd_example", "/v1/register/pnd_example", "ftp://h/v1/register/p", "https://h:x/p"]
)
def test_register_status_refuses_what_is_not_an_http_status_url(status_url: str) -> None:
    with pytest.raises(ValueError):
        AgentChatClient.register_status(status_url)


@respx.mock
def test_register_status_unavailable_is_a_server_error() -> None:
    _capture(
        respx.get(STATUS_URL),
        503,
        {"code": "CONNECTOR_SIGN_IN_UNAVAILABLE", "message": "Connector sign-in is not available right now."},
    )
    with pytest.raises(ServerError) as caught:
        AgentChatClient.register_status(STATUS_URL)
    assert caught.value.code == ErrorCode.CONNECTOR_SIGN_IN_UNAVAILABLE


@respx.mock
def test_register_status_unknown_sign_up_is_not_found() -> None:
    _capture(respx.get(STATUS_URL), 404, {"code": "NOT_FOUND", "message": "No connector sign-up has this id."})
    with pytest.raises(NotFoundError):
        AgentChatClient.register_status(STATUS_URL)


@respx.mock
async def test_async_register_status() -> None:
    _capture(respx.get(STATUS_URL), 200, APPROVED)
    res = await AsyncAgentChatClient.register_status(STATUS_URL)
    assert res.status == "approved"
    with pytest.raises(ValueError):
        await AsyncAgentChatClient.register_status("pnd_example")


# ─────────────── client.connections.end ───────────────


@respx.mock
def test_end_deletes_the_connection_with_the_key() -> None:
    seen = _capture(respx.delete(f"{BASE}/v1/agents/me/connections/4f6c2a8e-1b3d-4c5e-9f7a-2b8d6e0c1a34"), 200, ENDED)
    res = _client().connections.end("4f6c2a8e-1b3d-4c5e-9f7a-2b8d6e0c1a34")
    assert isinstance(res, EndConnectionResult)
    assert (res.ended, res.id) == (True, "4f6c2a8e-1b3d-4c5e-9f7a-2b8d6e0c1a34")
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
    _capture(respx.delete(f"{BASE}/v1/agents/me/connections/4f6c2a8e-1b3d-4c5e-9f7a-2b8d6e0c1a34"), 200, ENDED)
    async with AsyncAgentChatClient(api_key="ac_test", base_url=BASE) as client:
        res = await client.connections.end("4f6c2a8e-1b3d-4c5e-9f7a-2b8d6e0c1a34")
    assert res.ended is True


# ─────────────── connections in your own status ───────────────


def test_agent_model_reads_connections_and_tolerates_their_absence() -> None:
    agent = Agent.model_validate({**ME, "connections": CONNECTIONS})
    assert agent.connections is not None
    assert agent.connections[0].id == "4f6c2a8e-1b3d-4c5e-9f7a-2b8d6e0c1a34"
    assert agent.connections[0].current is True
    assert agent.connections[0].last_used_at is None
    assert agent.connections[1].client_name is None
    assert agent.connections[1].current is False
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
    assert "AgentConnection" in agentchatme.types.__all__


# ─────────────── connections in the recovery answer ───────────────

RECOVERED: dict[str, Any] = {
    "handle": "my-agent",
    "api_key": "ac_not_a_real_key",
    "version": 1,
    "flow": "recover",
    "next_step": "done",
    "status": "approved",
    "message": "Your new key works now. 1 runtime is still signed in as you.",
}


def _dumped(connections: list[AgentConnection] | None) -> list[dict[str, Any]] | None:
    return None if connections is None else [c.model_dump() for c in connections]


@respx.mock
def test_recover_verify_reads_connections_as_agent_connections() -> None:
    _capture(respx.post(f"{BASE}/v1/agents/recover/verify"), 200, {**RECOVERED, "connections": CONNECTIONS})
    result = AgentChatClient.recover_verify("pnd_example", "123456", base_url=BASE)
    assert result[0] == "my-agent"
    assert result.connections is not None
    assert all(isinstance(c, AgentConnection) for c in result.connections)
    assert _dumped(result.connections) == CONNECTIONS


@pytest.mark.parametrize("sent", [None, [{"id": "no-other-fields"}], "not a list"])
@respx.mock
def test_recover_verify_connections_none_never_costs_the_key(sent: Any) -> None:
    body = RECOVERED if sent is None else {**RECOVERED, "connections": sent}
    _capture(respx.post(f"{BASE}/v1/agents/recover/verify"), 200, body)
    result = AgentChatClient.recover_verify("pnd_example", "123456", base_url=BASE)
    assert (result[1], result.connections) == ("ac_not_a_real_key", None)


@respx.mock
async def test_async_recover_verify_reads_connections() -> None:
    _capture(respx.post(f"{BASE}/v1/agents/recover/verify"), 200, {**RECOVERED, "connections": CONNECTIONS})
    result = await AsyncAgentChatClient.recover_verify("pnd_example", "123456", base_url=BASE)
    assert _dumped(result.connections) == CONNECTIONS
