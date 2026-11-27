"""Connector sign-in: ``register`` and ``recover`` with ``finish="connector"``,
``verify`` and ``recover_verify`` on a ``next_step: "connect"`` answer
(``ConnectorVerifyResult``, ``ConnectorSignupError``), the integration kit's
``finish``, ``client.connections.end`` and ``connections`` on your own
status, on both clients. Mirrors the TypeScript coverage
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
    AgentChatClientIdentity,
    AgentChatError,
    AsyncAgentChatClient,
    ConnectorSignupError,
    ConnectorVerifyResult,
    ErrorCode,
    NotFoundError,
    RateLimitedError,
    RecoverVerifyResult,
    RegisterVerifyResult,
    RetryPolicy,
    ServerError,
)
from agentchatme.integration import account
from agentchatme.types import Agent, AgentConnection, EndConnectionResult

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
def test_register_with_finish_returns_the_pending_start() -> None:
    # The start step answers as without finish: the ordinary pending start.
    seen = _capture(respx.post(f"{BASE}/v1/register"), 200, ORDINARY_ANSWER)
    answer = AgentChatClient.register(
        email="agent@example.com", handle="my-agent", finish="connector", base_url=BASE
    )
    assert answer == ORDINARY_ANSWER
    assert json.loads(seen[0].content) == {
        "email": "agent@example.com",
        "handle": "my-agent",
        "finish": "connector",
    }


@respx.mock
async def test_async_register_with_finish_returns_the_pending_start() -> None:
    seen = _capture(respx.post(f"{BASE}/v1/register"), 200, ORDINARY_ANSWER)
    answer = await AsyncAgentChatClient.register(
        email="agent@example.com", handle="my-agent", finish="connector", base_url=BASE
    )
    assert answer == ORDINARY_ANSWER
    assert json.loads(seen[0].content)["finish"] == "connector"


@respx.mock
async def test_async_register_connector_sends_finish() -> None:
    seen = _capture(respx.post(f"{BASE}/v1/register"), 404, {"code": "NOT_FOUND", "message": "x"})
    with pytest.raises(NotFoundError):
        await AsyncAgentChatClient.register(
            email="agent@example.com", handle="my-agent", finish="connector", base_url=BASE
        )
    assert json.loads(seen[0].content)["finish"] == "connector"


# ─────────────── recover(finish="connector") ───────────────

RECOVER_STARTED: dict[str, Any] = {
    "pending_id": "pnd_example",
    "message": "If an account matches, we sent a 6-digit code to agent@example.com.",
    "version": 1,
    "flow": "recover",
    "next_step": "recover_verify",
    "status": "pending",
}


@pytest.mark.parametrize(
    ("finish", "sent"),
    [
        (None, {"email": "agent@example.com", "handle": "my-agent"}),
        ("connector", {"email": "agent@example.com", "handle": "my-agent", "finish": "connector"}),
    ],
)
@respx.mock
def test_recover_sends_finish_only_when_given(finish: Any, sent: dict[str, Any]) -> None:
    seen = _capture(respx.post(f"{BASE}/v1/agents/recover"), 200, RECOVER_STARTED)
    answer = AgentChatClient.recover("agent@example.com", handle="my-agent", finish=finish, base_url=BASE)
    assert answer == RECOVER_STARTED
    assert len(seen) == 1
    assert json.loads(seen[0].content) == sent


@pytest.mark.parametrize(
    ("finish", "sent"),
    [
        (None, {"email": "agent@example.com"}),
        ("connector", {"email": "agent@example.com", "finish": "connector"}),
    ],
)
@respx.mock
async def test_async_recover_sends_finish_only_when_given(finish: Any, sent: dict[str, Any]) -> None:
    seen = _capture(respx.post(f"{BASE}/v1/agents/recover"), 200, RECOVER_STARTED)
    answer = await AsyncAgentChatClient.recover("agent@example.com", finish=finish, base_url=BASE)
    assert answer == RECOVER_STARTED
    assert json.loads(seen[0].content) == sent


@respx.mock
def test_recover_connector_not_available_is_not_found() -> None:
    _capture(
        respx.post(f"{BASE}/v1/agents/recover"),
        404,
        {
            "code": "NOT_FOUND",
            "message": "Connecting a runtime through a connector isn't available here.",
            "details": {"version": 1, "flow": "recover", "next_step": "recover"},
        },
    )
    with pytest.raises(NotFoundError):
        AgentChatClient.recover("agent@example.com", handle="my-agent", finish="connector", base_url=BASE)


# ─────────────── verify / recover_verify on a connect answer ───────────────

CONNECT: dict[str, Any] = {
    "mcp_url": "https://api.test/mcp/connector/acg_example-not-a-real-grant",
    "resource": "https://api.test/mcp/connector/acg_example-not-a-real-grant",
    "issuer": "https://api.test",
    "authorization_endpoint": "https://api.test/oauth/authorize",
    "token_endpoint": "https://api.test/oauth/token",
    "registration_endpoint": "https://api.test/oauth/register",
    "scope": "agentchat",
    "expires_at": "2026-11-27T11:00:00.000Z",
}
REGISTER_CONNECT: dict[str, Any] = {
    "agent": {**ME},
    "connect": CONNECT,
    "claim_url": "https://app.agentchat.test/claim/acl_example-not-a-real-link",
    "claim_expires_at": "2026-11-27T10:10:00.000Z",
    "claim_expires_in_s": 600,
    "version": 1,
    "flow": "register",
    "next_step": "connect",
    "status": "approved",
    "message": "Email verified. @my-agent is registered. Add AgentChat to your runtime as a connector.",
}
RECOVER_CONNECT: dict[str, Any] = {
    "handle": "my-agent",
    "connect": CONNECT,
    "version": 1,
    "flow": "recover",
    "next_step": "connect",
    "status": "approved",
    "message": "Email verified. Add AgentChat to your runtime as a connector. Your API key is unchanged.",
}
REGISTERED: dict[str, Any] = {
    "agent": {**ME},
    "api_key": "ac_not_a_real_key",
    "version": 1,
    "flow": "register",
    "next_step": "done",
    "status": "approved",
    "message": "Email verified. Logged in as @my-agent.",
}
RECOVERED_DONE: dict[str, Any] = {
    "handle": "my-agent",
    "api_key": "ac_not_a_real_key",
    "version": 1,
    "flow": "recover",
    "next_step": "done",
    "status": "approved",
    "message": "Email verified. Logged in as @my-agent. Your old API key no longer works.",
}
VERIFY_SENT = {"pending_id": "pnd_example", "code": "123456"}

# (method name, path, connect answer, key answer, the ordinary result type)
_VERIFIES = [
    ("verify", "/v1/register/verify", REGISTER_CONNECT, REGISTERED, RegisterVerifyResult),
    ("recover_verify", "/v1/agents/recover/verify", RECOVER_CONNECT, RECOVERED_DONE, RecoverVerifyResult),
]
_VERIFY_IDS = [v[0] for v in _VERIFIES]


def _assert_connector_result(result: Any, answer: dict[str, Any]) -> None:
    assert isinstance(result, ConnectorVerifyResult)
    assert result.connect == CONNECT
    assert result.handle == "my-agent"
    assert (result.message, result.next_step, result.status, result.flow) == (
        answer["message"],
        "connect",
        "approved",
        answer["flow"],
    )
    assert result.claim_url == answer.get("claim_url")
    # No key and no client: nothing to save but the connector address.
    assert not hasattr(result, "api_key")
    assert not isinstance(result, tuple)
    assert account.flow_of(result) == {
        "version": 1,
        "flow": answer["flow"],
        "next_step": "connect",
        "status": "approved",
    }


def _assert_connector_error(err: Any, answer: dict[str, Any], status: int) -> None:
    assert isinstance(err, ConnectorSignupError)
    assert isinstance(err, AgentChatError)
    assert err.connect == CONNECT
    assert (err.handle, err.message, str(err)) == ("my-agent", answer["message"], answer["message"])
    assert (err.code, err.status) == ("CONNECTOR_SIGNUP", status)
    # details: the answer's flow fields, so flow_of reads it like any sign-up answer.
    flow = {"version": 1, "flow": answer["flow"], "next_step": "connect", "status": "approved"}
    assert err.details == flow
    assert account.flow_of(err) == flow
    # result: what the same call with finish="connector" returns.
    _assert_connector_result(err.result, answer)


@pytest.mark.parametrize(("name", "path", "answer", "_key", "_cls"), _VERIFIES, ids=_VERIFY_IDS)
@respx.mock
def test_verify_with_finish_returns_the_connector_result(
    name: str, path: str, answer: dict[str, Any], _key: Any, _cls: Any
) -> None:
    seen = _capture(respx.post(f"{BASE}{path}"), 201, answer)
    result = getattr(AgentChatClient, name)("pnd_example", "123456", finish="connector", base_url=BASE)
    _assert_connector_result(result, answer)
    # finish is the caller's: never sent.
    assert len(seen) == 1
    assert json.loads(seen[0].content) == VERIFY_SENT


@pytest.mark.parametrize(("name", "path", "answer", "_key", "_cls"), _VERIFIES, ids=_VERIFY_IDS)
@respx.mock
async def test_async_verify_with_finish_returns_the_connector_result(
    name: str, path: str, answer: dict[str, Any], _key: Any, _cls: Any
) -> None:
    seen = _capture(respx.post(f"{BASE}{path}"), 200, answer)
    result = await getattr(AsyncAgentChatClient, name)(
        "pnd_example", "123456", finish="connector", base_url=BASE
    )
    _assert_connector_result(result, answer)
    assert json.loads(seen[0].content) == VERIFY_SENT


@pytest.mark.parametrize(("name", "path", "answer", "_key", "_cls"), _VERIFIES, ids=_VERIFY_IDS)
@respx.mock
def test_verify_without_finish_raises_with_the_connect_block(
    name: str, path: str, answer: dict[str, Any], _key: Any, _cls: Any
) -> None:
    seen = _capture(respx.post(f"{BASE}{path}"), 201, answer)
    with pytest.raises(ConnectorSignupError) as caught:
        getattr(AgentChatClient, name)("pnd_example", "123456", base_url=BASE)
    _assert_connector_error(caught.value, answer, 201)
    assert len(seen) == 1


@pytest.mark.parametrize(("name", "path", "answer", "_key", "_cls"), _VERIFIES, ids=_VERIFY_IDS)
@respx.mock
async def test_async_verify_without_finish_raises_with_the_connect_block(
    name: str, path: str, answer: dict[str, Any], _key: Any, _cls: Any
) -> None:
    _capture(respx.post(f"{BASE}{path}"), 200, answer)
    with pytest.raises(ConnectorSignupError) as caught:
        await getattr(AsyncAgentChatClient, name)("pnd_example", "123456", base_url=BASE)
    _assert_connector_error(caught.value, answer, 200)


@pytest.mark.parametrize(("name", "path", "_connect", "answer", "cls"), _VERIFIES, ids=_VERIFY_IDS)
@respx.mock
def test_verify_with_finish_and_a_key_returns_the_ordinary_result(
    name: str, path: str, _connect: Any, answer: dict[str, Any], cls: type
) -> None:
    # The server's answer decides: finish only says what the caller can handle.
    _capture(respx.post(f"{BASE}{path}"), 200, answer)
    result = getattr(AgentChatClient, name)("pnd_example", "123456", finish="connector", base_url=BASE)
    assert isinstance(result, cls)
    assert result[1] == "ac_not_a_real_key"
    assert isinstance(result[2], AgentChatClient)
    assert result.next_step == "done"
    result[2].close()


@pytest.mark.parametrize(("name", "path", "_connect", "answer", "cls"), _VERIFIES, ids=_VERIFY_IDS)
@respx.mock
async def test_async_verify_with_finish_and_a_key_returns_the_ordinary_result(
    name: str, path: str, _connect: Any, answer: dict[str, Any], cls: type
) -> None:
    _capture(respx.post(f"{BASE}{path}"), 200, answer)
    result = await getattr(AsyncAgentChatClient, name)(
        "pnd_example", "123456", finish="connector", base_url=BASE
    )
    assert isinstance(result, cls)
    assert result[1] == "ac_not_a_real_key"
    assert isinstance(result[2], AsyncAgentChatClient)
    await result[2].aclose()


@pytest.mark.parametrize("name", ["verify", "recover_verify"])
@respx.mock
async def test_verify_refuses_an_unknown_finish_before_sending(name: str) -> None:
    route = respx.post(url__regex=rf"{BASE}/.*")
    with pytest.raises(ValueError):
        getattr(AgentChatClient, name)("pnd_example", "123456", finish="page", base_url=BASE)
    with pytest.raises(ValueError):
        await getattr(AsyncAgentChatClient, name)("pnd_example", "123456", finish="page", base_url=BASE)
    assert not route.called


_MALFORMED_CONNECT = [
    ("no address", {k: v for k, v in RECOVER_CONNECT.items() if k != "connect"}),
    ("no mcp_url", {**RECOVER_CONNECT, "connect": {k: v for k, v in CONNECT.items() if k != "mcp_url"}}),
    ("no handle", {k: v for k, v in RECOVER_CONNECT.items() if k != "handle"}),
]


@pytest.mark.parametrize("finish", [None, "connector"])
@pytest.mark.parametrize(("_why", "answer"), _MALFORMED_CONNECT, ids=[m[0] for m in _MALFORMED_CONNECT])
@respx.mock
async def test_a_connect_answer_the_sdk_cannot_use_raises_a_plain_error(
    _why: str, answer: dict[str, Any], finish: Any
) -> None:
    _capture(respx.post(f"{BASE}/v1/agents/recover/verify"), 200, answer)
    with pytest.raises(AgentChatError) as caught:
        AgentChatClient.recover_verify("pnd_example", "123456", finish=finish, base_url=BASE)
    assert type(caught.value) is AgentChatError
    assert (caught.value.code, caught.value.status) == ("INTERNAL_ERROR", 200)
    assert caught.value.details == answer
    with pytest.raises(AgentChatError) as caught_async:
        await AsyncAgentChatClient.recover_verify("pnd_example", "123456", finish=finish, base_url=BASE)
    assert type(caught_async.value) is AgentChatError


@pytest.mark.parametrize("finish", [None, "connector"])
@pytest.mark.parametrize(("name", "path", "_connect", "answer", "_cls"), _VERIFIES, ids=_VERIFY_IDS)
@respx.mock
async def test_an_answer_without_a_key_raises_a_plain_error_never_a_key_error(
    name: str, path: str, _connect: Any, answer: dict[str, Any], _cls: Any, finish: Any
) -> None:
    keyless = {k: v for k, v in answer.items() if k != "api_key"}
    _capture(respx.post(f"{BASE}{path}"), 200, keyless)
    with pytest.raises(AgentChatError) as caught:
        getattr(AgentChatClient, name)("pnd_example", "123456", finish=finish, base_url=BASE)
    assert type(caught.value) is AgentChatError
    assert caught.value.code == "INTERNAL_ERROR"
    assert "no API key" in str(caught.value)
    with pytest.raises(AgentChatError) as caught_async:
        await getattr(AsyncAgentChatClient, name)("pnd_example", "123456", finish=finish, base_url=BASE)
    assert type(caught_async.value) is AgentChatError


@respx.mock
def test_a_recovered_key_is_kept_when_the_answer_names_no_handle() -> None:
    _capture(
        respx.post(f"{BASE}/v1/agents/recover/verify"),
        200,
        {k: v for k, v in RECOVERED_DONE.items() if k != "handle"},
    )
    handle, api_key, client = AgentChatClient.recover_verify("pnd_example", "123456", base_url=BASE)
    assert (handle, api_key) == ("", "ac_not_a_real_key")
    client.close()


@respx.mock
def test_connector_result_repr_leaves_the_address_out() -> None:
    _capture(respx.post(f"{BASE}/v1/register/verify"), 201, REGISTER_CONNECT)
    result = AgentChatClient.verify("pnd_example", "123456", finish="connector", base_url=BASE)
    assert "acg_" not in repr(result)
    assert "my-agent" in repr(result)


# ─────────────── integration.account: finish on the start step ───────────────

KIT = {"client_identity": AgentChatClientIdentity(name="python_sdk"), "api_base": BASE}


@pytest.mark.parametrize(
    ("kind", "path", "answer", "given"),
    [
        ("recover", "/v1/agents/recover", RECOVER_STARTED, {"email": "agent@example.com", "handle": "my-agent"}),
        ("register", "/v1/register", ORDINARY_ANSWER, {"email": "agent@example.com", "handle": "my-agent"}),
    ],
)
@pytest.mark.parametrize("finish", [None, "connector"])
@respx.mock
def test_kit_start_passes_finish_only_when_given(
    kind: Any, path: str, answer: dict[str, Any], given: dict[str, Any], finish: Any
) -> None:
    seen = _capture(respx.post(f"{BASE}{path}"), 200, answer)
    sent = {**given, "finish": finish} if finish is not None else given
    result = account.start(kind, {**given, "finish": finish}, **KIT)
    assert json.loads(seen[0].content) == sent
    assert result.action == "ask_code"
    assert result.flow.get("pending_id") == "pnd_example"


@respx.mock
async def test_async_kit_start_passes_finish_on_recover() -> None:
    seen = _capture(respx.post(f"{BASE}/v1/agents/recover"), 200, RECOVER_STARTED)
    result = await account.start_async(
        "recover", {"email": "agent@example.com", "finish": "connector"}, **KIT
    )
    assert json.loads(seen[0].content) == {"email": "agent@example.com", "finish": "connector"}
    assert result.action == "ask_code"


@pytest.mark.parametrize(
    ("step", "path", "answer"),
    [
        ("recover_verify", "/v1/agents/recover/verify", RECOVER_CONNECT),
        ("verify", "/v1/register/verify", REGISTER_CONNECT),
    ],
)
@respx.mock
def test_kit_submit_code_keeps_the_connect_block(step: str, path: str, answer: dict[str, Any]) -> None:
    _capture(respx.post(f"{BASE}{path}"), 200, answer)
    flow: Any = {"version": 1, "flow": answer["flow"], "next_step": step, "pending_id": "pnd_example"}
    result = account.submit_code(flow, "123456", **KIT)
    assert (result.action, result.handle, result.api_key) == ("connect", "my-agent", None)
    assert result.connect == CONNECT
    assert result.message == answer["message"]
    assert "acg_" not in repr(result)


@respx.mock
async def test_async_kit_submit_code_keeps_the_connect_block() -> None:
    _capture(respx.post(f"{BASE}/v1/agents/recover/verify"), 200, RECOVER_CONNECT)
    flow: Any = {"version": 1, "flow": "recover", "next_step": "recover_verify", "pending_id": "pnd_example"}
    result = await account.submit_code_async(flow, "123456", **KIT)
    assert (result.action, result.connect) == ("connect", CONNECT)


@pytest.mark.parametrize(("_why", "answer"), _MALFORMED_CONNECT, ids=[m[0] for m in _MALFORMED_CONNECT])
@respx.mock
def test_kit_connect_answer_without_an_address_or_handle_stops(_why: str, answer: dict[str, Any]) -> None:
    _capture(respx.post(f"{BASE}/v1/agents/recover/verify"), 200, answer)
    flow: Any = {"version": 1, "flow": "recover", "next_step": "recover_verify", "pending_id": "pnd_example"}
    result = account.submit_code(flow, "123456", **KIT)
    assert (result.action, result.connect, result.handle) == ("stop", None, None)


def test_kit_action_of_no_longer_reads_get_my_status_as_done() -> None:
    flow: Any = {"version": 1, "flow": "register", "next_step": "get_my_status"}
    assert account.action_of(flow, 200, "verify") == "stop"


async def test_kit_run_refuses_finish() -> None:
    def never(*_: Any) -> Any:
        raise AssertionError("nothing is asked, shown or sent")

    for kind in ("register", "recover"):
        given: Any = {"email": "agent@example.com", "handle": "my-agent", "finish": "connector"}
        with pytest.raises(TypeError):
            account.run(kind=kind, input=given, ask=never, show=never, **KIT)
        with pytest.raises(TypeError):
            await account.run_async(kind=kind, input=given, ask=never, show=never, **KIT)


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
    assert "AgentConnection" in agentchatme.types.__all__
    assert "ConnectorSignupConnect" in agentchatme.types.__all__
    assert {"ConnectorVerifyResult", "ConnectorSignupError"} <= set(agentchatme.__all__)
    # The connector status read and its code describe a server that is gone.
    assert not hasattr(AgentChatClient, "register_status")
    assert not hasattr(AsyncAgentChatClient, "register_status")
    assert not hasattr(ErrorCode, "FINISH_ON_PAGE")
    assert "RegisterStatusResult" not in agentchatme.types.__all__


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
