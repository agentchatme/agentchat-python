"""Wake links: ``create_wake_link`` on both clients, the once-only rule, and
the errors the route answers. Mirrors the TypeScript coverage
(``tests/wake-link.test.ts``)."""

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
    CallOptions,
    ErrorCode,
    RateLimitedError,
    RetryPolicy,
    ServerError,
    ValidationError,
    create_agentchat_error,
)
from agentchatme.types import CreateWakeLinkResult

BASE = "https://api.test"
ROUTE = f"{BASE}/v1/agents/me/wake-links"

# A placeholder link: the shape the server answers, not a real token.
URL = "https://app.agentchat.test/wake/awl_example"
LINE = (
    "Give this link to the person you work with. On it they paste your trigger's URL and "
    f"secret; AgentChat tests it with one wake and saves it. This link expires in 60 minutes: {URL}"
)
LINK: dict[str, Any] = {
    "wake_url": URL,
    "expires_at": "2026-11-22T15:00:00.000Z",
    "expires_in_s": 3600,
    "message": LINE,
}
FAST = RetryPolicy(max_retries=2, base_delay_ms=1, max_delay_ms=2)


def _capture(route: Any, status: int, body: Any, headers: dict[str, str] | None = None) -> list[httpx.Request]:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(status, json=body, headers=headers)

    route.mock(side_effect=handler)
    return seen


def _client(**kwargs: Any) -> AgentChatClient:
    return AgentChatClient(api_key="ac_test", base_url=BASE, **kwargs)


# ─────────────── the wire ───────────────


@respx.mock
def test_create_wake_link_posts_the_dialect_only_and_parses_the_answer() -> None:
    seen = _capture(respx.post(ROUTE), 201, LINK)
    link = _client().create_wake_link("claude-code")
    assert isinstance(link, CreateWakeLinkResult)
    assert (link.wake_url, link.expires_at, link.expires_in_s, link.message) == (
        URL,
        "2026-11-22T15:00:00.000Z",
        3600,
        LINE,
    )
    assert len(seen) == 1
    assert json.loads(seen[0].content) == {"dialect": "claude-code"}
    assert "idempotency-key" not in seen[0].headers
    assert seen[0].headers["authorization"] == "Bearer ac_test"


@respx.mock
def test_create_wake_link_sends_url_and_rewake_when_given() -> None:
    seen = _capture(respx.post(ROUTE), 201, LINK)
    _client().create_wake_link(
        "standard-webhooks",
        url="https://hooks.example.com/agent",
        rewake=False,
        opts=CallOptions(timeout_ms=5_000, idempotency_key="caller-key-1"),
    )
    assert json.loads(seen[0].content) == {
        "dialect": "standard-webhooks",
        "url": "https://hooks.example.com/agent",
        "rewake": False,
    }


# ─────────────── exactly one attempt ───────────────


@respx.mock
def test_create_wake_link_is_never_retried() -> None:
    route = respx.post(ROUTE).mock(side_effect=[httpx.Response(503, json={}), httpx.Response(201, json=LINK)])
    with pytest.raises(ServerError):
        _client(retry=FAST).create_wake_link("agentchat", opts=CallOptions(retry=FAST))
    assert route.call_count == 1


@pytest.mark.parametrize("retry", [FAST, "auto"])
def test_a_caller_key_and_retry_policy_still_give_exactly_one_attempt(retry: Any) -> None:
    with respx.mock:
        route = respx.post(ROUTE).mock(side_effect=[httpx.Response(503, json={}), httpx.Response(201, json=LINK)])
        with pytest.raises(ServerError):
            _client(retry=FAST).create_wake_link(
                "agentchat", opts=CallOptions(idempotency_key="caller-key-1", retry=retry)
            )
        assert route.call_count == 1
        assert "idempotency-key" not in route.calls[0].request.headers


@respx.mock
def test_create_wake_link_keeps_the_caller_timeout() -> None:
    seen = _capture(respx.post(ROUTE), 201, LINK)
    _client().create_wake_link("agentchat", opts=CallOptions(timeout_ms=1234))
    assert seen[0].extensions["timeout"]["read"] == pytest.approx(1.234)


@respx.mock
async def test_async_create_wake_link_has_the_same_wire_and_one_attempt() -> None:
    seen = _capture(respx.post(ROUTE), 201, LINK)
    async with AsyncAgentChatClient(api_key="ac_test", base_url=BASE, retry=FAST) as client:
        link = await client.create_wake_link("hermes", url="https://hooks.example.com/agent", rewake=True)
    assert isinstance(link, CreateWakeLinkResult) and link.wake_url == URL
    assert json.loads(seen[0].content) == {"dialect": "hermes", "url": "https://hooks.example.com/agent", "rewake": True}
    assert "idempotency-key" not in seen[0].headers


@respx.mock
async def test_async_a_caller_key_and_retry_policy_still_give_exactly_one_attempt() -> None:
    route = respx.post(ROUTE).mock(side_effect=[httpx.Response(503, json={}), httpx.Response(201, json=LINK)])
    async with AsyncAgentChatClient(api_key="ac_test", base_url=BASE, retry=FAST) as client:
        with pytest.raises(ServerError):
            await client.create_wake_link("agentchat", opts=CallOptions(idempotency_key="caller-key-1", retry=FAST))
    assert route.call_count == 1
    assert "idempotency-key" not in route.calls[0].request.headers


# ─────────────── errors ───────────────


@pytest.mark.parametrize("code", ["INVALID_WAKE_WEBHOOK_URL", "WAKE_WEBHOOK_URL_UNRESOLVED"])
def test_a_refused_url_is_a_plain_agentchat_error_carrying_its_code(code: str) -> None:
    message = f"The webhook URL was refused ({code})."
    with respx.mock:
        route = respx.post(ROUTE).mock(return_value=httpx.Response(400, json={"code": code, "message": message}))
        with pytest.raises(AgentChatError) as info:
            _client(retry=FAST).create_wake_link("agentchat", url="https://hooks.example.com/agent")
        assert route.call_count == 1
    err = info.value
    assert type(err) is AgentChatError
    assert (err.status, err.code, str(err)) == (400, getattr(ErrorCode, code), message)


@respx.mock
def test_validation_error_is_a_validation_error() -> None:
    _capture(respx.post(ROUTE), 400, {"code": "VALIDATION_ERROR", "message": "dialect: Invalid enum value"})
    with pytest.raises(ValidationError):
        _client().create_wake_link("not-a-dialect")


@respx.mock
def test_wake_destination_exists_is_a_plain_agentchat_error() -> None:
    message = "This agent has a wake check. Clear it first (DELETE /v1/agents/me/wake-check)."
    _capture(respx.post(ROUTE), 409, {"code": "WAKE_DESTINATION_EXISTS", "message": message})
    with pytest.raises(AgentChatError) as info:
        _client().create_wake_link("agentchat")
    err = info.value
    assert type(err) is AgentChatError
    assert (err.status, err.code, str(err)) == (409, ErrorCode.WAKE_DESTINATION_EXISTS, message)


@respx.mock
def test_rate_limited_carries_the_wait_and_is_not_retried() -> None:
    route = respx.post(ROUTE).mock(
        return_value=httpx.Response(
            429,
            json={
                "code": "RATE_LIMITED",
                "message": "Too many wake links. Try again in 42 minutes.",
                "details": {"retry_after_s": 2520},
            },
            headers={"Retry-After": "2520"},
        )
    )
    with pytest.raises(RateLimitedError) as info:
        _client(retry=FAST).create_wake_link("agentchat")
    assert info.value.retry_after_ms == 2_520_000
    assert info.value.retry_after_s == 2520
    assert route.call_count == 1


@pytest.mark.parametrize("code", ["WAKE_LINKS_UNAVAILABLE", "WAKE_SECRET_UNAVAILABLE"])
def test_unavailable_is_a_server_error_carrying_its_code_in_one_attempt(code: str) -> None:
    with respx.mock:
        route = respx.post(ROUTE).mock(
            side_effect=[
                httpx.Response(503, json={"code": code, "message": "Not available on this server."}),
                httpx.Response(201, json=LINK),
            ]
        )
        with pytest.raises(ServerError) as info:
            _client(retry=FAST).create_wake_link("agentchat")
        assert route.call_count == 1
    assert (info.value.status, info.value.code) == (503, getattr(ErrorCode, code))


def test_the_four_codes_take_the_status_fallbacks() -> None:
    for code in (ErrorCode.INVALID_WAKE_WEBHOOK_URL, ErrorCode.WAKE_WEBHOOK_URL_UNRESOLVED):
        err = create_agentchat_error({"code": code, "message": "refused"}, 400)
        assert type(err) is AgentChatError
        assert (err.code, err.status) == (code, 400)
    for code in (ErrorCode.WAKE_LINKS_UNAVAILABLE, ErrorCode.WAKE_SECRET_UNAVAILABLE):
        err = create_agentchat_error({"code": code, "message": "unavailable"}, 503)
        assert isinstance(err, ServerError)
        assert (err.code, err.status) == (code, 503)


# ─────────────── public surface ───────────────


def test_public_surface() -> None:
    assert callable(AgentChatClient.create_wake_link)
    assert callable(AsyncAgentChatClient.create_wake_link)
    assert agentchatme.types.CreateWakeLinkResult is CreateWakeLinkResult
    assert ErrorCode.INVALID_WAKE_WEBHOOK_URL == "INVALID_WAKE_WEBHOOK_URL"
    assert ErrorCode.WAKE_WEBHOOK_URL_UNRESOLVED == "WAKE_WEBHOOK_URL_UNRESOLVED"
    assert ErrorCode.WAKE_LINKS_UNAVAILABLE == "WAKE_LINKS_UNAVAILABLE"
    assert ErrorCode.WAKE_SECRET_UNAVAILABLE == "WAKE_SECRET_UNAVAILABLE"
