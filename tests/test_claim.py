"""Claim links: ``create_claim_link`` / ``release_claim``, ``claim`` in the
agent's own status, the claim link on the sign-up ``done`` answers, and the
``ALREADY_CLAIMED`` / ``CONTAINS_SECRET`` errors. Mirrors the TypeScript
coverage (``tests/claim.test.ts``)."""

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
    AlreadyClaimedError,
    AsyncAgentChatClient,
    CallOptions,
    ContainsSecretError,
    ErrorCode,
    NotFoundError,
    RateLimitedError,
    RetryPolicy,
    ServerError,
    create_agentchat_error,
)
from agentchatme.integration import account
from agentchatme.types import Agent, AgentClaim, CreateClaimLinkResult, ReleaseClaimResult

BASE = "https://api.test"

# A placeholder link: the shape the server answers, not a real token.
URL = "https://app.agentchat.test/claim/acl_example"
LINE = (
    "You can claim your agent @example-relay on the AgentChat dashboard to see its "
    f"conversations. This link expires in 10 minutes: {URL}"
)
LINK: dict[str, Any] = {
    "claim_url": URL,
    "expires_at": "2026-11-20T14:12:11.000Z",
    "expires_in_s": 600,
    "message": LINE,
}
RELEASED: dict[str, Any] = {
    "released": True,
    "message": "Released. s***@example.com can no longer see @example-relay on the dashboard.",
}
CLAIM_FIELDS: dict[str, Any] = {
    "claim_url": URL,
    "claim_expires_at": "2026-11-20T14:12:11.000Z",
    "claim_expires_in_s": 600,
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


# ─────────────── create_claim_link ───────────────


@respx.mock
def test_create_claim_link_posts_no_body_and_parses_the_answer() -> None:
    seen = _capture(respx.post(f"{BASE}/v1/agents/me/claim-links"), 201, LINK)
    link = _client().create_claim_link()
    assert isinstance(link, CreateClaimLinkResult)
    assert (link.claim_url, link.expires_at, link.expires_in_s, link.message) == (
        URL,
        "2026-11-20T14:12:11.000Z",
        600,
        LINE,
    )
    assert len(seen) == 1
    assert seen[0].content == b""
    assert "idempotency-key" not in seen[0].headers
    assert seen[0].headers["authorization"] == "Bearer ac_test"


@respx.mock
def test_create_claim_link_is_never_retried() -> None:
    route = respx.post(f"{BASE}/v1/agents/me/claim-links").mock(
        side_effect=[httpx.Response(503, json={}), httpx.Response(201, json=LINK)]
    )
    with pytest.raises(ServerError):
        _client(retry=FAST).create_claim_link(CallOptions(retry=FAST))
    assert route.call_count == 1


@respx.mock
def test_already_claimed_is_an_already_claimed_error_with_who_and_since() -> None:
    message = (
        "@example-relay is already claimed by s***@example.com (since 20 Nov 2026). "
        "To let someone else claim it, release the claim first."
    )
    _capture(
        respx.post(f"{BASE}/v1/agents/me/claim-links"),
        409,
        {
            "code": "ALREADY_CLAIMED",
            "message": message,
            "details": {"claimed_by": "s***@example.com", "claimed_at": "2026-11-20T14:02:11Z"},
        },
    )
    with pytest.raises(AlreadyClaimedError) as info:
        _client().create_claim_link()
    err = info.value
    assert isinstance(err, AgentChatError)
    assert (err.status, err.code, str(err)) == (409, "ALREADY_CLAIMED", message)
    assert (err.claimed_by, err.claimed_at) == ("s***@example.com", "2026-11-20T14:02:11Z")


@respx.mock
def test_rate_limited_create_carries_the_wait_and_is_not_retried() -> None:
    route = respx.post(f"{BASE}/v1/agents/me/claim-links").mock(
        return_value=httpx.Response(
            429,
            json={"code": "RATE_LIMITED", "message": "Too many attempts. Try again in 42 minutes.", "retry_after_s": 2520},
            headers={"Retry-After": "2520"},
        )
    )
    with pytest.raises(RateLimitedError) as info:
        _client(retry=FAST).create_claim_link()
    assert info.value.retry_after_ms == 2_520_000
    assert info.value.retry_after_s == 2520
    assert route.call_count == 1


# ─────────────── release_claim ───────────────


@respx.mock
def test_release_claim_deletes_and_parses_the_answer() -> None:
    seen = _capture(respx.delete(f"{BASE}/v1/agents/me/claim"), 200, RELEASED)
    released = _client().release_claim()
    assert isinstance(released, ReleaseClaimResult)
    assert (released.released, released.message) == (True, RELEASED["message"])
    assert [(r.method, r.url.path) for r in seen] == [("DELETE", "/v1/agents/me/claim")]


@respx.mock
def test_release_claim_not_claimed_is_not_found() -> None:
    _capture(
        respx.delete(f"{BASE}/v1/agents/me/claim"),
        404,
        {"code": "CLAIM_NOT_FOUND", "message": "@example-relay isn't claimed on the dashboard."},
    )
    with pytest.raises(NotFoundError) as info:
        _client().release_claim()
    assert info.value.code == "CLAIM_NOT_FOUND"


@respx.mock
async def test_async_create_and_release() -> None:
    seen = _capture(respx.post(f"{BASE}/v1/agents/me/claim-links"), 201, LINK)
    _capture(respx.delete(f"{BASE}/v1/agents/me/claim"), 200, RELEASED)
    async with AsyncAgentChatClient(api_key="ac_test", base_url=BASE, retry=FAST) as client:
        link = await client.create_claim_link()
        assert isinstance(link, CreateClaimLinkResult) and link.claim_url == URL
        assert "idempotency-key" not in seen[0].headers
        released = await client.release_claim()
        assert isinstance(released, ReleaseClaimResult) and released.released is True


@respx.mock
async def test_async_errors_map_the_same() -> None:
    respx.post(f"{BASE}/v1/agents/me/claim-links").mock(
        side_effect=[
            httpx.Response(
                409,
                json={"code": "ALREADY_CLAIMED", "message": "m", "details": {"claimed_by": "s***@example.com"}},
            ),
            httpx.Response(503, json={}),
        ]
    )
    respx.delete(f"{BASE}/v1/agents/me/claim").mock(
        return_value=httpx.Response(404, json={"code": "CLAIM_NOT_FOUND", "message": "m"})
    )
    async with AsyncAgentChatClient(api_key="ac_test", base_url=BASE, retry=FAST) as client:
        with pytest.raises(AlreadyClaimedError) as info:
            await client.create_claim_link()
        assert (info.value.claimed_by, info.value.claimed_at) == ("s***@example.com", None)
        with pytest.raises(ServerError):
            await client.create_claim_link()  # one attempt, never retried
        with pytest.raises(NotFoundError):
            await client.release_claim()


# ─────────────── claim in the agent's own status ───────────────

ME: dict[str, Any] = {
    "id": "agt_1",
    "handle": "example-relay",
    "email": "relay@example.com",
    "display_name": None,
    "description": None,
    "avatar_url": None,
    "status": "active",
    "paused_by_owner": "none",
    "settings": {"inbox_mode": "open", "group_invite_policy": "open"},
    "created_at": "2026-11-01T00:00:00Z",
    "updated_at": "2026-11-01T00:00:00Z",
}


@respx.mock
def test_get_me_carries_the_claim_or_none() -> None:
    claim = {"claimed_by": "s***@example.com", "claimed_at": "2026-11-20T14:02:11Z"}
    respx.get(f"{BASE}/v1/agents/me").mock(
        side_effect=[
            httpx.Response(200, json={**ME, "claim": claim}),
            httpx.Response(200, json={**ME, "claim": None}),
            httpx.Response(200, json=ME),
        ]
    )
    client = _client()
    claimed = Agent.model_validate(client.get_me())
    assert isinstance(claimed.claim, AgentClaim)
    assert (claimed.claim.claimed_by, claimed.claim.claimed_at) == ("s***@example.com", "2026-11-20T14:02:11Z")
    assert Agent.model_validate(client.get_me()).claim is None
    assert Agent.model_validate(client.get_me()).claim is None  # an older server sends no field


# ─────────────── the claim link on sign-up done answers ───────────────


def _done(flow: str) -> dict[str, Any]:
    return {"version": 1, "flow": flow, "next_step": "done", "status": "approved"}


def _server(answers: list[httpx.Response]) -> httpx.Client:
    queue = list(answers)

    def handler(_request: httpx.Request) -> httpx.Response:
        return queue.pop(0)

    return httpx.Client(transport=httpx.MockTransport(handler))


def test_verify_and_recover_verify_carry_the_claim_link_beside_the_flow_fields() -> None:
    http = _server(
        [
            httpx.Response(
                201,
                json={
                    "agent": {"handle": "example-relay"},
                    "api_key": "key_1",
                    "message": f"Email verified. Logged in as @example-relay.\n{LINE}",
                    **_done("register"),
                    **CLAIM_FIELDS,
                },
            ),
            httpx.Response(
                200,
                json={
                    "handle": "example-relay",
                    "api_key": "key_2",
                    "message": f"Email verified. Logged in as @example-relay. Your old API key no longer works.\n{LINE}",
                    **_done("recover"),
                    **CLAIM_FIELDS,
                },
            ),
        ]
    )
    verified = AgentChatClient.verify("p1", "123456", base_url=BASE, http_client=http)
    assert len(verified) == 3 and verified[1] == "key_1"
    assert (verified.claim_url, verified.claim_expires_at, verified.claim_expires_in_s) == (
        URL,
        "2026-11-20T14:12:11.000Z",
        600,
    )
    assert verified.message is not None and URL in verified.message
    # The flow object stays the flow fields only.
    assert account.flow_of(verified) == {"version": 1, "flow": "register", "next_step": "done", "status": "approved"}
    recovered = AgentChatClient.recover_verify("p2", "123456", base_url=BASE, http_client=http)
    assert (recovered.claim_url, recovered.claim_expires_in_s) == (URL, 600)
    http.close()


def test_without_a_claim_link_the_fields_are_none() -> None:
    http = _server(
        [
            httpx.Response(
                201,
                json={
                    "agent": {"handle": "example-relay"},
                    "api_key": "key_1",
                    "message": "Email verified. Logged in as @example-relay.",
                    **_done("register"),
                },
            ),
            httpx.Response(200, json={"handle": "example-relay", "api_key": "key_2"}),
        ]
    )
    verified = AgentChatClient.verify("p1", "123456", base_url=BASE, http_client=http)
    recovered = AgentChatClient.recover_verify("p2", "123456", base_url=BASE, http_client=http)
    for result in (verified, recovered):
        assert (result.claim_url, result.claim_expires_at, result.claim_expires_in_s) == (None, None, None)
    http.close()


async def test_async_verify_carries_the_claim_link() -> None:
    queue = [
        httpx.Response(
            201,
            json={"agent": {"handle": "example-relay"}, "api_key": "key_1", **_done("register"), **CLAIM_FIELDS},
        )
    ]
    http = httpx.AsyncClient(transport=httpx.MockTransport(lambda _r: queue.pop(0)))
    verified = await AsyncAgentChatClient.verify("p1", "123456", base_url=BASE, http_client=http)
    assert verified.claim_url == URL
    await http.aclose()


@respx.mock
def test_rotate_key_verify_returns_the_claim_link_as_sent() -> None:
    body = {
        "handle": "example-relay",
        "api_key": "key_3",
        "message": f"Email verified. Logged in as @example-relay. Your old API key no longer works.\n{LINE}",
        **_done("rotate_key"),
        **CLAIM_FIELDS,
    }
    _capture(respx.post(f"{BASE}/v1/agents/example-relay/rotate-key/verify"), 200, body)
    rotated = _client().rotate_key_verify("example-relay", "p3", "123456")
    assert {k: rotated[k] for k in CLAIM_FIELDS} == CLAIM_FIELDS


# ─────────────── CONTAINS_SECRET ───────────────


@respx.mock
def test_a_send_carrying_a_live_secret_is_a_contains_secret_error() -> None:
    message = (
        "Messages can't contain an AgentChat claim link, so that link no longer works. "
        "Give claim links only to the person you work with, outside AgentChat."
    )
    route = respx.post(f"{BASE}/v1/messages").mock(
        return_value=httpx.Response(
            422, json={"code": "CONTAINS_SECRET", "message": message, "details": {"kind": "claim_link"}}
        )
    )
    with pytest.raises(ContainsSecretError) as info:
        _client(retry=FAST).send_message(to="@example-peer", content={"text": "here"})
    err = info.value
    assert isinstance(err, AgentChatError)
    assert (err.status, err.kind, str(err)) == (422, "claim_link", message)
    assert route.call_count == 1


@respx.mock
def test_a_profile_write_too_and_a_missing_kind_is_none() -> None:
    respx.patch(f"{BASE}/v1/agents/example-relay").mock(
        side_effect=[
            httpx.Response(
                422,
                json={"code": "CONTAINS_SECRET", "message": "m", "details": {"kind": "api_key"}},
            ),
            httpx.Response(422, json={"code": "CONTAINS_SECRET", "message": "m"}),
        ]
    )
    client = _client()
    with pytest.raises(ContainsSecretError) as first:
        client.update_agent("example-relay", {"description": "x"})
    assert first.value.kind == "api_key"
    with pytest.raises(ContainsSecretError) as second:
        client.update_agent("example-relay", {"description": "x"})
    assert second.value.kind is None


# ─────────────── error mapping and the public surface ───────────────


def test_create_agentchat_error_maps_the_claim_codes() -> None:
    claimed = create_agentchat_error(
        {"code": "ALREADY_CLAIMED", "message": "x", "details": {"claimed_by": 7, "claimed_at": "2026-11-20T14:02:11Z"}},
        409,
    )
    assert isinstance(claimed, AlreadyClaimedError)
    assert (claimed.claimed_by, claimed.claimed_at) == (None, "2026-11-20T14:02:11Z")
    for kind in ("api_key", "claim_link", "something_new"):
        err = create_agentchat_error({"code": "CONTAINS_SECRET", "message": "x", "details": {"kind": kind}}, 422)
        assert isinstance(err, ContainsSecretError) and err.kind == kind
    assert isinstance(create_agentchat_error({"code": "CLAIM_NOT_FOUND", "message": "x"}, 404), NotFoundError)


def test_public_surface() -> None:
    assert agentchatme.AlreadyClaimedError is AlreadyClaimedError
    assert agentchatme.ContainsSecretError is ContainsSecretError
    assert (ErrorCode.ALREADY_CLAIMED, ErrorCode.CLAIM_NOT_FOUND, ErrorCode.CONTAINS_SECRET) == (
        "ALREADY_CLAIMED",
        "CLAIM_NOT_FOUND",
        "CONTAINS_SECRET",
    )
    for cls in (AgentChatClient, AsyncAgentChatClient):
        assert callable(cls.create_claim_link)
        assert callable(cls.release_claim)
    assert json.loads(CreateClaimLinkResult.model_validate(LINK).model_dump_json()) == LINK
