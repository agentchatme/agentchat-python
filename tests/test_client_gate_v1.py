"""Client surfaces added for contract v1: the per-call retry override,
``register`` without ``null`` fields, ``/context`` parity, invite dismiss,
the wake-webhook methods, the ``sync`` long-poll with its headers, the tray
v1 shape, and wake-webhook request verification."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from typing import Any

import httpx
import pytest
import respx

from agentchatme import (
    AgentChatClient,
    AsyncAgentChatClient,
    CallOptions,
    NotFoundError,
    RetryPolicy,
    ServerError,
    WakeWebhookVerificationError,
    verify_wake_webhook,
)
from agentchatme.types import (
    AgentConversationContext,
    ClearWakeWebhookResult,
    DirectConversationLookup,
    InboxTray,
    TestWakeResult,
    Texts,
    WakeWebhookStatus,
)

from ._texts import texts

BASE = "https://api.test"


def _capture(route: Any, status: int, body: Any, headers: dict[str, str] | None = None) -> list[httpx.Request]:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(status, json=body, headers=headers)

    route.mock(side_effect=handler)
    return seen


def _client() -> AgentChatClient:
    return AgentChatClient(api_key="ac_test", base_url=BASE)


# ─────────────── register ───────────────


@respx.mock
def test_register_sends_no_null_optional_fields() -> None:
    seen = _capture(respx.post(f"{BASE}/v1/register"), 200, {"pending_id": "p1"})
    AgentChatClient.register(email="a@example.com", handle="nova", base_url=BASE)
    assert json.loads(seen[0].content) == {"email": "a@example.com", "handle": "nova"}
    AgentChatClient.register(
        email="a@example.com", handle="nova", description="d", display_name="N", base_url=BASE
    )
    assert json.loads(seen[1].content) == {
        "email": "a@example.com",
        "handle": "nova",
        "display_name": "N",
        "description": "d",
    }


def test_static_calls_take_the_clients_http_options() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path == "/v1/register/verify":
            return httpx.Response(200, json={"api_key": "ac_test_key", "agent": {"handle": "example-a"}})
        return httpx.Response(200, json={"pending_id": "p1"})

    http = httpx.Client(transport=httpx.MockTransport(handler))
    AgentChatClient.register(email="a@example.com", handle="example-a", base_url=BASE, http_client=http, timeout_ms=1234)
    _agent, _key, client = AgentChatClient.verify("p1", "123456", base_url=BASE, http_client=http, timeout_ms=1234)
    AgentChatClient.recover("a@example.com", handle="example-a", base_url=BASE, http_client=http)
    assert [r.url.path for r in seen] == ["/v1/register", "/v1/register/verify", "/v1/agents/recover"]
    assert seen[0].extensions["timeout"]["read"] == pytest.approx(1.234)
    assert not http.is_closed  # a given client is never closed by the call
    assert client._http._client is http  # the returned client uses it too
    http.close()


@respx.mock
def test_tray_include_invites_sends_the_flag_only_when_asked() -> None:
    tray = {"you": "@example-a", "question": "q", "categories": [], "conversations": [], "deferred": 0, "truncated": False}
    seen = _capture(respx.get(f"{BASE}/v1/inbox"), 200, tray)
    _client().inbox.tray()
    _client().inbox.tray(include_invites=True, wait_ms=1000)
    assert "invites" not in seen[0].url.params
    assert seen[1].url.params["invites"] == "1" and seen[1].url.params["wait_ms"] == "1000"


@respx.mock
@pytest.mark.asyncio
async def test_async_register_sends_no_null_optional_fields() -> None:
    seen = _capture(respx.post(f"{BASE}/v1/register"), 200, {"pending_id": "p1"})
    await AsyncAgentChatClient.register(email="a@example.com", handle="nova", base_url=BASE)
    assert json.loads(seen[0].content) == {"email": "a@example.com", "handle": "nova"}


# ─────────────── per-call retry ───────────────


@respx.mock
def test_retry_never_makes_one_attempt_on_an_idempotent_get() -> None:
    route = respx.get(f"{BASE}/v1/agents/me").mock(return_value=httpx.Response(503, json={}))
    with pytest.raises(ServerError):
        _client().get_me(CallOptions(retry="never"))
    assert route.call_count == 1


@respx.mock
def test_a_retry_policy_of_its_own_applies_to_one_call() -> None:
    route = respx.get(f"{BASE}/v1/agents/me").mock(
        side_effect=[httpx.Response(503, json={}), httpx.Response(200, json={"ok": True})]
    )
    policy = RetryPolicy(max_retries=1, base_delay_ms=1, max_delay_ms=1)
    assert _client().get_me(CallOptions(retry=policy)) == {"ok": True}
    assert route.call_count == 2


@pytest.mark.parametrize(("key", "calls"), [(None, 1), ("k1", 2)])
def test_a_per_call_retry_never_retries_a_post_without_an_idempotency_key(
    key: str | None, calls: int
) -> None:
    policy = RetryPolicy(max_retries=1, base_delay_ms=1, max_delay_ms=1)
    with respx.mock:
        route = respx.post(f"{BASE}/v1/contacts").mock(
            side_effect=[httpx.Response(503, json={}), httpx.Response(200, json={"ok": True})]
        )
        options = CallOptions(retry=policy, idempotency_key=key)
        if key is None:
            with pytest.raises(ServerError):
                _client().add_contact("@example-peer", options)  # it could have run twice
        else:
            assert _client().add_contact("@example-peer", options) == {"ok": True}
        assert route.call_count == calls


# ─────────────── decisions.record on any 2xx ───────────────


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(204),
        httpx.Response(200, text=""),
        httpx.Response(200, json=None),
        httpx.Response(200, json={"ok": True}),
        httpx.Response(201, json={"decision": {"id": "dec_1", "decision": "reply"}}),
    ],
)
@respx.mock
def test_decisions_record_succeeds_on_any_2xx(response: httpx.Response) -> None:
    respx.post(f"{BASE}/v1/conversations/c1/decisions").mock(return_value=response)
    decision = _client().decisions.record(
        "c1", decision="reply", category="open_request", through_message_id="m1"
    )
    assert decision.conversation_id == "c1" and decision.decision == "reply"
    assert decision.through_message_id == "m1" and decision.category == "open_request"
    if response.status_code == 201:
        assert decision.id == "dec_1"
    else:
        assert decision.id is None and decision.created_at is None


# ─────────────── /context ───────────────

CONTEXT: dict[str, Any] = {
    "conversation_id": "conv_1",
    "type": "direct",
    "group": None,
    "counterparty": {"handle": "peer", "display_name": "Peer", "avatar_url": None},
    "relationship": {"is_contact": True, "added_at": "2026-11-01T00:00:00Z", "note": "met at x"},
    "direct_state": {"state": "established", "initiated_by_self": False, "last_message_at": None},
    "unread": {"count": 2, "oldest_seq": 4, "newest_seq": 5},
}


@respx.mock
def test_get_conversation_context() -> None:
    seen = _capture(respx.get(f"{BASE}/v1/conversations/conv%2F1/context"), 200, CONTEXT)
    ctx = _client().get_conversation_context("conv/1")
    assert isinstance(ctx, AgentConversationContext)
    assert ctx.unread.newest_seq == 5
    assert ctx.relationship is not None and ctx.relationship.note == "met at x"
    assert seen[0].url.raw_path == b"/v1/conversations/conv%2F1/context"


@respx.mock
def test_get_direct_conversation_context_strips_the_at() -> None:
    body = {"state": "cold", "counterparty": {"handle": "peer", "display_name": None}, "conversation": CONTEXT}
    seen = _capture(respx.get(f"{BASE}/v1/conversations/direct/peer/context"), 200, body)
    lookup = _client().get_direct_conversation_context("@peer")
    assert isinstance(lookup, DirectConversationLookup)
    assert lookup.state == "cold"
    assert lookup.conversation is not None and lookup.conversation.conversation_id == "conv_1"
    assert len(seen) == 1


@respx.mock
@pytest.mark.asyncio
async def test_async_context_methods() -> None:
    _capture(respx.get(f"{BASE}/v1/conversations/conv_1/context"), 200, CONTEXT)
    body = {"state": "new", "counterparty": {"handle": "peer", "display_name": None}, "conversation": None}
    _capture(respx.get(f"{BASE}/v1/conversations/direct/peer/context"), 200, body)
    async with AsyncAgentChatClient(api_key="ac_test", base_url=BASE) as client:
        assert (await client.get_conversation_context("conv_1")).type == "direct"
        assert (await client.get_direct_conversation_context("peer")).conversation is None


# ─────────────── invite dismiss ───────────────


@respx.mock
def test_dismiss_invite_posts_with_an_idempotency_key() -> None:
    seen = _capture(respx.post(f"{BASE}/v1/groups/invites/inv_1/dismiss"), 200, {"dismissed": True})
    assert _client().groups.dismiss_invite("inv_1").dismissed is True
    key = seen[0].headers.get("Idempotency-Key")
    assert key
    _client().groups.dismiss_invite("inv_1", CallOptions(idempotency_key="mine"))
    assert seen[1].headers.get("Idempotency-Key") == "mine"


@respx.mock
def test_dismiss_invite_retries_a_transient_failure_under_its_key() -> None:
    route = respx.post(f"{BASE}/v1/groups/invites/inv_1/dismiss").mock(
        side_effect=[httpx.Response(503, json={}), httpx.Response(200, json={"dismissed": True})]
    )
    client = AgentChatClient(
        api_key="ac_test",
        base_url=BASE,
        retry=RetryPolicy(max_retries=1, base_delay_ms=1, max_delay_ms=1),
    )
    assert client.groups.dismiss_invite("inv_1").dismissed is True
    keys = {call.request.headers.get("Idempotency-Key") for call in route.calls}
    assert len(keys) == 1 and None not in keys


@respx.mock
def test_dismiss_an_unknown_invite_is_not_found() -> None:
    respx.post(f"{BASE}/v1/groups/invites/inv_x/dismiss").mock(
        return_value=httpx.Response(404, json={"code": "INVITE_NOT_FOUND", "message": "no"})
    )
    with pytest.raises(NotFoundError):
        _client().groups.dismiss_invite("inv_x")


@respx.mock
@pytest.mark.asyncio
async def test_async_dismiss_invite() -> None:
    seen = _capture(respx.post(f"{BASE}/v1/groups/invites/inv_1/dismiss"), 200, {"dismissed": True})
    async with AsyncAgentChatClient(api_key="ac_test", base_url=BASE) as client:
        assert (await client.groups.dismiss_invite("inv_1")).dismissed is True
    assert seen[0].headers.get("Idempotency-Key")


# ─────────────── wake webhook ───────────────

STATUS: dict[str, Any] = {
    "url": "https://hooks.example.com/wake",
    "dialect": "hermes",
    "prompt": None,
    "state": "active",
    "consecutive_failures": 0,
    "last_attempt_at": None,
    "last_success_at": None,
    "last_error": None,
    "created_at": "2026-11-08T00:00:00Z",
    "rewake": True,
}


@respx.mock
def test_set_wake_webhook_sends_rewake_only_when_given() -> None:
    seen = _capture(respx.put(f"{BASE}/v1/agents/me/wake-webhook"), 200, {"wake_webhook": STATUS})
    status = _client().wake_webhook.set(
        url="https://hooks.example.com/wake", secret="s" * 32, dialect="hermes"
    )
    assert isinstance(status, WakeWebhookStatus) and status.rewake is True
    assert json.loads(seen[0].content) == {
        "url": "https://hooks.example.com/wake",
        "secret": "s" * 32,
        "dialect": "hermes",
    }
    _client().wake_webhook.set(url="https://h.example.com", secret="s" * 32, rewake=False, prompt="p")
    assert json.loads(seen[1].content) == {
        "url": "https://h.example.com",
        "secret": "s" * 32,
        "prompt": "p",
        "rewake": False,
    }


@respx.mock
def test_test_and_clear_wake_webhook() -> None:
    result = {
        "ok": True,
        "dialect": "hermes",
        "url_host": "hooks.example.com",
        "status": 202,
        "duration_ms": 41,
        "error": None,
        "delivery_id": "8d1c2d0e-0000-4000-8000-000000000000",
    }
    _capture(respx.post(f"{BASE}/v1/agents/me/wake-webhook/test"), 200, {"test_wake": result})
    _capture(respx.delete(f"{BASE}/v1/agents/me/wake-webhook"), 200, {"ok": True, "removed": True})
    client = _client()
    tested = client.wake_webhook.test()
    assert isinstance(tested, TestWakeResult) and tested.status == 202
    cleared = client.wake_webhook.clear()
    assert isinstance(cleared, ClearWakeWebhookResult) and cleared.removed is True


@respx.mock
@pytest.mark.asyncio
async def test_async_wake_webhook_methods() -> None:
    _capture(respx.put(f"{BASE}/v1/agents/me/wake-webhook"), 200, {"wake_webhook": STATUS})
    _capture(respx.delete(f"{BASE}/v1/agents/me/wake-webhook"), 200, {"ok": True, "removed": False})
    async with AsyncAgentChatClient(api_key="ac_test", base_url=BASE) as client:
        status = await client.wake_webhook.set(url="https://h.example.com", secret="s" * 32)
        assert status.state == "active"
        assert (await client.wake_webhook.clear()).removed is False


# ─────────────── sync long-poll ───────────────


@respx.mock
def test_sync_long_poll_stretches_the_timeout_and_exposes_the_headers() -> None:
    seen = _capture(
        respx.get(f"{BASE}/v1/messages/sync"),
        200,
        [],
        headers={"X-AgentChat-Waited-Ms": "25000"},
    )
    page = _client().sync_page(wait_ms=25_000, limit=10)
    assert page.rows == [] and page.waited_ms == 25_000 and page.poll_interval_seconds is None
    assert seen[0].url.params["wait_ms"] == "25000"
    assert seen[0].url.params["limit"] == "10"
    assert seen[0].extensions["timeout"]["read"] >= 35.0


@respx.mock
def test_plain_sync_sends_no_wait_and_reads_the_idle_hint() -> None:
    seen = _capture(
        respx.get(f"{BASE}/v1/messages/sync"),
        200,
        [],
        headers={"X-AgentChat-Poll-Interval-Seconds": "300"},
    )
    page = _client().sync_page()
    assert "wait_ms" not in seen[0].url.params
    assert page.poll_interval_seconds == 300
    assert _client().sync(wait_ms=0) == []
    assert "wait_ms" not in seen[1].url.params


def test_sync_rejects_a_wait_out_of_range() -> None:
    with pytest.raises(ValueError):
        _client().sync(wait_ms=60_001)
    with pytest.raises(TypeError):
        _client().sync(wait_ms=True)


@respx.mock
@pytest.mark.asyncio
async def test_async_sync_long_poll() -> None:
    row = {"id": "m1", "conversation_id": "c", "delivery_id": "del_1"}
    seen = _capture(respx.get(f"{BASE}/v1/messages/sync"), 200, [row])
    async with AsyncAgentChatClient(api_key="ac_test", base_url=BASE) as client:
        assert await client.sync(wait_ms=1000) == [row]
    assert seen[0].url.params["wait_ms"] == "1000"


# ─────────────── tray v1, texts ───────────────


def test_tray_v1_with_invites_parses() -> None:
    tray = InboxTray.model_validate(
        {
            "version": 1,
            "you": "@nova",
            "question": "q",
            "categories": ["other"],
            "conversations": [],
            "deferred": 0,
            "truncated": False,
            "invites": [
                {
                    "invite_id": "inv_1",
                    "group": {"id": "grp_1", "name": "crew", "member_count": 4},
                    "invited_by": "@peer",
                    "at": "2026-11-08T00:00:00Z",
                }
            ],
            "invites_truncated": False,
        }
    )
    assert tray.version == 1
    assert tray.invites[0].group.name == "crew"


def test_a_tray_from_an_older_server_has_no_invites() -> None:
    tray = InboxTray.model_validate(
        {"you": "@nova", "question": "q", "categories": [], "conversations": [], "deferred": 0, "truncated": False}
    )
    assert tray.invites is None and tray.version is None


def test_texts_carry_the_gate_call_fields_and_the_manual_hash() -> None:
    parsed = Texts.model_validate(texts())
    assert parsed.gate_call_preamble and parsed.gate_call_answer_format
    assert parsed.manual.sha256 == "0" * 64


# ─────────────── wake webhook verification ───────────────
# The shared cases are in conformance/v1/wake-webhook.cases.json; these cover
# the Python header shapes.

WAKE: dict[str, Any] = {
    "type": "agentchat.wake",
    "version": 1,
    "handle": "@nova",
    "conversations": [{"id": "conv_1", "unread": 2, "latest_sender": "@peer"}],
    "summary": "2 unread from @peer",
    "delivery_id": "8d1c2d0e-0000-4000-8000-000000000000",
    "ts": "2026-11-08T00:00:00Z",
}
BODY = json.dumps(WAKE)
SECRET = "wake-secret-0123456789"


def test_verify_takes_httpx_headers_and_list_values() -> None:
    ts = "1762560000"
    sig = hmac.new(SECRET.encode(), f"{ts}.{BODY}".encode(), hashlib.sha256).hexdigest()
    headers = httpx.Headers({"X-Webhook-Timestamp": ts, "X-Webhook-Signature-V2": sig})
    payload = verify_wake_webhook(
        dialect="hermes", body=BODY, headers=headers, secret=SECRET, now=lambda: 1762560100
    )
    assert payload.handle == "@nova"
    listed = {"authorization": [f"Bearer {SECRET}"]}
    assert verify_wake_webhook(dialect="agentchat", body=BODY.encode(), headers=listed, secret=SECRET)


def test_verify_standard_webhooks_raw_and_whsec_secrets() -> None:
    for secret in (SECRET, "whsec_" + base64.b64encode(b"k" * 24).decode()):
        key = base64.b64decode(secret[6:]) if secret.startswith("whsec_") else secret.encode()
        mid, ts = "8d1c2d0e:1", "1762560000"
        sig = base64.b64encode(hmac.new(key, f"{mid}.{ts}.{BODY}".encode(), hashlib.sha256).digest()).decode()
        headers = {"webhook-id": mid, "webhook-timestamp": ts, "webhook-signature": f"v1,x v1,{sig}"}
        payload = verify_wake_webhook(
            dialect="standard-webhooks", body=BODY, headers=headers, secret=secret, now=lambda: 1762560000
        )
        assert payload.version == 1


def test_verify_refuses_an_unknown_dialect_and_a_partial_wake() -> None:
    with pytest.raises(WakeWebhookVerificationError) as unknown:
        verify_wake_webhook(dialect="poke", body=BODY, headers={}, secret=SECRET)
    assert unknown.value.reason == "malformed_signature"
    partial = json.dumps({"type": "agentchat.wake"})
    with pytest.raises(WakeWebhookVerificationError) as bad:
        verify_wake_webhook(
            dialect="agentchat", body=partial, headers={"Authorization": f"Bearer {SECRET}"}, secret=SECRET
        )
    assert bad.value.reason == "malformed_payload"
