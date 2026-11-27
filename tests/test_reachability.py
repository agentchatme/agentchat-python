"""Reachability (contract v1 §3.12), ``settings.wake_mode`` and the listener
report (§3.13) on both clients and in ``integration.account``. Mirrors the
TypeScript coverage (``tests/reachability.test.ts``)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest
import respx

import agentchatme
from agentchatme import (
    AgentChatClient,
    AsyncAgentChatClient,
    CallOptions,
    ErrorCode,
    NotFoundError,
    RateLimitedError,
    RetryPolicy,
    ServerError,
    UnauthorizedError,
    ValidationError,
)
from agentchatme.integration import account
from agentchatme.types import Agent, InboxTray, ListenerReport, Reachability

BASE = "https://api.test"
FAST = RetryPolicy(max_retries=2, base_delay_ms=1, max_delay_ms=2)
IDENTITY = agentchatme.AgentChatClientIdentity(name="python_sdk")
FIXTURES = Path(__file__).resolve().parent.parent / "conformance" / "v1"
EXAMPLES: dict[str, dict[str, Any]] = json.loads(
    (FIXTURES / "reachability.v1.json").read_text(encoding="utf-8")
)["agent"]
NONE = EXAMPLES["none"]

# A reachability a later server might send: a state and a field this SDK does not know.
LATER: dict[str, Any] = {
    **EXAMPLES["failing_listener_not_ready"],
    "state": "hibernating",
    "problem": {"kind": "listener_sleepy", "since": None, "text": "A later problem kind."},
    "extra_field": {"nested": True},
}

ME: dict[str, Any] = {
    "id": "agt_1",
    "handle": "example-relay",
    "email": "r***@example.com",
    "display_name": None,
    "description": None,
    "avatar_url": None,
    "status": "active",
    "paused_by_owner": "none",
    "settings": {"inbox_mode": "open", "group_invite_policy": "open", "wake_mode": "when_run"},
    "created_at": "2026-11-28T09:30:00.000Z",
    "updated_at": "2026-11-28T09:30:00.000Z",
    "wake_webhook": None,
    "reachability": LATER,
}

DONE: dict[str, Any] = {
    "agent": {"handle": "example-relay"},
    "api_key": "ac_example-not-a-real-key",
    "flow": "register",
    "next_step": "done",
    "status": "approved",
    "version": 1,
    "message": "Email verified. Logged in as @example-relay.",
    "reachability": NONE,
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


# ─────────────── get_me ───────────────


@respx.mock
def test_get_me_reachability_and_wake_mode_parse_unknown_values_kept() -> None:
    _capture(respx.get(f"{BASE}/v1/agents/me"), 200, ME)
    raw = _client().get_me()
    assert raw["reachability"] == LATER
    agent = Agent.model_validate(raw)
    assert agent.settings.wake_mode == "when_run"
    assert isinstance(agent.reachability, Reachability)
    assert agent.reachability.state == "hibernating"
    assert agent.reachability.problem is not None
    assert agent.reachability.problem.kind == "listener_sleepy"
    assert agent.reachability.text == EXAMPLES["failing_listener_not_ready"]["text"]
    assert agent.reachability.model_dump(mode="json") == LATER  # extra fields kept


def test_agent_parses_without_reachability_and_with_one_it_cannot_read() -> None:
    older = {k: v for k, v in ME.items() if k != "reachability"}
    older["settings"] = {"inbox_mode": "open", "group_invite_policy": "open"}
    agent = Agent.model_validate(older)
    assert agent.reachability is None
    assert agent.settings.wake_mode is None
    for unreadable in ({"state": "ready"}, "ready", ["ready"], {"version": 1, "text": 5}):
        assert Agent.model_validate({**ME, "reachability": unreadable}).reachability is None


# ─────────────── update_agent ───────────────


@respx.mock
def test_update_agent_patches_only_the_wake_mode() -> None:
    seen = _capture(respx.patch(f"{BASE}/v1/agents/example-relay"), 200, ME)
    _client().update_agent("example-relay", {"settings": {"wake_mode": "when_run"}})
    assert len(seen) == 1
    assert json.loads(seen[0].content) == {"settings": {"wake_mode": "when_run"}}


# ─────────────── the sign-up done answers ───────────────


@respx.mock
def test_verify_exposes_reachability_and_next_step_stays_done() -> None:
    _capture(respx.post(f"{BASE}/v1/register/verify"), 201, DONE)
    result = AgentChatClient.verify("pnd_1", "123456", base_url=BASE)
    assert result[1] == DONE["api_key"]
    assert isinstance(result.reachability, Reachability)
    assert result.reachability.model_dump(mode="json") == NONE
    assert result.next_step == "done"


@pytest.mark.parametrize("reachability", [None, "none", ["none"], {"state": "none"}])
def test_verify_without_a_readable_reachability_keeps_the_key(reachability: Any) -> None:
    with respx.mock:
        _capture(respx.post(f"{BASE}/v1/register/verify"), 201, {**DONE, "reachability": reachability})
        result = AgentChatClient.verify("pnd_1", "123456", base_url=BASE)
    assert result[1] == DONE["api_key"]
    assert result.reachability is None


@respx.mock
def test_recover_verify_exposes_reachability_unknown_values_kept() -> None:
    body = {**DONE, "flow": "recover", "handle": "example-relay", "reachability": LATER}
    del body["agent"]
    _capture(respx.post(f"{BASE}/v1/agents/recover/verify"), 200, body)
    result = AgentChatClient.recover_verify("pnd_1", "123456", base_url=BASE)
    assert result[0] == "example-relay"
    assert result.reachability is not None
    assert result.reachability.state == "hibernating"


@respx.mock
async def test_async_verify_and_recover_verify_expose_reachability() -> None:
    _capture(respx.post(f"{BASE}/v1/register/verify"), 201, DONE)
    body = {**DONE, "handle": "example-relay", "reachability": EXAMPLES["offline"]}
    _capture(respx.post(f"{BASE}/v1/agents/recover/verify"), 200, body)
    registered = await AsyncAgentChatClient.verify("pnd_1", "123456", base_url=BASE)
    recovered = await AsyncAgentChatClient.recover_verify("pnd_1", "123456", base_url=BASE)
    assert registered.reachability is not None and registered.reachability.state == "none"
    assert recovered.reachability is not None and recovered.reachability.state == "offline"


@respx.mock
def test_rotate_key_verify_passes_reachability_through() -> None:
    body = {"flow": "rotate_key", "next_step": "done", "handle": "example-relay", "api_key": "ac_new", "reachability": EXAMPLES["offline"]}
    _capture(respx.post(f"{BASE}/v1/agents/example-relay/rotate-key/verify"), 200, body)
    assert _client().rotate_key_verify("example-relay", "pnd_1", "123456")["reachability"] == EXAMPLES["offline"]


@respx.mock
def test_account_submit_code_and_run_carry_it_on_done() -> None:
    flow: Any = {"version": 1, "flow": "register", "next_step": "verify", "pending_id": "pnd_1"}
    route = respx.post(f"{BASE}/v1/register/verify")
    _capture(route, 201, DONE)
    result = account.submit_code(flow, "123456", client_identity=IDENTITY, api_base=BASE)
    assert result.action == "done"
    assert result.reachability is not None
    assert result.reachability.model_dump(mode="json") == NONE

    started = {"pending_id": "pnd_1", "flow": "register", "next_step": "verify", "status": "pending", "version": 1, "code_length": 6, "message": "We sent a code."}
    _capture(respx.post(f"{BASE}/v1/register"), 200, started)
    ran = account.run(
        kind="register",
        input={"email": "relay@example.com", "handle": "example-relay"},
        ask=lambda _q: account.AccountAnswer("value", "123456"),
        show=lambda _m: None,
        client_identity=IDENTITY,
        api_base=BASE,
    )
    assert (ran.outcome, ran.api_key, ran.handle) == ("done", DONE["api_key"], "example-relay")
    assert ran.reachability is not None and ran.reachability.state == "none"

    older = {k: v for k, v in DONE.items() if k != "reachability"}
    _capture(route, 201, older)
    assert account.submit_code(flow, "123456", client_identity=IDENTITY, api_base=BASE).reachability is None


@respx.mock
def test_a_reachability_on_an_answer_that_is_not_done_is_not_carried() -> None:
    wrong = {"code": "INVALID_CODE", "message": "That code didn't work.", "details": {"flow": "register", "next_step": "verify", "status": "pending", "version": 1, "param": "code", "reachability": NONE}}
    _capture(respx.post(f"{BASE}/v1/register/verify"), 400, wrong)
    flow: Any = {"version": 1, "flow": "register", "next_step": "verify", "pending_id": "pnd_1"}
    result = account.submit_code(flow, "000000", client_identity=IDENTITY, api_base=BASE)
    assert result.action == "ask_code"
    assert result.reachability is None


# ─────────────── report_listener ───────────────

ROUTE = f"{BASE}/v1/agents/me/listener"
RECORD: dict[str, Any] = {
    "instance_id": "inst_0123456789abcdef",
    "state": "runs_failing",
    "reason_kind": "runtime_check_failed",
    "state_since": "2026-11-28T11:45:00.000Z",
    "reported_at": "2026-11-28T12:00:00.000Z",
}


@respx.mock
def test_report_listener_puts_the_documented_body_and_returns_the_record() -> None:
    seen = _capture(respx.put(ROUTE), 200, {"listener": RECORD})
    record = _client().report_listener(
        instance_id="inst_0123456789abcdef",
        state="runs_failing",
        reason_kind="runtime_check_failed",
        opts=CallOptions(timeout_ms=5_000),
    )
    assert isinstance(record, ListenerReport)
    assert record.model_dump(mode="json") == RECORD
    assert len(seen) == 1
    assert seen[0].headers["authorization"] == "Bearer ac_test"
    assert json.loads(seen[0].content) == {
        "instance_id": "inst_0123456789abcdef",
        "state": "runs_failing",
        "reason_kind": "runtime_check_failed",
    }


@respx.mock
def test_report_listener_leaves_reason_kind_out_when_none_is_given() -> None:
    seen = _capture(respx.put(ROUTE), 200, {"listener": {**RECORD, "state": "ok", "reason_kind": None}})
    _client().report_listener(instance_id="inst_0123456789abcdef", state="ok")
    assert json.loads(seen[0].content) == {"instance_id": "inst_0123456789abcdef", "state": "ok"}


@respx.mock
def test_report_listener_is_retried_like_any_put() -> None:
    route = respx.put(ROUTE).mock(
        side_effect=[httpx.Response(502, json={"code": "BAD_GATEWAY", "message": "upstream"}), httpx.Response(200, json={"listener": RECORD})]
    )
    assert _client(retry=FAST).report_listener(instance_id="inst_0123456789abcdef", state="runs_failing").state == "runs_failing"
    assert route.call_count == 2


@respx.mock
def test_listener_reports_unavailable_is_a_server_error_never_retried() -> None:
    route = respx.put(ROUTE).mock(
        side_effect=[
            httpx.Response(503, json={"code": "LISTENER_REPORTS_UNAVAILABLE", "message": "Listener reports are not available right now"}),
            httpx.Response(200, json={"listener": RECORD}),
        ]
    )
    with pytest.raises(ServerError) as info:
        _client(retry=FAST).report_listener(instance_id="inst_0123456789abcdef", state="ok")
    assert (info.value.status, info.value.code) == (503, ErrorCode.LISTENER_REPORTS_UNAVAILABLE)
    assert route.call_count == 1


@pytest.mark.parametrize(
    ("status", "body", "kind"),
    [
        (400, {"code": "VALIDATION_ERROR", "message": "Invalid request"}, ValidationError),
        (413, {"code": "VALIDATION_ERROR", "message": "Request body too large"}, ValidationError),
        (401, {"code": "UNAUTHORIZED", "message": "Invalid API key"}, UnauthorizedError),
        (404, {"code": "NOT_FOUND", "message": "Not found"}, NotFoundError),
    ],
)
def test_report_listener_errors_map_to_the_sdk_errors(status: int, body: dict[str, Any], kind: type[Exception]) -> None:
    with respx.mock:
        _capture(respx.put(ROUTE), status, body)
        with pytest.raises(kind):
            _client(retry=FAST).report_listener(instance_id="inst_0123456789abcdef", state="ok")


@respx.mock
def test_report_listener_429_carries_the_wait() -> None:
    _capture(respx.put(ROUTE), 429, {"code": "RATE_LIMITED", "message": "Too many requests"}, {"Retry-After": "1800"})
    with pytest.raises(RateLimitedError) as info:
        _client().report_listener(instance_id="inst_0123456789abcdef", state="ok")
    assert info.value.retry_after_ms == 1_800_000


@respx.mock
async def test_async_report_listener() -> None:
    seen = _capture(respx.put(ROUTE), 200, {"listener": RECORD})
    async with AsyncAgentChatClient(api_key="ac_test", base_url=BASE) as client:
        record = await client.report_listener(instance_id="inst_0123456789abcdef", state="not_ready", reason_kind="not_signed_in")
    assert record.instance_id == "inst_0123456789abcdef"
    assert json.loads(seen[0].content) == {"instance_id": "inst_0123456789abcdef", "state": "not_ready", "reason_kind": "not_signed_in"}


# ─────────────── tray notices on the receive path ───────────────


def test_a_notice_this_sdk_cannot_read_never_breaks_the_tray() -> None:
    tray = json.loads((FIXTURES / "tray.v1.sample.json").read_text(encoding="utf-8"))
    readable = {"kind": "no_wake_yet", "since": "2026-11-28T09:30:00.000Z", "text": "You joined ..."}
    later = {"kind": "later_kind", "text": "A later notice.", "later_field": [1, 2]}
    # A later kind may use a known field name for a value of another type: still a notice.
    reuses = {"kind": "later_kind_2", "text": "t", "status": "four hundred", "since": 5}
    broken = [{"kind": "no_wake"}, {"text": "no kind"}, "a string", {"kind": 7, "text": "t"}]
    model = InboxTray.model_validate({**tray, "notices": [readable, *broken, later, reuses]})
    assert model.notices is not None
    assert [n.kind for n in model.notices] == ["no_wake_yet", "later_kind", "later_kind_2"]
    assert model.notices[1].text == "A later notice."
    assert model.notices[1].model_dump(mode="json", exclude_unset=True) == later
    assert model.notices[2].model_dump(mode="json", exclude_unset=True) == reuses
    assert InboxTray.model_validate({**tray, "notices": "not a list"}).notices is None


# ─────────────── surface ───────────────


def test_public_surface() -> None:
    assert callable(AgentChatClient.report_listener)
    assert callable(AsyncAgentChatClient.report_listener)
    assert agentchatme.types.LISTENER_REPORT_RESEND_MS == 30 * 60_000
    assert agentchatme.types.LISTENER_REPORT_FRESH_MS == 45 * 60_000
    assert ErrorCode.LISTENER_REPORTS_UNAVAILABLE == "LISTENER_REPORTS_UNAVAILABLE"
    assert callable(agentchatme.integration.ListenerReporter)
    assert callable(agentchatme.integration.listener_instance_id)
