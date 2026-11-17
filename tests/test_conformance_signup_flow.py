"""Contract v1 conformance: ``conformance/v1/signup-flow.cases.json``
(contract v1, "Sign-up flows").

A byte-identical copy of the contract's fixture (the manifest check in
``test_conformance_v1.py`` covers it). Each case: the request a client made,
the server's response, and what the client derives (the flow object, the
code, the message to show, the action). Nothing here restates a case; the
expectations come from the file.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from agentchatme import AgentChatClientIdentity, AgentChatError, create_agentchat_error
from agentchatme.integration import account
from agentchatme.types import SignupFlow

FIXTURE = Path(__file__).resolve().parent.parent / "conformance" / "v1" / "signup-flow.cases.json"
DATA: dict[str, Any] = json.loads(FIXTURE.read_bytes().decode("utf-8"))
CASES: list[dict[str, Any]] = DATA["cases"]
API = "https://api.agentchat.test"
IDENTITY = AgentChatClientIdentity(name="python_sdk")


def _is_error(case: dict[str, Any]) -> bool:
    return isinstance(case["response"]["body"].get("code"), str)


def _surfaced(case: dict[str, Any]) -> Any:
    """The response as the SDK surfaces it: the body of a 2xx, the SDK error otherwise."""
    response = case["response"]
    if not _is_error(case):
        return response["body"]
    return create_agentchat_error(
        response["body"], response["status"], httpx.Headers(response["headers"])
    )


def _expect_flow(actual: SignupFlow | None, expected: dict[str, Any] | None) -> None:
    """Every flow field the fixture lists, with its value; a parser may keep more."""
    if expected is None:
        assert actual is None
        return
    assert actual is not None
    for key, value in expected.items():
        assert dict(actual).get(key) == value, key


# ─────────────── the vocabulary ───────────────


def test_names_the_same_actions_steps_and_wait_limit_as_the_sdk() -> None:
    assert DATA["version"] == 1
    sdk_actions = ["done", "ask_code", "ask_param", "offer_new_code", "wait_then_retry", "stop", "legacy"]
    assert sorted(DATA["actions"]) == sorted(sdk_actions)
    assert DATA["wait_limit_s"] == account.MAX_WAIT_S
    assert DATA["start_steps"] == {"register": "register", "recover": "recover", "rotate_key": "rotate_key"}
    assert DATA["verify_steps"] == {
        "register": "verify",
        "recover": "recover_verify",
        "rotate_key": "rotate_key_verify",
    }


def test_covers_every_action() -> None:
    seen = {case["expect"]["action"] for case in CASES}
    assert set(DATA["actions"]) <= seen


# ─────────────── flow_of / action_of ───────────────


@pytest.mark.parametrize("case", CASES, ids=[c["name"] for c in CASES])
def test_flow_of_and_action_of(case: dict[str, Any]) -> None:
    expect = case["expect"]
    response = case["response"]

    # From the raw body (code that calls the endpoints itself).
    from_body = account.flow_of(response["body"])
    _expect_flow(from_body, expect["flow"])
    assert account.action_of(from_body, response["status"]) == expect["action"]

    # From what the SDK surfaces (the body, or the SDK error).
    shown = _surfaced(case)
    from_sdk = account.flow_of(shown)
    assert from_sdk == from_body
    if _is_error(case):
        assert isinstance(shown, AgentChatError)
        assert shown.code == expect["code"]
        assert str(shown) == expect["message"]
        assert shown.details == response["body"].get("details")
    else:
        assert expect["code"] is None
        assert response["body"].get("message") == expect["message"]


# ─────────────── the steps, against the fixture's responses ───────────────

_STEP_KINDS: dict[str, tuple[account.AccountKind, str]] = {
    "register": ("register", "start"),
    "verify": ("register", "verify"),
    "recover": ("recover", "start"),
    "recover_verify": ("recover", "verify"),
}

# What the SDK reads from an older server's answer (the fixture says only
# ``legacy``). The same readings as the TypeScript SDK's.
_LEGACY_READING: dict[str, tuple[str, dict[str, Any]]] = {
    "legacy-register-code-sent": (
        "ask_code",
        {"version": 1, "flow": "register", "next_step": "verify", "status": "pending"},
    ),
    "legacy-verify-wrong-code": (
        "ask_code",
        {"version": 1, "flow": "register", "next_step": "verify", "status": "pending", "param": "code"},
    ),
    "legacy-cooldown": (
        "wait_then_retry",
        {"version": 1, "flow": "register", "next_step": "register", "retry_after_s": 42},
    ),
    "legacy-validation-error": (
        "ask_param",
        {"version": 1, "flow": "register", "next_step": "register", "param": "handle"},
    ),
}

STEP_CASES = [c for c in CASES if c["request"]["kind"] in _STEP_KINDS]


def test_drives_every_register_and_recover_case() -> None:
    # Rotate key is the client's rotate_key / rotate_key_verify.
    assert STEP_CASES
    rest = [c for c in CASES if c["request"]["kind"] not in _STEP_KINDS]
    for case in rest:
        assert case["request"]["kind"].startswith("rotate_key"), case["name"]


def _client_for(case: dict[str, Any], seen: list[dict[str, Any]]) -> httpx.Client:
    response = case["response"]

    def answer(request: httpx.Request) -> httpx.Response:
        seen.append(
            {
                "method": request.method,
                "path": request.url.path,
                "body": json.loads(request.content) if request.content else None,
            }
        )
        return httpx.Response(
            response["status"],
            headers={"content-type": "application/json", **response["headers"]},
            content=json.dumps(response["body"]).encode("utf-8"),
        )

    return httpx.Client(transport=httpx.MockTransport(answer))


@pytest.mark.parametrize("case", STEP_CASES, ids=[c["name"] for c in STEP_CASES])
def test_start_and_submit_code(case: dict[str, Any]) -> None:
    kind, step = _STEP_KINDS[case["request"]["kind"]]
    expect = case["expect"]
    body = case["request"]["body"]
    seen: list[dict[str, Any]] = []
    options: dict[str, Any] = {
        "client_identity": IDENTITY,
        "api_base": API,
        "http_client": _client_for(case, seen),
    }

    if step == "verify" and not isinstance(body.get("pending_id"), str):
        # A request the SDK cannot make (no pending id: a programmer error)
        # is refused before anything is sent.
        flow: Any = {"version": 1, "flow": kind, "next_step": case["request"]["kind"]}
        with pytest.raises(ValueError):
            account.submit_code(flow, str(body.get("code", "")), **options)
        assert seen == []
        return
    if step == "start":
        result = account.start(kind, body, **options)
    else:
        flow = {
            "version": 1,
            "flow": kind,
            "next_step": case["request"]["kind"],
            "pending_id": body["pending_id"],
        }
        result = account.submit_code(flow, body["code"], **options)

    # The request is the fixture's: one call, never retried.
    assert seen == [
        {"method": case["request"]["method"], "path": case["request"]["path"], "body": body}
    ]
    assert result.message == expect["message"]
    assert result.code == expect["code"]
    assert result.http_status == case["response"]["status"]

    if expect["action"] == "legacy":
        assert case["name"] in _LEGACY_READING, f"add the SDK's reading of {case['name']}"
        action, reading = _LEGACY_READING[case["name"]]
        assert result.legacy is True
        assert result.action == action
        _expect_flow(result.flow, reading)
        return
    assert result.legacy is False
    assert result.action == expect["action"]
    _expect_flow(result.flow, expect["flow"])
    if expect["action"] == "done":
        served = case["response"]["body"]
        assert result.api_key == served["api_key"]
        handle = served.get("handle") or (served.get("agent") or {}).get("handle")
        assert result.handle == handle
    else:
        assert result.api_key is None
    if result.action == "ask_code":
        assert result.flow.get("pending_id") == (
            body.get("pending_id") or (expect["flow"] or {}).get("pending_id")
        )
