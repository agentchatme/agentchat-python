"""``agentchatme.integration.account``: scripted conversations against a
scripted server, fake time, no network.

Each server entry answers one request, in order; every request is recorded.
The bodies follow the sign-up flow contract (contract v1, "Sign-up flows")
or, for the legacy cases, what a server before it answered.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Any, Union

import httpx
import pytest

import agentchatme
from agentchatme import (
    AgentChatClient,
    AgentChatClientIdentity,
    AsyncAgentChatClient,
    ConnectionError,
    RateLimitedError,
    create_agentchat_error,
)
from agentchatme.integration import account
from agentchatme.integration.account import AccountAnswer, AccountQuestion

API = "https://api.agentchat.test"
IDENTITY = AgentChatClientIdentity(name="hermes", version="9.9.9")

# ─────────────── a scripted server ───────────────

Entry = Union[httpx.Response, Exception]


@dataclass
class Server:
    answers: list[Entry]
    seen: list[dict[str, Any]] = field(default_factory=list)

    def _answer(self, request: httpx.Request) -> httpx.Response:
        self.seen.append(
            {
                "method": request.method,
                "path": request.url.path,
                "body": json.loads(request.content) if request.content else None,
                "headers": request.headers,
            }
        )
        if not self.answers:
            raise AssertionError(f"server: unexpected request {request.url.path}")
        entry = self.answers.pop(0)
        if isinstance(entry, Exception):
            raise entry
        return entry

    def client(self) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(self._answer))

    def async_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self._answer))

    def paths(self) -> list[str]:
        return [s["path"] for s in self.seen]

    def bodies(self) -> list[Any]:
        return [s["body"] for s in self.seen]


def respond(status: int, body: Any, headers: dict[str, str] | None = None) -> httpx.Response:
    return httpx.Response(
        status,
        headers={"content-type": "application/json", **(headers or {})},
        content=json.dumps(body).encode("utf-8"),
    )


def verify_step(kind: str) -> str:
    return "verify" if kind == "register" else "recover_verify"


def pending(pending_id: str = "pnd_1", **over: Any) -> dict[str, Any]:
    return {
        "status": "pending",
        "pending_id": pending_id,
        "expires_at": "2026-11-17T10:10:00.000Z",
        "expires_in_s": 600,
        "code_length": 6,
        "attempts": 0,
        "max_attempts": 5,
        "resend_after_s": 60,
        **over,
    }


SENT_TEXT = "We sent a 6-digit code to a@example.com. It expires in 10 minutes."


def sent(kind: str = "register", pending_id: str = "pnd_1", message: str = SENT_TEXT, **over: Any) -> httpx.Response:
    return respond(
        200,
        {
            "pending_id": pending_id,
            "message": message,
            "version": 1,
            "flow": kind,
            "next_step": verify_step(kind),
            **pending(pending_id, **over),
        },
    )


def error(
    status: int, code: str, message: str, details: dict[str, Any], headers: dict[str, str] | None = None
) -> httpx.Response:
    return respond(status, {"code": code, "message": message, "details": {"version": 1, **details}}, headers)


def wrong_code(attempts: int = 1, kind: str = "register") -> httpx.Response:
    return error(
        400,
        "INVALID_CODE",
        "That code didn't work. Check the code and try again.",
        {"flow": kind, "next_step": verify_step(kind), **pending("pnd_1", attempts=attempts), "param": "code"},
    )


def registered(handle: str = "alice") -> httpx.Response:
    return respond(
        201,
        {
            "agent": {"handle": handle, "email": "a@example.com"},
            "api_key": "key_test_1",
            "message": f"Email verified. Logged in as @{handle}.",
            "version": 1,
            "flow": "register",
            "next_step": "done",
            "status": "approved",
        },
    )


def recovered(handle: str = "alice") -> httpx.Response:
    return respond(
        200,
        {
            "handle": handle,
            "api_key": "key_test_2",
            "message": f"Email verified. Logged in as @{handle}. Your old API key no longer works.",
            "version": 1,
            "flow": "recover",
            "next_step": "done",
            "status": "approved",
        },
    )


def cooldown(seconds: int, kind: str = "register") -> httpx.Response:
    return error(
        429,
        "OTP_COOLDOWN",
        f"You can request a new code in {seconds} seconds.",
        {"flow": kind, "next_step": kind, "retry_after_s": seconds},
        {"Retry-After": str(seconds)},
    )


# ─────────────── a scripted person, on fake time ───────────────


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Iterator[Clock]:
    fake = Clock()
    monkeypatch.setattr(account, "_now", fake)
    yield fake


@dataclass
class Person:
    answers: list[AccountAnswer]
    clock: Clock | None = None
    think_s: float = 0.0
    """Fake seconds each answer takes."""
    asked: list[AccountQuestion] = field(default_factory=list)
    shown: list[str] = field(default_factory=list)
    slept: list[float] = field(default_factory=list)

    def ask(self, question: AccountQuestion) -> AccountAnswer:
        self.asked.append(question)
        if not self.answers:
            raise AssertionError(f"person: unexpected question ({question.field})")
        if self.clock is not None:
            self.clock.now += self.think_s
        return self.answers.pop(0)

    def show(self, message: str) -> None:
        self.shown.append(message)

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        if self.clock is not None:
            self.clock.now += seconds


def value(text: str) -> AccountAnswer:
    return AccountAnswer("value", text)


NEW_CODE = AccountAnswer("new_code")
CANCEL = AccountAnswer("cancel")


def run_with(server: Server, person: Person, **over: Any) -> account.RunResult:
    options: dict[str, Any] = {
        "kind": "register",
        "input": {"email": "a@example.com", "handle": "alice"},
        "ask": person.ask,
        "show": person.show,
        "sleep": person.sleep,
        "client_identity": IDENTITY,
        "api_base": API,
        "http_client": server.client(),
        **over,
    }
    return account.run(**options)


# ─────────────── run(): scripted conversations ───────────────


def test_is_reachable_as_integration_account() -> None:
    assert agentchatme.integration.account.run is account.run
    assert account.DEFAULT_LABELS == {
        "email": "Email address",
        "handle": "Handle",
        "code": "Enter the 6-digit code",
    }


def test_enter_the_code_done_with_the_key_and_the_handle_the_server_returned(clock: Clock) -> None:
    server = Server([sent(), registered("alice")])
    person = Person([value(" 123456 ")])
    result = run_with(
        server, person, input={"email": "a@example.com", "handle": "alice", "display_name": "Alice"}
    )

    assert result == account.RunResult(outcome="done", api_key="key_test_1", handle="alice")
    assert person.shown == [SENT_TEXT, "Email verified. Logged in as @alice."]
    assert [(q.field, q.label, q.answers) for q in person.asked] == [
        ("code", "Enter the 6-digit code", ("value", "new_code", "cancel"))
    ]
    flow = person.asked[0].flow
    assert flow is not None
    assert (flow["next_step"], flow.get("pending_id"), flow.get("resend_after_s")) == ("verify", "pnd_1", 60)
    assert server.paths() == ["/v1/register", "/v1/register/verify"]
    assert server.bodies() == [
        {"email": "a@example.com", "handle": "alice", "display_name": "Alice"},
        {"pending_id": "pnd_1", "code": "123456"},
    ]
    for seen in server.seen:
        assert seen["headers"].get("x-agentchat-client") == "hermes"
        assert seen["headers"].get("x-agentchat-client-version") == "9.9.9"
        assert seen["headers"].get("authorization") is None
    assert "key_test_1" not in repr(result)


def test_wrong_code_then_the_right_one_same_pending_id(clock: Clock) -> None:
    server = Server([sent(), wrong_code(1), registered()])
    person = Person([value("111111"), value("123456")])
    result = run_with(server, person)

    assert result.outcome == "done"
    assert person.shown[1] == "That code didn't work. Check the code and try again."
    assert [q.field for q in person.asked] == ["code", "code"]
    second = person.asked[1].flow
    assert second is not None and (second.get("attempts"), second.get("max_attempts")) == (1, 5)
    assert server.bodies()[1:] == [
        {"pending_id": "pnd_1", "code": "111111"},
        {"pending_id": "pnd_1", "code": "123456"},
    ]


def test_no_attempt_cap_of_its_own(clock: Clock) -> None:
    server = Server([sent(), wrong_code(1), wrong_code(2), wrong_code(3), wrong_code(4), registered()])
    person = Person([value("1"), value("2"), value("3"), value("4"), value("123456")])
    assert run_with(server, person).outcome == "done"
    assert server.answers == []


def test_ask_for_a_new_code_after_the_resend_wait_only_when_asked(clock: Clock) -> None:
    server = Server([sent("register", "pnd_1"), sent("register", "pnd_2"), registered()])
    # The person takes 70 s to ask: resend_after_s (60) has passed, no wait.
    person = Person([NEW_CODE, value("654321")], clock=clock, think_s=70)
    result = run_with(server, person)

    assert result.outcome == "done"
    assert person.slept == []
    assert server.paths() == ["/v1/register", "/v1/register", "/v1/register/verify"]
    assert server.bodies()[1] == server.bodies()[0]
    assert server.bodies()[2] == {"pending_id": "pnd_2", "code": "654321"}


def test_a_new_code_asked_for_inside_resend_after_s_waits_what_is_left(clock: Clock) -> None:
    # A recover asked for again too soon is answered like any other (the
    # decoy design) and sends nothing; the server cannot be asked how long.
    server = Server(
        [
            sent("recover", "pnd_1", "If @alice is registered with a@example.com, we sent a 6-digit code there."),
            sent("recover", "pnd_2", "If @alice is registered with a@example.com, we sent a 6-digit code there."),
            recovered(),
        ]
    )
    person = Person([NEW_CODE, value("654321")], clock=clock, think_s=15)
    result = run_with(server, person, kind="recover", input={"email": "a@example.com", "handle": "alice"})

    assert result.outcome == "done"
    assert person.slept == [45.0]
    assert server.paths() == ["/v1/agents/recover", "/v1/agents/recover", "/v1/agents/recover/verify"]
    assert server.bodies()[2] == {"pending_id": "pnd_2", "code": "654321"}


def test_a_new_code_that_would_wait_over_60_s_stops_with_the_server_message(clock: Clock) -> None:
    server = Server([sent(resend_after_s=1800)])
    person = Person([NEW_CODE], clock=clock)
    result = run_with(server, person)

    assert result == account.RunResult(outcome="stopped", code=None, message=SENT_TEXT)
    assert person.slept == []
    assert len(server.seen) == 1


def test_cancel_at_the_code(clock: Clock) -> None:
    server = Server([sent()])
    person = Person([CANCEL])
    assert run_with(server, person) == account.RunResult(outcome="cancelled")
    assert len(server.seen) == 1


def test_a_wait_of_60_s_or_less_shows_the_sentence_waits_and_repeats_the_call(clock: Clock) -> None:
    server = Server([cooldown(42), sent(), registered()])
    person = Person([value("123456")])
    result = run_with(server, person)

    assert result.outcome == "done"
    assert person.shown[0] == "You can request a new code in 42 seconds."
    assert person.slept == [42.0]
    assert server.paths() == ["/v1/register", "/v1/register", "/v1/register/verify"]


def test_a_wait_of_exactly_60_s_is_waited_out(clock: Clock) -> None:
    server = Server([cooldown(60), sent(), registered()])
    person = Person([value("123456")])
    assert run_with(server, person).outcome == "done"
    assert person.slept == [60.0]


def test_a_wait_over_60_s_stops_with_the_server_sentence_no_wait(clock: Clock) -> None:
    message = "Too many codes have been sent. Enter the last code you received, or try again in 30 minutes."
    server = Server(
        [
            error(
                429,
                "OTP_HOURLY_CAP",
                message,
                {"flow": "register", "next_step": "register", "retry_after_s": 1800},
                {"Retry-After": "1800"},
            )
        ]
    )
    person = Person([])
    result = run_with(server, person)

    assert result == account.RunResult(outcome="stopped", code="OTP_HOURLY_CAP", message=message)
    assert person.shown == [message]
    assert person.slept == []


def test_handle_taken_re_asks_the_handle_only_keeps_the_email(clock: Clock) -> None:
    server = Server(
        [
            error(
                409,
                "HANDLE_TAKEN",
                "The handle @alice is taken. Try another.",
                {"flow": "register", "next_step": "register", "param": "handle"},
            ),
            sent(),
            registered("alice-2"),
        ]
    )
    person = Person([value(" alice-2 "), value("123456")])
    result = run_with(server, person)

    assert result == account.RunResult(outcome="done", api_key="key_test_1", handle="alice-2")
    assert person.shown[0] == "The handle @alice is taken. Try another."
    assert [(q.field, q.label) for q in person.asked] == [
        ("handle", "Handle"),
        ("code", "Enter the 6-digit code"),
    ]
    assert person.asked[0].answers == ("value", "cancel")
    assert server.bodies()[1] == {"email": "a@example.com", "handle": "alice-2"}


def test_the_handle_saved_is_the_servers_not_the_typed_one(clock: Clock) -> None:
    server = Server([sent(), registered("alice")])
    person = Person([value("123456")])
    result = run_with(server, person, input={"email": "a@example.com", "handle": "Alice"})
    assert result.handle == "alice"


def test_email_at_its_limit_re_asks_the_email_only(clock: Clock) -> None:
    server = Server(
        [
            error(
                409,
                "EMAIL_LIMIT_REACHED",
                "This email address already has 10 active agents, the most allowed. Use a different email address.",
                {"limit": 10, "flow": "register", "next_step": "register", "param": "email"},
            ),
            sent(),
            registered(),
        ]
    )
    person = Person([value("b@example.com"), value("123456")])
    assert run_with(server, person).outcome == "done"
    assert [q.field for q in person.asked] == ["email", "code"]
    assert server.bodies()[1] == {"email": "b@example.com", "handle": "alice"}


def test_cancel_while_re_asking_a_field(clock: Clock) -> None:
    server = Server(
        [
            error(
                409,
                "HANDLE_TAKEN",
                "The handle @alice is taken. Try another.",
                {"flow": "register", "next_step": "register", "param": "handle"},
            )
        ]
    )
    assert run_with(server, Person([CANCEL])) == account.RunResult(outcome="cancelled")


def test_expired_asks_before_sending_a_new_code_a_yes_sends_one(clock: Clock) -> None:
    server = Server(
        [
            sent(),
            error(
                400,
                "EXPIRED",
                "That code has expired. Request a new code.",
                {"flow": "register", "next_step": "register", "status": "expired"},
            ),
            sent("register", "pnd_2"),
            registered(),
        ]
    )
    person = Person([value("123456"), NEW_CODE, value("222222")])
    assert run_with(server, person).outcome == "done"
    assert [(q.field, q.label, q.answers) for q in person.asked] == [
        ("code", "Enter the 6-digit code", ("value", "new_code", "cancel")),
        ("new_code", None, ("new_code", "cancel")),
        ("code", "Enter the 6-digit code", ("value", "new_code", "cancel")),
    ]
    assert person.shown[1] == "That code has expired. Request a new code."
    assert server.bodies()[3] == {"pending_id": "pnd_2", "code": "222222"}


def test_expired_and_the_person_declines_a_new_code(clock: Clock) -> None:
    server = Server(
        [
            sent(),
            error(
                400,
                "EXPIRED",
                "That code has expired. Request a new code.",
                {"flow": "register", "next_step": "register", "status": "expired"},
            ),
        ]
    )
    person = Person([value("123456"), CANCEL])
    assert run_with(server, person) == account.RunResult(outcome="cancelled")
    assert len(server.seen) == 2


def test_the_last_wrong_code_asks_before_a_new_code_and_never_resends_unasked(clock: Clock) -> None:
    server = Server(
        [
            sent(),
            error(
                429,
                "OTP_ATTEMPTS_EXHAUSTED",
                "You entered the wrong code too many times. Request a new code.",
                {
                    "flow": "register",
                    "next_step": "register",
                    "status": "max_attempts_reached",
                    "attempts": 5,
                    "max_attempts": 5,
                    "retry_after_s": 0,
                },
                {"Retry-After": "0"},
            ),
        ]
    )
    person = Person([value("999999"), CANCEL])
    assert run_with(server, person) == account.RunResult(outcome="cancelled")
    assert [q.field for q in person.asked] == ["code", "new_code"]
    assert person.slept == []
    assert len(server.seen) == 2


def test_a_yes_to_a_new_code_first_waits_out_retry_after_s(clock: Clock) -> None:
    server = Server(
        [
            sent("recover"),
            error(
                429,
                "OTP_ATTEMPTS_EXHAUSTED",
                "You entered the wrong code too many times. Request a new code.",
                {
                    "flow": "recover",
                    "next_step": "recover",
                    "status": "max_attempts_reached",
                    "attempts": 5,
                    "max_attempts": 5,
                    "retry_after_s": 30,
                },
                {"Retry-After": "30"},
            ),
            sent("recover", "pnd_2"),
            recovered(),
        ]
    )
    person = Person([value("999999"), NEW_CODE, value("123456")])
    result = run_with(server, person, kind="recover", input={"email": "a@example.com", "handle": "alice"})
    assert result.outcome == "done"
    assert person.slept == [30.0]
    assert server.paths()[2] == "/v1/agents/recover"


def test_a_per_ip_429_on_a_code_waits_and_submits_the_same_code_again(clock: Clock) -> None:
    server = Server(
        [
            sent(),
            error(
                429,
                "RATE_LIMITED",
                "Too many attempts. Try again in 30 seconds.",
                {"flow": "register", "next_step": "verify", "retry_after_s": 30},
                {"Retry-After": "30"},
            ),
            registered(),
        ]
    )
    person = Person([value("123456")])
    assert run_with(server, person).outcome == "done"
    assert person.slept == [30.0]
    assert server.bodies()[1:] == [
        {"pending_id": "pnd_1", "code": "123456"},
        {"pending_id": "pnd_1", "code": "123456"},
    ]


def test_a_wait_that_points_to_the_start_step_after_a_code_asks_first(clock: Clock) -> None:
    # A 429 on a code whose wait outlasts it names the start step: repeating
    # it would send a new code nobody asked for.
    server = Server(
        [
            sent(),
            error(
                429,
                "RATE_LIMITED",
                "Too many attempts. Try again in 40 seconds.",
                {"flow": "register", "next_step": "register", "retry_after_s": 40},
                {"Retry-After": "40"},
            ),
            sent("register", "pnd_2"),
            registered(),
        ]
    )
    person = Person([value("123456"), NEW_CODE, value("222222")])
    assert run_with(server, person).outcome == "done"
    assert [q.field for q in person.asked] == ["code", "new_code", "code"]
    assert person.slept == [40.0]
    assert server.paths() == ["/v1/register", "/v1/register/verify", "/v1/register", "/v1/register/verify"]


def test_the_same_declined_cancelled_no_code_sent_no_wait(clock: Clock) -> None:
    server = Server(
        [
            sent(),
            error(
                429,
                "RATE_LIMITED",
                "Too many attempts. Try again in 40 seconds.",
                {"flow": "register", "next_step": "register", "retry_after_s": 40},
                {"Retry-After": "40"},
            ),
        ]
    )
    person = Person([value("123456"), CANCEL])
    assert run_with(server, person) == account.RunResult(outcome="cancelled")
    assert person.slept == []
    assert len(server.seen) == 2


def test_a_malformed_code_is_re_asked_with_the_same_pending_id(clock: Clock) -> None:
    server = Server(
        [
            sent(),
            error(
                400,
                "VALIDATION_ERROR",
                "Enter the code using only 6 digits.",
                {
                    "formErrors": [],
                    "fieldErrors": {"code": ["Invalid"]},
                    "flow": "register",
                    "next_step": "verify",
                    "param": "code",
                    "code_length": 6,
                },
            ),
            registered(),
        ]
    )
    person = Person([value("12a456"), value("123456")])
    assert run_with(server, person).outcome == "done"
    assert person.shown[1] == "Enter the code using only 6 digits."
    assert server.bodies()[2] == {"pending_id": "pnd_1", "code": "123456"}


@pytest.mark.parametrize("next_step", ["register", "verify"])
def test_the_per_email_lock_stops(clock: Clock, next_step: str) -> None:
    message = "You entered the wrong code too many times. Wait 2 hours, then request a new code."
    server = Server(
        [
            sent(),
            respond(
                429,
                {
                    "code": "OTP_EMAIL_LOCKED",
                    "message": message,
                    "retry_after_s": 7200,
                    "details": {"version": 1, "flow": "register", "next_step": next_step, "retry_after_s": 7200},
                },
                {"Retry-After": "7200"},
            ),
        ]
    )
    person = Person([value("123456")])
    assert run_with(server, person) == account.RunResult(
        outcome="stopped", code="OTP_EMAIL_LOCKED", message=message
    )
    assert person.slept == []


def test_the_code_email_could_not_be_sent_stops(clock: Clock) -> None:
    message = "We couldn't send the code. Try again in a few minutes."
    server = Server([error(500, "OTP_FAILED", message, {"flow": "register", "next_step": "register"})])
    assert run_with(server, Person([])) == account.RunResult(
        outcome="stopped", code="OTP_FAILED", message=message
    )


def test_an_unknown_next_step_stops_with_the_message(clock: Clock) -> None:
    server = Server(
        [
            respond(
                200,
                {
                    "pending_id": "pnd_1",
                    "message": "Open the link we sent.",
                    "version": 1,
                    "flow": "register",
                    "next_step": "open_link",
                },
            )
        ]
    )
    assert run_with(server, Person([])) == account.RunResult(
        outcome="stopped", code=None, message="Open the link we sent."
    )


def test_asks_for_the_inputs_it_was_not_given_with_the_labels(clock: Clock) -> None:
    server = Server([sent(), registered()])
    person = Person([value("a@example.com"), value("alice"), value("123456")])
    result = run_with(server, person, input={}, labels={"handle": "Your handle"})
    assert result.outcome == "done"
    assert [(q.field, q.label, q.flow is None) for q in person.asked] == [
        ("email", "Email address", True),
        ("handle", "Your handle", True),
        ("code", "Enter the 6-digit code", False),
    ]


def test_recover_sends_the_handle_and_gets_the_new_key(clock: Clock) -> None:
    message = "If @alice is registered with a@example.com, we sent a 6-digit code there. It expires in 10 minutes."
    server = Server([sent("recover", "pnd_1", message), recovered("alice")])
    person = Person([value("123456")])
    result = run_with(server, person, kind="recover", input={"email": "a@example.com", "handle": "alice"})
    assert result == account.RunResult(outcome="done", api_key="key_test_2", handle="alice")
    assert list(zip(server.paths(), server.bodies())) == [
        ("/v1/agents/recover", {"email": "a@example.com", "handle": "alice"}),
        ("/v1/agents/recover/verify", {"pending_id": "pnd_1", "code": "123456"}),
    ]
    assert person.shown[1] == "Email verified. Logged in as @alice. Your old API key no longer works."


def test_recover_on_an_email_with_several_agents_offers_the_handles(clock: Clock) -> None:
    server = Server(
        [
            sent("recover", "pnd_1", "If an agent is registered with a@example.com, we sent a 6-digit code there."),
            error(
                409,
                "HANDLE_REQUIRED",
                "More than one agent uses this email address. Choose the handle to recover.",
                {"handles": ["alice", "alice-2"], "flow": "recover", "next_step": "recover", "param": "handle"},
            ),
            sent("recover", "pnd_2"),
            recovered("alice-2"),
        ]
    )
    person = Person([value("123456"), value("alice-2"), value("654321")], clock=clock, think_s=61)
    result = run_with(server, person, kind="recover", input={"email": "a@example.com"})
    assert result == account.RunResult(outcome="done", api_key="key_test_2", handle="alice-2")
    assert server.bodies()[0] == {"email": "a@example.com"}
    assert (person.asked[1].field, person.asked[1].handles) == ("handle", ("alice", "alice-2"))
    assert server.bodies()[2] == {"email": "a@example.com", "handle": "alice-2"}


def test_recover_asks_for_the_email_only(clock: Clock) -> None:
    server = Server([sent("recover"), recovered()])
    person = Person([value("a@example.com"), value("123456")])
    run_with(server, person, kind="recover", input={})
    assert [q.field for q in person.asked] == ["email", "code"]


def test_a_network_error_raises(clock: Clock) -> None:
    server = Server([httpx.ConnectError("no route")])
    with pytest.raises(ConnectionError):
        run_with(server, Person([]))


def test_an_answer_the_question_does_not_allow_is_a_programmer_error(clock: Clock) -> None:
    # A bare string, not an AccountAnswer.
    with pytest.raises(TypeError):
        run_with(Server([sent()]), Person(["123456"]))  # type: ignore[list-item]
    # A value answer without a value.
    with pytest.raises(TypeError):
        run_with(Server([sent()]), Person([AccountAnswer("value")]))
    # A new code is no answer to the email question; nothing is sent.
    server = Server([])
    with pytest.raises(TypeError):
        run_with(server, Person([NEW_CODE]), input={})
    assert server.seen == []


def test_an_unknown_kind_is_refused() -> None:
    with pytest.raises(ValueError):
        account.run(kind="rotate_key", ask=lambda q: CANCEL, show=print, client_identity=IDENTITY)  # type: ignore[arg-type]


# ─────────────── run(): a server before the flow contract ───────────────


def legacy_sent(pending_id: str = "pnd_1") -> httpx.Response:
    return respond(200, {"pending_id": pending_id, "message": "Verification code sent to email"})


def legacy_registered() -> httpx.Response:
    return respond(201, {"agent": {"handle": "alice", "email": "a@example.com"}, "api_key": "key_test_1"})


def test_legacy_code_sent_wrong_code_right_code(clock: Clock) -> None:
    server = Server(
        [
            legacy_sent(),
            respond(400, {"code": "INVALID_CODE", "message": "Invalid or expired verification code"}),
            legacy_registered(),
        ]
    )
    person = Person([value("111111"), value("123456")])
    assert run_with(server, person) == account.RunResult(outcome="done", api_key="key_test_1", handle="alice")
    assert person.shown == ["Verification code sent to email", "Invalid or expired verification code"]
    assert server.bodies()[1:] == [
        {"pending_id": "pnd_1", "code": "111111"},
        {"pending_id": "pnd_1", "code": "123456"},
    ]


def test_legacy_handle_taken_re_asks_the_handle_only(clock: Clock) -> None:
    server = Server(
        [
            respond(409, {"code": "HANDLE_TAKEN", "message": "Handle @alice is already taken"}),
            legacy_sent(),
            legacy_registered(),
        ]
    )
    person = Person([value("alice-2"), value("123456")])
    assert run_with(server, person).outcome == "done"
    assert [q.field for q in person.asked] == ["handle", "code"]


@pytest.mark.parametrize(
    ("status", "code", "message"),
    [
        (400, "EXPIRED", "Registration expired or invalid. Please start again."),
        (429, "OTP_ATTEMPTS_EXHAUSTED", "Too many incorrect codes (max 5). Please request a new one."),
    ],
)
def test_legacy_dead_code_asks_before_a_new_code(clock: Clock, status: int, code: str, message: str) -> None:
    server = Server([legacy_sent(), respond(status, {"code": code, "message": message})])
    person = Person([value("123456"), CANCEL])
    assert run_with(server, person) == account.RunResult(outcome="cancelled")
    assert [q.field for q in person.asked] == ["code", "new_code"]


def test_legacy_cooldown_with_retry_after_waits_then_sends(clock: Clock) -> None:
    server = Server(
        [
            respond(429, {"code": "OTP_COOLDOWN", "message": "Please wait 20s before requesting another code."}, {"Retry-After": "20"}),
            legacy_sent(),
            legacy_registered(),
        ]
    )
    person = Person([value("123456")])
    assert run_with(server, person).outcome == "done"
    assert person.slept == [20.0]


def test_legacy_email_lock_long_stops(clock: Clock) -> None:
    server = Server(
        [
            legacy_sent(),
            respond(
                429,
                {"code": "OTP_EMAIL_LOCKED", "message": "Too many wrong codes for this email. Try again in 3 hours.", "retry_after_s": 10800},
                {"Retry-After": "10800"},
            ),
        ]
    )
    result = run_with(server, Person([value("123456")]))
    assert (result.outcome, result.code) == ("stopped", "OTP_EMAIL_LOCKED")


def test_legacy_429_with_no_wait_named_stops_on_a_start_or_a_code(clock: Clock) -> None:
    server = Server([respond(429, {"code": "RATE_LIMITED", "message": "Too many requests"})])
    result = run_with(server, Person([]))
    assert (result.outcome, result.code) == ("stopped", "RATE_LIMITED")
    on_code = Server([legacy_sent(), respond(429, {"code": "RATE_LIMITED", "message": "Too many requests"})])
    result = run_with(on_code, Person([value("123456")]))
    assert (result.outcome, result.code) == ("stopped", "RATE_LIMITED")


def test_legacy_email_exhausted_re_asks_the_email(clock: Clock) -> None:
    server = Server(
        [
            respond(409, {"code": "EMAIL_EXHAUSTED", "message": "Lifetime limit.", "details": {"limit": 30}}),
            legacy_sent(),
            legacy_registered(),
        ]
    )
    person = Person([value("b@example.com"), value("123456")])
    assert run_with(server, person).outcome == "done"
    assert [q.field for q in person.asked] == ["email", "code"]


def test_legacy_handle_required_on_recover_offers_the_handles(clock: Clock) -> None:
    server = Server(
        [
            respond(200, {"pending_id": "pnd_1", "message": "If an agent is registered with that email, a code was sent."}),
            respond(409, {"code": "HANDLE_REQUIRED", "message": "Several agents.", "details": {"handles": ["alice", "bob"]}}),
            respond(200, {"pending_id": "pnd_2", "message": "If an agent is registered with that email, a code was sent."}),
            respond(200, {"handle": "bob", "api_key": "key_test_2"}),
        ]
    )
    person = Person([value("123456"), value("bob"), value("654321")])
    result = run_with(server, person, kind="recover", input={"email": "a@example.com"})
    assert result == account.RunResult(outcome="done", api_key="key_test_2", handle="bob")
    assert (person.asked[1].field, person.asked[1].handles) == ("handle", ("alice", "bob"))


def test_legacy_5xx_with_no_json_body_stops(clock: Clock) -> None:
    server = Server([httpx.Response(502, content=b"bad gateway")])
    result = run_with(server, Person([]))
    assert (result.outcome, result.code) == ("stopped", "INTERNAL_ERROR")


# ─────────────── run_async ───────────────


async def test_run_async_takes_coroutine_callbacks(clock: Clock) -> None:
    server = Server([cooldown(5), sent(), wrong_code(1), registered()])
    asked: list[str] = []
    shown: list[str] = []
    slept: list[float] = []
    answers = [value("111111"), value("123456")]

    async def ask(question: AccountQuestion) -> AccountAnswer:
        asked.append(question.field)
        await asyncio.sleep(0)
        return answers.pop(0)

    async def sleep(seconds: float) -> None:
        slept.append(seconds)

    result = await account.run_async(
        kind="register",
        input={"email": "a@example.com", "handle": "alice"},
        ask=ask,
        show=shown.append,
        sleep=sleep,
        client_identity=IDENTITY,
        api_base=API,
        http_client=server.async_client(),
    )
    assert result == account.RunResult(outcome="done", api_key="key_test_1", handle="alice")
    assert asked == ["code", "code"]
    assert slept == [5.0]
    assert shown[0] == "You can request a new code in 5 seconds."
    assert server.paths() == ["/v1/register", "/v1/register", "/v1/register/verify", "/v1/register/verify"]


async def test_run_async_cancel_and_plain_callbacks(clock: Clock) -> None:
    server = Server([sent()])
    result = await account.run_async(
        kind="register",
        input={"email": "a@example.com", "handle": "alice"},
        ask=lambda q: CANCEL,
        show=lambda m: None,
        client_identity=IDENTITY,
        api_base=API,
        http_client=server.async_client(),
    )
    assert result == account.RunResult(outcome="cancelled")


# ─────────────── the steps alone (one per process run) ───────────────


def _options(server: Server) -> dict[str, Any]:
    return {"client_identity": IDENTITY, "api_base": API, "http_client": server.client()}


def test_start_returns_a_plain_json_flow_a_later_process_submits_the_code() -> None:
    first = Server([sent()])
    started = account.start("register", {"email": "a@example.com", "handle": "alice"}, **_options(first))
    assert (started.action, started.http_status, started.legacy, started.message, started.code) == (
        "ask_code",
        200,
        False,
        SENT_TEXT,
        None,
    )
    saved = json.loads(json.dumps(started.flow))
    assert saved == started.flow

    second = Server([registered("alice")])
    done = account.submit_code(saved, "123456", **_options(second))
    assert (done.action, done.api_key, done.handle) == ("done", "key_test_1", "alice")
    assert second.bodies() == [{"pending_id": "pnd_1", "code": "123456"}]
    assert "key_test_1" not in repr(done)


def test_api_outcomes_return_with_the_code_never_raise() -> None:
    server = Server(
        [
            error(
                409,
                "HANDLE_TAKEN",
                "The handle @alice is taken. Try another.",
                {"flow": "register", "next_step": "register", "param": "handle"},
            )
        ]
    )
    result = account.start("register", {"email": "a@example.com", "handle": "alice"}, **_options(server))
    assert (result.action, result.code, result.http_status) == ("ask_param", "HANDLE_TAKEN", 409)
    assert result.flow.get("param") == "handle"


def test_a_legacy_answer_is_read_into_the_same_flow_shape_marked_legacy() -> None:
    server = Server([legacy_sent()])
    result = account.start("register", {"email": "a@example.com", "handle": "alice"}, **_options(server))
    assert (result.legacy, result.action) == (True, "ask_code")
    assert result.flow == {
        "version": 1,
        "flow": "register",
        "next_step": "verify",
        "status": "pending",
        "pending_id": "pnd_1",
        "code_length": 6,
    }


def test_submit_code_refuses_a_flow_that_is_not_waiting_for_a_code() -> None:
    server = Server([])
    flows: list[Any] = [
        {"version": 1, "flow": "register", "next_step": "register"},
        {"version": 1, "flow": "rotate_key", "next_step": "rotate_key_verify", "pending_id": "pnd_1"},
        {"version": 1, "flow": "register", "next_step": "verify"},
    ]
    for flow in flows:
        with pytest.raises(ValueError):
            account.submit_code(flow, "123456", **_options(server))
    with pytest.raises(TypeError):
        account.submit_code("not a flow", "123456", **_options(server))  # type: ignore[arg-type]
    assert server.seen == []


def test_start_refuses_an_unknown_kind() -> None:
    with pytest.raises(ValueError):
        account.start("rotate_key", {"email": "a@example.com"}, client_identity=IDENTITY)  # type: ignore[arg-type]


def test_start_sends_only_the_fields_set() -> None:
    server = Server([sent("recover")])
    account.start("recover", {"email": "a@example.com"}, **_options(server))
    assert server.bodies() == [{"email": "a@example.com"}]


def test_done_without_a_key_in_the_body_is_not_done() -> None:
    server = Server(
        [respond(201, {"message": "Email verified.", "version": 1, "flow": "register", "next_step": "done"})]
    )
    result = account.submit_code(
        {"version": 1, "flow": "register", "next_step": "verify", "pending_id": "pnd_1"},
        "123456",
        **_options(server),
    )
    assert (result.action, result.api_key) == ("stop", None)


def test_a_network_error_raises_from_a_step() -> None:
    server = Server([httpx.ConnectTimeout("slow")])
    with pytest.raises(ConnectionError):
        account.start("register", {"email": "a@example.com", "handle": "alice"}, **_options(server))


def test_never_retries_one_request_per_step() -> None:
    server = Server([error(503, "INTERNAL_ERROR", "Unavailable", {"flow": "register", "next_step": "register"})])
    result = account.start("register", {"email": "a@example.com", "handle": "alice"}, **_options(server))
    assert (result.action, len(server.seen)) == ("stop", 1)


async def test_the_async_steps() -> None:
    server = Server([sent(), registered()])
    options: dict[str, Any] = {"client_identity": IDENTITY, "api_base": API, "http_client": server.async_client()}
    started = await account.start_async("register", {"email": "a@example.com", "handle": "alice"}, **options)
    done = await account.submit_code_async(started.flow, "123456", **options)
    assert (done.action, done.api_key, done.handle) == ("done", "key_test_1", "alice")


# ─────────────── check_key ───────────────


def test_check_key_ok_with_the_handle_the_key_belongs_to() -> None:
    server = Server([respond(200, {"handle": "alice", "email": "a@example.com"})])
    result = account.check_key("key_test_1", **_options(server))
    assert result == account.CheckKeyResult(ok=True, handle="alice")
    assert server.seen[0]["method"] == "GET"
    assert server.seen[0]["path"] == "/v1/agents/me"
    assert server.seen[0]["headers"].get("authorization") == "Bearer key_test_1"
    assert server.seen[0]["headers"].get("x-agentchat-client") == "hermes"


def test_check_key_rejected_is_the_servers_code_and_message_one_attempt() -> None:
    server = Server([respond(401, {"code": "UNAUTHORIZED", "message": "Invalid API key"})])
    result = account.check_key("bad", **_options(server))
    assert result == account.CheckKeyResult(ok=False, code="UNAUTHORIZED", message="Invalid API key")
    assert len(server.seen) == 1


def test_check_key_a_5xx_is_not_retried() -> None:
    server = Server([respond(503, {"code": "INTERNAL_ERROR", "message": "Unavailable"})])
    result = account.check_key("key", **_options(server))
    assert (result.ok, result.code, len(server.seen)) == (False, "INTERNAL_ERROR", 1)


def test_check_key_without_a_usable_answer_raises() -> None:
    with pytest.raises(ConnectionError):
        account.check_key("key", **_options(Server([httpx.ConnectError("no route")])))
    with pytest.raises(ConnectionError):
        account.check_key("key", **_options(Server([respond(200, {"email": "a@example.com"})])))


async def test_check_key_async() -> None:
    server = Server([respond(200, {"handle": "alice"})])
    result = await account.check_key_async(
        "key", client_identity=IDENTITY, api_base=API, http_client=server.async_client()
    )
    assert result == account.CheckKeyResult(ok=True, handle="alice")


# ─────────────── flow_of / action_of ───────────────


def test_flow_of_reads_an_sdk_errors_details() -> None:
    err = create_agentchat_error(
        {
            "code": "OTP_COOLDOWN",
            "message": "You can request a new code in 42 seconds.",
            "details": {"version": 1, "flow": "register", "next_step": "register", "retry_after_s": 42},
        },
        429,
        httpx.Headers({"Retry-After": "42"}),
    )
    assert account.flow_of(err) == {"version": 1, "flow": "register", "next_step": "register", "retry_after_s": 42}
    assert account.action_of(account.flow_of(err), 429) == "wait_then_retry"


def test_flow_of_none_without_a_next_step_unknown_fields_dropped() -> None:
    assert account.flow_of({"pending_id": "pnd_1", "message": "Verification code sent to email"}) is None
    assert account.flow_of({"code": "INVALID_CODE", "message": "x"}) is None
    assert account.flow_of(None) is None
    assert account.flow_of("verify") is None
    flow = account.flow_of(
        {
            "version": 1,
            "flow": "register",
            "next_step": "verify",
            "pending_id": "pnd_1",
            "attempts": True,
            "max_attempts": -1,
            "expires_in_s": 600.0,
            "resend_after_s": 1.5,
            "future_field": "x",
        }
    )
    assert flow == {"version": 1, "flow": "register", "next_step": "verify", "pending_id": "pnd_1", "expires_in_s": 600}
    assert account.action_of(None, 200) == "legacy"


def test_a_5xx_stops_even_on_a_verify_step() -> None:
    flow: Any = {"version": 1, "flow": "register", "next_step": "verify", "pending_id": "pnd_1"}
    assert account.action_of(flow, 503) == "stop"
    assert account.action_of(flow, 400) == "ask_code"


# ─────────────── the client's static sign-up calls: flow fields (additive) ───────────────


def _with_server(server: Server) -> dict[str, Any]:
    return {"base_url": API, "client_identity": IDENTITY, "http_client": server.client()}


def test_register_verify_recover_and_recover_verify_carry_the_flow_fields() -> None:
    server = Server([sent(), registered("alice"), sent("recover"), recovered("alice")])
    options = _with_server(server)

    started = AgentChatClient.register(email="a@example.com", handle="alice", **options)
    assert (started["pending_id"], started["next_step"], started["message"]) == ("pnd_1", "verify", SENT_TEXT)

    verified = AgentChatClient.verify("pnd_1", "123456", **options)
    agent, api_key, client = verified
    assert (agent["handle"], api_key) == ("alice", "key_test_1")
    assert isinstance(client, AgentChatClient)
    assert verified == (agent, api_key, client)
    assert len(verified) == 3
    assert (verified.message, verified.next_step, verified.status, verified.version, verified.flow) == (
        "Email verified. Logged in as @alice.",
        "done",
        "approved",
        1,
        "register",
    )
    assert verified.attempts is None
    assert account.flow_of(verified) == {"version": 1, "flow": "register", "next_step": "done", "status": "approved"}

    recovering = AgentChatClient.recover("a@example.com", handle="alice", **options)
    assert recovering["next_step"] == "recover_verify"
    handle, key, _client = recovered_result = AgentChatClient.recover_verify("pnd_1", "123456", **options)
    assert (handle, key) == ("alice", "key_test_2")
    assert (recovered_result.next_step, recovered_result.message) == (
        "done",
        "Email verified. Logged in as @alice. Your old API key no longer works.",
    )


def test_an_old_servers_verify_answer_has_no_flow_fields() -> None:
    server = Server([legacy_registered()])
    verified = AgentChatClient.verify("pnd_1", "123456", **_with_server(server))
    assert (verified.message, verified.next_step, verified.status) == (None, None, None)
    assert account.flow_of(verified) is None


async def test_the_async_verify_results_carry_the_flow_fields() -> None:
    server = Server([registered("alice"), recovered("alice")])
    options = {"base_url": API, "client_identity": IDENTITY, "http_client": server.async_client()}
    verified = await AsyncAgentChatClient.verify("pnd_1", "123456", **options)
    assert isinstance(verified[2], AsyncAgentChatClient)
    assert verified.next_step == "done"
    recovered_result = await AsyncAgentChatClient.recover_verify("pnd_1", "123456", **options)
    assert recovered_result.message == "Email verified. Logged in as @alice. Your old API key no longer works."


def test_a_sign_up_error_keeps_details_and_retry_after_s() -> None:
    locked = create_agentchat_error(
        {
            "code": "OTP_EMAIL_LOCKED",
            "message": "You entered the wrong code too many times. Wait 14 minutes, then request a new code.",
            "retry_after_s": 840,
            "details": {"version": 1, "flow": "register", "next_step": "register", "retry_after_s": 840},
        },
        429,
    )
    assert isinstance(locked, RateLimitedError)
    assert locked.retry_after_s == 840
    assert locked.retry_after_ms == 840_000  # no Retry-After header: from the body
    assert locked.details is not None and locked.details["next_step"] == "register"

    in_details = create_agentchat_error(
        {"code": "RATE_LIMITED", "message": "Too many attempts.", "details": {"retry_after_s": 35}},
        429,
        httpx.Headers({"Retry-After": "30"}),
    )
    assert isinstance(in_details, RateLimitedError)
    assert (in_details.retry_after_s, in_details.retry_after_ms) == (35, 30_000)  # the header wins for ms

    plain = create_agentchat_error({"code": "INVALID_CODE", "message": "x"}, 400)
    assert plain.retry_after_s is None


def test_default_labels_are_the_only_words() -> None:
    assert set(account.DEFAULT_LABELS) == {"email", "handle", "code"}
