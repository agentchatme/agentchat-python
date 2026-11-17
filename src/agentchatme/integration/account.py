"""Sign-up for a setup wizard: register a new agent, or recover a lost key.

The server's answer to every sign-up call says the next step (contract v1,
"Sign-up flows"): which call to make (``next_step``), the facts
(``pending_id``, ``attempts``, ``retry_after_s`` ...) and the sentence to
show (``message``). This module follows it, so an integration's wizard only
asks the person, shows the sentence and saves the key, and carries no
rulebook of its own.

- :func:`flow_of` reads the flow object out of an answer (pure).
- :func:`action_of` says what a client does with it (pure; the conformance
  vocabulary of ``signup-flow.cases.json``).
- :func:`start` and :func:`submit_code` make one call each and return a
  :class:`StepResult`; they never raise for an API answer. A command line
  can run one step per process: save ``result.flow`` (plain JSON data) and
  pass it to :func:`submit_code` in the next run.
- :func:`run` is the loop a terminal wizard wants: it shows every message,
  asks only for the input the server names, waits up to 60 seconds when
  the server says to wait, and returns the key and the handle.
- :func:`check_key` asks the server whether a pasted key works.

``*_async`` twins run on an event loop. Rules for every integration: never
save a key the server rejected, and save the handle the server returned,
not the one the person typed.

A server older than the contract sends no ``next_step``. Then the answer is
read by its ``code`` the way clients read it before (``legacy``), and the
step functions return the flow that reading implies, so the rest is the same.
"""

from __future__ import annotations

import asyncio
import inspect
import math
import time
from collections.abc import Awaitable, Generator, Mapping
from dataclasses import dataclass, field
from typing import Any, Callable, Literal, TypedDict, Union, cast

import httpx

from .._client import DEFAULT_BASE_URL, _static_options
from .._client_identity import AgentChatClientIdentity
from .._http import AsyncHttpTransport, HttpResponse, HttpTransport, HttpTransportOptions
from .._signup_flow import flow_of, whole_seconds
from ..errors import AgentChatError, ConnectionError, RateLimitedError
from ..types.signup import SIGNUP_FLOW_VERSION, SignupFlow

AccountKind = Literal["register", "recover"]
"""What a wizard does: register a new agent, or recover a lost key."""

SignupAction = Literal[
    "done",
    "ask_code",
    "ask_param",
    "offer_new_code",
    "wait_then_retry",
    "stop",
    "legacy",
]
"""What a client does with an answer (contract v1, "Sign-up flows",
client actions):

- ``done``: the key is in the body.
- ``ask_code``: ask for the code, then submit it.
- ``ask_param``: ask again for the one input ``param`` names, then call the
  start step again.
- ``offer_new_code``: the code is dead; ask the person before sending a new one.
- ``wait_then_retry``: a 429 that asks for at most 60 seconds; wait, then
  repeat ``next_step``.
- ``stop``: show the message and end.
- ``legacy``: no ``next_step`` (a server older than the contract); read the
  answer by its ``code``.
"""

WAIT_MAX_S = 60
"""The longest wait :func:`run` sits through; a longer one stops the run."""

DEFAULT_LABELS: Mapping[str, str] = {
    "email": "Email address",
    "handle": "Handle",
    "code": "Enter the 6-digit code",
}
"""The field labels :func:`run` asks with: the only words in this module."""

_START_STEPS = frozenset({"register", "recover", "rotate_key"})
_VERIFY_STEPS = frozenset({"verify", "recover_verify", "rotate_key_verify"})
_KIND_OF_STEP: Mapping[str, str] = {
    "register": "register",
    "verify": "register",
    "recover": "recover",
    "recover_verify": "recover",
    "rotate_key": "rotate_key",
    "rotate_key_verify": "rotate_key",
}
_VERIFY_STEP_OF: Mapping[str, str] = {
    "register": "verify",
    "recover": "recover_verify",
    "rotate_key": "rotate_key_verify",
}
_START_PATH: Mapping[str, str] = {"register": "/v1/register", "recover": "/v1/agents/recover"}
_VERIFY_PATH: Mapping[str, str] = {
    "verify": "/v1/register/verify",
    "recover_verify": "/v1/agents/recover/verify",
}
_DEAD_CODE = frozenset({"expired", "max_attempts_reached"})


class AccountInput(TypedDict, total=False):
    """What the person typed. ``register`` needs ``email`` and ``handle``;
    ``recover`` needs ``email`` and should have ``handle`` (an email may back
    several agents)."""

    email: str
    handle: str
    display_name: str
    """``register`` only."""
    description: str
    """``register`` only."""


class NewCode:
    """The answer "send me a new code" (:data:`NEW_CODE`)."""

    _instance: NewCode | None = None

    def __new__(cls) -> NewCode:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __repr__(self) -> str:
        return "NEW_CODE"


NEW_CODE = NewCode()
"""Return it from ``ask`` to request a new code (on the ``code`` and
``new_code`` questions)."""

Answer = Union[str, NewCode, None]
"""An answer to a :class:`Question`: the value typed, :data:`NEW_CODE`, or
``None`` to cancel."""


@dataclass(frozen=True)
class Question:
    """What :func:`run` asks the person.

    ``param`` is ``"email"``, ``"handle"`` or ``"code"`` (or an input a later
    server names), or ``"new_code"``: the code is dead and the person decides
    whether to get a new one (answer :data:`NEW_CODE` to send it, ``None`` to
    stop). The server's message, already shown, says why.

    On ``code`` the person may enter the code, answer :data:`NEW_CODE`, or
    cancel. Every other answer is the value typed, or ``None`` to cancel.
    """

    param: str
    label: str | None
    """The field label (:data:`DEFAULT_LABELS`); ``None`` for ``new_code``
    and for an input this release has no label for."""
    flow: SignupFlow
    """The flow being followed: every fact the server sent (``code_length``,
    ``expires_in_s``, ``attempts`` ...)."""
    handles: tuple[str, ...] = ()
    """On ``handle`` after ``HANDLE_REQUIRED``: the agents on that email."""


@dataclass(frozen=True)
class StepResult:
    """One sign-up call's answer, read."""

    flow: SignupFlow
    """The flow to follow next: the server's, or, from a server older than
    the contract, the one its answer implies. Plain JSON data: save it to
    continue in a later run."""
    message: str
    """The server's sentence, to show as is (``""`` when it sent none)."""
    action: SignupAction
    """:func:`action_of` for ``flow``: never ``legacy`` here."""
    http_status: int
    code: str | None = None
    """The error ``code`` of a non-2xx answer."""
    api_key: str | None = field(default=None, repr=False)
    """On ``done``: the new API key. Shown once; save it."""
    handle: str | None = None
    """On ``done``: the handle the server returned. Save this one."""
    details: Mapping[str, Any] | None = None
    """An error answer's ``details`` (``handles``, ``limit`` ...)."""


RunOutcome = Literal["done", "cancelled", "stopped"]


@dataclass(frozen=True)
class RunResult:
    """How :func:`run` ended. Every server message was already shown."""

    outcome: RunOutcome
    api_key: str | None = field(default=None, repr=False)
    """``done``: the new API key. Save it with ``handle``."""
    handle: str | None = None
    """``done``: the handle the server returned."""
    code: str | None = None
    """``stopped``: the error code, when the answer that stopped it had one."""
    message: str | None = None
    """``stopped``: the server's message that stopped it."""


@dataclass(frozen=True)
class CheckKeyResult:
    """Whether a key works (``GET /v1/agents/me``)."""

    ok: bool
    handle: str | None = None
    """``ok``: the key's handle."""
    code: str | None = None
    """Not ``ok``: the server's error code."""
    message: str | None = None
    """Not ``ok``: the server's message."""


# ─────────────── reading an answer (pure) ───────────────


def action_of(flow: SignupFlow | None, http_status: int | None = None) -> SignupAction:
    """What a client does with ``flow`` (contract v1, client actions). Pure.

    Read from ``next_step``, ``status``, ``param``, ``retry_after_s`` and the
    HTTP status alone, never from the message. ``http_status`` may be
    omitted for a saved flow; ``retry_after_s`` then stands for the 429.
    """
    if flow is None:
        return "legacy"
    step = flow.get("next_step")
    if step == "done":
        return "done"
    if http_status is not None and http_status >= 500:
        return "stop"
    retry = whole_seconds(flow.get("retry_after_s"))
    if step in _VERIFY_STEPS and retry is None:
        return "ask_code"
    if step in _START_STEPS:
        if flow.get("param") is not None:
            return "ask_param"
        if flow.get("status") in _DEAD_CODE:
            return "offer_new_code"
    if retry is not None and (http_status is None or http_status == 429):
        return "wait_then_retry" if retry <= WAIT_MAX_S else "stop"
    return "stop"


# ─────────────── an old server: read the answer by its code ───────────────

_HANDLE_CODES = frozenset({"HANDLE_TAKEN", "INVALID_HANDLE", "RESERVED_HANDLE"})
_EMAIL_CODES = frozenset({"EMAIL_LIMIT_REACHED", "EMAIL_EXHAUSTED", "EMAIL_TAKEN"})
_SEND_LIMIT_CODES = frozenset({"OTP_COOLDOWN", "OTP_HOURLY_CAP"})


def _retry_seconds(err: AgentChatError) -> int | None:
    if err.retry_after_s is not None:
        return err.retry_after_s
    if isinstance(err, RateLimitedError) and err.retry_after_ms is not None:
        return max(0, math.ceil(err.retry_after_ms / 1000))
    return None


def _field_errors(details: Mapping[str, Any] | None) -> set[str]:
    """The fields a zod ``flatten()`` names in ``details.fieldErrors``."""
    errors = details.get("fieldErrors") if details is not None else None
    if not isinstance(errors, Mapping):
        return set()
    return {str(name) for name in cast("Mapping[str, Any]", errors)}


def _legacy_flow(
    kind: str,
    called: str,
    *,
    data: Any = None,
    err: AgentChatError | None = None,
    request_flow: SignupFlow | None = None,
) -> SignupFlow:
    """The flow an answer without ``next_step`` implies: each code read the
    way the contract now spells it out."""
    start = kind
    verify = _VERIFY_STEP_OF[kind]
    flow: dict[str, Any] = {"version": SIGNUP_FLOW_VERSION, "flow": kind}
    pending_id = request_flow.get("pending_id") if request_flow is not None else None

    if err is None:
        body = cast("Mapping[str, Any]", data) if isinstance(data, Mapping) else {}
        if called == start and isinstance(body.get("pending_id"), str):
            flow.update(next_step=verify, status="pending", pending_id=body["pending_id"])
        elif called == verify and isinstance(body.get("api_key"), str):
            flow.update(next_step="done", status="approved")
        else:
            flow["next_step"] = start  # an answer this client cannot read: stop
        return cast("SignupFlow", flow)

    code = err.code
    if code == "INVALID_CODE" and called == verify and pending_id:
        flow.update(next_step=verify, status="pending", pending_id=pending_id, param="code")
    elif code == "EXPIRED":
        flow.update(next_step=start, status="expired")
    elif code == "OTP_ATTEMPTS_EXHAUSTED":
        flow.update(next_step=start, status="max_attempts_reached")
    elif code in _SEND_LIMIT_CODES or code in ("OTP_EMAIL_LOCKED", "RATE_LIMITED"):
        flow["next_step"] = start if code in _SEND_LIMIT_CODES else called
        if flow["next_step"] == verify and pending_id:
            flow["pending_id"] = pending_id
        seconds = _retry_seconds(err)
        if seconds is not None:
            flow["retry_after_s"] = seconds
    elif code in _HANDLE_CODES and kind == "register":
        flow.update(next_step="register", param="handle")
    elif code in _EMAIL_CODES and kind == "register":
        flow.update(next_step="register", param="email")
    elif code == "HANDLE_REQUIRED" and kind == "recover":
        flow.update(next_step="recover", param="handle")
    elif code == "VALIDATION_ERROR":
        fields = _field_errors(err.details)
        if called == verify and "code" in fields and pending_id:
            flow.update(next_step=verify, pending_id=pending_id, param="code")
        elif called == start and "handle" in fields:
            flow.update(next_step=start, param="handle")
        elif called == start and "email" in fields:
            flow.update(next_step=start, param="email")
        else:
            flow["next_step"] = start
    else:
        flow["next_step"] = start  # OTP_FAILED, INTERNAL_ERROR, ...: stop
    return cast("SignupFlow", flow)


# ─────────────── one call: the request and its answer ───────────────


def _start_request(kind: str, input: AccountInput) -> tuple[str, dict[str, Any]]:
    if kind not in _START_PATH:
        raise ValueError(
            f"start() takes kind 'register' or 'recover', not {kind!r} "
            "(a key rotation is AgentChatClient.rotate_key)"
        )
    email = input.get("email")
    if not isinstance(email, str):
        raise ValueError(f"{kind} needs input['email']")
    body: dict[str, Any] = {"email": email}
    handle = input.get("handle")
    if kind == "register":
        if not isinstance(handle, str):
            raise ValueError("register needs input['handle']")
        body["handle"] = handle
        for name in ("display_name", "description"):
            value = input.get(name)
            if value is not None:
                body[name] = value
    elif handle is not None:
        body["handle"] = handle
    return _START_PATH[kind], body


def _verify_request(flow: SignupFlow, code: str) -> tuple[str, dict[str, Any]]:
    step = flow.get("next_step")
    path = _VERIFY_PATH.get(step) if isinstance(step, str) else None
    if path is None:
        raise ValueError(
            f"submit_code() needs a flow whose next_step is 'verify' or 'recover_verify', not {step!r}"
        )
    pending_id = flow.get("pending_id")
    if not isinstance(pending_id, str) or not pending_id:
        raise ValueError("submit_code() needs a flow with a pending_id")
    if not isinstance(code, str):
        raise TypeError("code must be a str")
    return path, {"pending_id": pending_id, "code": code.strip()}


def _with_pending_id(flow: SignupFlow, request_flow: SignupFlow | None) -> SignupFlow:
    """A verify step answered without ``pending_id`` (a malformed code, a 429)
    keeps the code being verified: the same pending id."""
    if request_flow is None or flow.get("pending_id") or flow.get("next_step") not in _VERIFY_STEPS:
        return flow
    if flow.get("next_step") != request_flow.get("next_step"):
        return flow
    pending_id = request_flow.get("pending_id")
    if not pending_id:
        return flow
    carried = cast("dict[str, Any]", dict(flow))
    carried["pending_id"] = pending_id
    return cast("SignupFlow", carried)


def _handle_of(body: Mapping[str, Any]) -> str | None:
    handle = body.get("handle")
    if isinstance(handle, str) and handle:
        return handle
    agent = body.get("agent")
    if isinstance(agent, Mapping):
        nested = cast("Mapping[str, Any]", agent).get("handle")
        if isinstance(nested, str) and nested:
            return nested
    return None


def _read_success(
    kind: str, called: str, res: HttpResponse, request_flow: SignupFlow | None
) -> StepResult:
    data: Any = res.data
    body = cast("Mapping[str, Any]", data) if isinstance(data, Mapping) else {}
    flow = flow_of(body) if body else None
    if flow is None:
        flow = _legacy_flow(kind, called, data=body, request_flow=request_flow)
    flow = _with_pending_id(flow, request_flow)
    message = body.get("message")
    action = action_of(flow, res.status)
    api_key: str | None = None
    handle: str | None = None
    if action == "done":
        key = body.get("api_key")
        handle = _handle_of(body)
        if not isinstance(key, str) or not key or handle is None:
            raise ConnectionError(
                "AgentChat SDK: the server answered done without an api_key and a handle"
            )
        api_key = key
    return StepResult(
        flow=flow,
        message=message if isinstance(message, str) else "",
        action=action,
        http_status=res.status,
        api_key=api_key,
        handle=handle,
    )


def _read_error(
    kind: str, called: str, err: AgentChatError, request_flow: SignupFlow | None
) -> StepResult:
    flow = flow_of(err)
    if flow is None:
        flow = _legacy_flow(kind, called, err=err, request_flow=request_flow)
    flow = _with_pending_id(flow, request_flow)
    return StepResult(
        flow=flow,
        message=str(err),
        action=action_of(flow, err.status),
        http_status=err.status,
        code=err.code,
        details=err.details,
    )


def _kind_of(step: Any) -> str:
    return _KIND_OF_STEP.get(step, "") if isinstance(step, str) else ""


# ─────────────── the step functions ───────────────


def start(
    kind: AccountKind,
    input: AccountInput,
    *,
    client_identity: AgentChatClientIdentity | None,
    api_base: str = DEFAULT_BASE_URL,
    timeout_ms: int | None = None,
    http_client: httpx.Client | None = None,
) -> StepResult:
    """Start a flow: ``POST /v1/register`` or ``POST /v1/agents/recover``
    (the server emails a code). One attempt, never retried.

    Returns the answer read, for every answer the server gives; raises only
    :class:`~agentchatme.ConnectionError` (no answer, or one that cannot be
    read) and ``ValueError`` for an ``input`` the kind cannot use. A given
    ``http_client`` is never closed here.
    """
    path, body = _start_request(kind, input)
    transport = HttpTransport(
        _static_options(api_base, client_identity, timeout_ms), client=http_client
    )
    try:
        try:
            res = transport.request("POST", path, body=body, retry="never")
        except AgentChatError as err:
            return _read_error(kind, kind, err, None)
        return _read_success(kind, kind, res, None)
    finally:
        transport.close()


def submit_code(
    flow: SignupFlow,
    code: str,
    *,
    client_identity: AgentChatClientIdentity | None,
    api_base: str = DEFAULT_BASE_URL,
    timeout_ms: int | None = None,
    http_client: httpx.Client | None = None,
) -> StepResult:
    """Submit the code for ``flow`` (its ``next_step`` is ``verify`` or
    ``recover_verify``): ``POST /v1/register/verify`` or
    ``POST /v1/agents/recover/verify``. One attempt, never retried.

    On ``done`` the result holds the API key and the handle the server
    returned. Raises like :func:`start`; ``ValueError`` for a flow that is
    not waiting for a code.
    """
    path, body = _verify_request(flow, code)
    step = str(flow.get("next_step"))
    kind = _kind_of(step)
    transport = HttpTransport(
        _static_options(api_base, client_identity, timeout_ms), client=http_client
    )
    try:
        try:
            res = transport.request("POST", path, body=body, retry="never")
        except AgentChatError as err:
            return _read_error(kind, step, err, flow)
        return _read_success(kind, step, res, flow)
    finally:
        transport.close()


async def start_async(
    kind: AccountKind,
    input: AccountInput,
    *,
    client_identity: AgentChatClientIdentity | None,
    api_base: str = DEFAULT_BASE_URL,
    timeout_ms: int | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> StepResult:
    """:func:`start` on an event loop."""
    path, body = _start_request(kind, input)
    async with AsyncHttpTransport(
        _static_options(api_base, client_identity, timeout_ms), client=http_client
    ) as transport:
        try:
            res = await transport.request("POST", path, body=body, retry="never")
        except AgentChatError as err:
            return _read_error(kind, kind, err, None)
        return _read_success(kind, kind, res, None)


async def submit_code_async(
    flow: SignupFlow,
    code: str,
    *,
    client_identity: AgentChatClientIdentity | None,
    api_base: str = DEFAULT_BASE_URL,
    timeout_ms: int | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> StepResult:
    """:func:`submit_code` on an event loop."""
    path, body = _verify_request(flow, code)
    step = str(flow.get("next_step"))
    kind = _kind_of(step)
    async with AsyncHttpTransport(
        _static_options(api_base, client_identity, timeout_ms), client=http_client
    ) as transport:
        try:
            res = await transport.request("POST", path, body=body, retry="never")
        except AgentChatError as err:
            return _read_error(kind, step, err, flow)
        return _read_success(kind, step, res, flow)


# ─────────────── run(): the loop ───────────────


@dataclass(frozen=True)
class _Start:
    kind: AccountKind
    input: AccountInput


@dataclass(frozen=True)
class _Submit:
    flow: SignupFlow
    code: str


@dataclass(frozen=True)
class _Ask:
    question: Question


@dataclass(frozen=True)
class _Show:
    message: str


@dataclass(frozen=True)
class _Sleep:
    seconds: float


_Effect = Union[_Start, _Submit, _Ask, _Show, _Sleep]


def _now() -> float:
    """Seconds on a monotonic clock (a test replaces it with fake time)."""
    return time.monotonic()


def _run_kind(step: Any) -> AccountKind | None:
    if step == "register":
        return "register"
    if step == "recover":
        return "recover"
    return None


def _stopped(result: StepResult) -> RunResult:
    return RunResult(outcome="stopped", code=result.code, message=result.message)


def _handles(details: Mapping[str, Any] | None) -> tuple[str, ...]:
    raw = details.get("handles") if details is not None else None
    if not isinstance(raw, list):
        return ()
    items: list[Any] = raw
    return tuple(h for h in items if isinstance(h, str))


def _check_kind(kind: Any) -> AccountKind:
    run_kind = _run_kind(kind)
    if run_kind is None:
        raise ValueError(f"run() takes kind 'register' or 'recover', not {kind!r}")
    return run_kind


def _machine(
    kind: AccountKind, input: AccountInput, labels: Mapping[str, str]
) -> Generator[_Effect, Any, RunResult]:
    """The loop of :func:`run`, without I/O: it yields what to do and is
    sent the outcome (a :class:`StepResult` for a call, an :data:`Answer`
    for a question, ``None`` otherwise)."""
    values = cast("AccountInput", dict(input))
    result: StepResult = yield _Start(kind, cast("AccountInput", dict(values)))
    received_at = _now()
    last_code: str | None = None
    while True:
        if result.message:
            yield _Show(result.message)
        flow = result.flow
        step = flow.get("next_step")
        action = result.action

        if action == "done":
            return RunResult(outcome="done", api_key=result.api_key, handle=result.handle)

        if action == "ask_code":
            if not flow.get("pending_id"):
                return _stopped(result)
            answer = yield _Ask(Question("code", labels.get("code"), flow))
            if isinstance(answer, NewCode):
                # Never before the server allows it: a recover started too
                # soon answers like any other (the decoy design) and sends nothing.
                start_kind = _run_kind(_kind_of(step))
                resend = whole_seconds(flow.get("resend_after_s")) or 0
                wait = max(0.0, resend - (_now() - received_at))
                if start_kind is None or wait > WAIT_MAX_S:
                    return _stopped(result)
                if wait > 0:
                    yield _Sleep(wait)
                result = yield _Start(start_kind, cast("AccountInput", dict(values)))
                received_at, last_code = _now(), None
                continue
            if not isinstance(answer, str):
                return RunResult(outcome="cancelled")
            last_code = answer
            result = yield _Submit(flow, answer)
            received_at = _now()
            continue

        if action == "ask_param":
            param = str(flow.get("param"))
            start_kind = _run_kind(step)
            if start_kind is None or param == "code":
                return _stopped(result)
            answer = yield _Ask(
                Question(param, labels.get(param), flow, handles=_handles(result.details))
            )
            if not isinstance(answer, str):
                return RunResult(outcome="cancelled")
            values = cast("AccountInput", {**values, param: answer})
            result = yield _Start(start_kind, cast("AccountInput", dict(values)))
            received_at, last_code = _now(), None
            continue

        if action == "offer_new_code":
            start_kind = _run_kind(step)
            if start_kind is None:
                return _stopped(result)
            answer = yield _Ask(Question("new_code", None, flow))
            if not isinstance(answer, NewCode):
                return RunResult(outcome="cancelled")
            result = yield _Start(start_kind, cast("AccountInput", dict(values)))
            received_at, last_code = _now(), None
            continue

        if action == "wait_then_retry":
            yield _Sleep(float(whole_seconds(flow.get("retry_after_s")) or 0))
            start_kind = _run_kind(step)
            if start_kind is not None:
                result = yield _Start(start_kind, cast("AccountInput", dict(values)))
                last_code = None
            elif step in _VERIFY_STEPS and last_code is not None and flow.get("pending_id"):
                result = yield _Submit(flow, last_code)
            else:
                return _stopped(result)
            received_at = _now()
            continue

        return _stopped(result)


def run(
    *,
    kind: AccountKind,
    input: AccountInput,
    ask: Callable[[Question], Answer],
    show: Callable[[str], Any],
    client_identity: AgentChatClientIdentity | None,
    sleep: Callable[[float], Any] | None = None,
    api_base: str = DEFAULT_BASE_URL,
    labels: Mapping[str, str] | None = None,
    timeout_ms: int | None = None,
    http_client: httpx.Client | None = None,
) -> RunResult:
    """Register an agent, or recover a lost key, from start to the key.

    Follows ``next_step``. Shows every server message with ``show``, as is.
    Asks with ``ask`` only for the input the server names (``param``) or for
    the code; on the code the person can enter it, ask for a new code
    (:data:`NEW_CODE`), or cancel (``None``). It never requests a new code
    unasked, and when the code is dead it asks first (the ``new_code``
    question). When the server says to wait (``retry_after_s``), or a new
    code is asked for before ``resend_after_s``, it waits with ``sleep`` for
    up to :data:`WAIT_MAX_S` seconds; a longer wait stops the run with the
    server's message. No attempt limits of its own: the server's decide.

    Returns ``done`` with the API key and the handle the server returned
    (save both; save nothing otherwise), ``cancelled``, or ``stopped`` with
    the code and message that ended it (already shown). Raises
    :class:`~agentchatme.ConnectionError` when the server cannot be reached;
    whatever ``ask``, ``show`` or ``sleep`` raise propagates.
    """
    machine = _machine(_check_kind(kind), input, {**DEFAULT_LABELS, **(labels or {})})
    pause = sleep if sleep is not None else time.sleep
    options: dict[str, Any] = {
        "client_identity": client_identity,
        "api_base": api_base,
        "timeout_ms": timeout_ms,
        "http_client": http_client,
    }
    try:
        effect = next(machine)
        while True:
            outcome: Any = None
            if isinstance(effect, _Start):
                outcome = start(effect.kind, effect.input, **options)
            elif isinstance(effect, _Submit):
                outcome = submit_code(effect.flow, effect.code, **options)
            elif isinstance(effect, _Ask):
                outcome = ask(effect.question)
            elif isinstance(effect, _Show):
                show(effect.message)
            else:
                pause(effect.seconds)
            effect = machine.send(outcome)
    except StopIteration as end:
        return cast("RunResult", end.value)
    finally:
        machine.close()


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


async def run_async(
    *,
    kind: AccountKind,
    input: AccountInput,
    ask: Callable[[Question], Answer | Awaitable[Answer]],
    show: Callable[[str], Any],
    client_identity: AgentChatClientIdentity | None,
    sleep: Callable[[float], Any] | None = None,
    api_base: str = DEFAULT_BASE_URL,
    labels: Mapping[str, str] | None = None,
    timeout_ms: int | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> RunResult:
    """:func:`run` on an event loop. ``ask``, ``show`` and ``sleep`` may be
    plain functions or coroutine functions; ``sleep`` defaults to
    :func:`asyncio.sleep`."""
    machine = _machine(_check_kind(kind), input, {**DEFAULT_LABELS, **(labels or {})})
    pause = sleep if sleep is not None else asyncio.sleep
    options: dict[str, Any] = {
        "client_identity": client_identity,
        "api_base": api_base,
        "timeout_ms": timeout_ms,
        "http_client": http_client,
    }
    try:
        effect = next(machine)
        while True:
            outcome: Any = None
            if isinstance(effect, _Start):
                outcome = await start_async(effect.kind, effect.input, **options)
            elif isinstance(effect, _Submit):
                outcome = await submit_code_async(effect.flow, effect.code, **options)
            elif isinstance(effect, _Ask):
                outcome = await _maybe_await(ask(effect.question))
            elif isinstance(effect, _Show):
                await _maybe_await(show(effect.message))
            else:
                await _maybe_await(pause(effect.seconds))
            effect = machine.send(outcome)
    except StopIteration as end:
        return cast("RunResult", end.value)
    finally:
        machine.close()


# ─────────────── check_key ───────────────


def _key_options(
    api_key: str,
    api_base: str,
    client_identity: AgentChatClientIdentity | None,
    timeout_ms: int | None,
) -> HttpTransportOptions:
    options = _static_options(api_base, client_identity, timeout_ms)
    options.api_key = api_key
    return options


def _read_me(res: HttpResponse) -> CheckKeyResult:
    data: Any = res.data
    handle = _handle_of(cast("Mapping[str, Any]", data)) if isinstance(data, Mapping) else None
    if handle is None:
        raise ConnectionError("AgentChat SDK: GET /v1/agents/me answered without a handle")
    return CheckKeyResult(ok=True, handle=handle)


def check_key(
    api_key: str,
    *,
    client_identity: AgentChatClientIdentity | None,
    api_base: str = DEFAULT_BASE_URL,
    timeout_ms: int | None = None,
    http_client: httpx.Client | None = None,
) -> CheckKeyResult:
    """Does ``api_key`` work? One ``GET /v1/agents/me``, never retried.

    ``ok`` with the key's handle (save that one), or not ``ok`` with the
    server's code and message (``UNAUTHORIZED`` for a key that does not
    authenticate). Raises :class:`~agentchatme.ConnectionError` when the
    server cannot be reached.
    """
    transport = HttpTransport(
        _key_options(api_key, api_base, client_identity, timeout_ms), client=http_client
    )
    try:
        try:
            res = transport.request("GET", "/v1/agents/me", retry="never")
        except AgentChatError as err:
            return CheckKeyResult(ok=False, code=err.code, message=str(err))
        return _read_me(res)
    finally:
        transport.close()


async def check_key_async(
    api_key: str,
    *,
    client_identity: AgentChatClientIdentity | None,
    api_base: str = DEFAULT_BASE_URL,
    timeout_ms: int | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> CheckKeyResult:
    """:func:`check_key` on an event loop."""
    async with AsyncHttpTransport(
        _key_options(api_key, api_base, client_identity, timeout_ms), client=http_client
    ) as transport:
        try:
            res = await transport.request("GET", "/v1/agents/me", retry="never")
        except AgentChatError as err:
            return CheckKeyResult(ok=False, code=err.code, message=str(err))
        return _read_me(res)


__all__ = [
    "DEFAULT_LABELS",
    "NEW_CODE",
    "WAIT_MAX_S",
    "AccountInput",
    "AccountKind",
    "Answer",
    "CheckKeyResult",
    "NewCode",
    "Question",
    "RunOutcome",
    "RunResult",
    "SignupAction",
    "SignupFlow",
    "StepResult",
    "action_of",
    "check_key",
    "check_key_async",
    "flow_of",
    "run",
    "run_async",
    "start",
    "start_async",
    "submit_code",
    "submit_code_async",
]
