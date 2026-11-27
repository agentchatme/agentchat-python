"""Sign-up for an integration's setup (``agentchatme.integration.account``):
register a new agent or recover a lost key by email code, following the
server's answer instead of a rulebook of its own.

Every answer of the sign-up endpoints carries a flow object (contract v1,
"Sign-up flows"): the next call to make (``next_step``), the state of the
pending code, the one input that must change (``param``), how long to wait
(``retry_after_s``), and a ``message`` written to be shown to a person as
is. The integration asks the person, shows the message, and saves the key;
the rules stay on the server.

- ``flow_of(answer)``: the flow object of a body or SDK error, or ``None``.
- ``action_of(flow, http_status, called_step)``: what the client does next
  (the conformance vocabulary).
- ``start(kind, input, ...)``: one start call (sends a code).
- ``submit_code(flow, code, ...)``: one verify call.
- ``run(kind=, input=, ask=, show=, ...)``: the whole conversation, for a wizard.
- ``check_key(api_key, ...)``: does the server accept this key
  (``GET /v1/agents/me``).

``*_async`` twins run on an event loop. The step functions work alone, one
per process run: save the returned ``flow`` (plain JSON) between
:func:`start` and :func:`submit_code`. They return every answer the server
gives and raise only when there is no answer, and for misuse.

Rules for every integration: never save a key the server rejected; save
the handle the server returned, not the one typed; show ``message``
verbatim and branch only on ``action`` / ``next_step`` / ``code``, never on
the text.

A server older than the flow contract sends no ``next_step``: the step
functions then read today's codes into the same flow shape (``legacy`` on
the result), so a caller follows one path for both.

The TypeScript SDK's ``integration.account`` is the twin, with the same
names in TypeScript casing.
"""

from __future__ import annotations

import asyncio
import inspect
import math
import time
from collections.abc import Generator, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Literal, TypedDict, Union, cast

import httpx

from .._client import DEFAULT_BASE_URL, _static_options
from .._client_identity import AgentChatClientIdentity
from .._http import AsyncHttpTransport, HttpResponse, HttpTransport, HttpTransportOptions
from .._signup_flow import flow_of, whole_seconds
from ..errors import AgentChatError, ConnectionError, RateLimitedError
from ..types.reachability import Reachability, reachability_of
from ..types.signup import SIGNUP_FLOW_VERSION, SignupFlow

MAX_WAIT_S = 60
"""The longest wait :func:`run` sits out; a longer one stops the flow."""

AccountKind = Literal["register", "recover"]
"""The flows an integration's setup runs. Key rotation needs the old key:
:meth:`~agentchatme.AgentChatClient.rotate_key`."""

SignupAction = Literal[
    "done",
    "connect",
    "ask_code",
    "ask_param",
    "offer_new_code",
    "wait_then_retry",
    "stop",
    "legacy",
]
"""What the client does next, derived only from ``next_step``, ``status``,
``param``, ``retry_after_s``, the HTTP status and the step that was called
(conformance ``signup-flow.cases.json``; :func:`action_of` has the order):

- ``done``: ``next_step: "done"`` (the key is in the body), or
  ``get_my_status`` (a connector sign-up's status: the connector is signed in).
- ``connect``: ``next_step: "connect"`` (a connector sign-up): add the
  connector at ``connect.mcp_url``, show ``message``, then follow
  ``connect.status_url``. :func:`run` signs up with a key: it stops here.
- ``ask_code``: a verify step, no ``retry_after_s``.
- ``ask_param``: a start step with ``param``: re-ask that field, call the
  start step again.
- ``offer_new_code``: a start step, no ``param``, the code is gone
  (``status`` ``expired`` / ``max_attempts_reached``, or a 429 to a code),
  no wait over 60 s: ask before sending a new code (wait ``retry_after_s``
  first when set).
- ``wait_then_retry``: 429 with ``retry_after_s`` <= 60 naming the step
  that was called: wait, then repeat it.
- ``stop``: a longer wait, a 5xx, an unknown ``next_step``, anything else:
  show ``message``, end.
- ``legacy``: no flow object (a server older than the flow contract).
"""


class AccountLabels(TypedDict):
    """The field labels :func:`run` asks with."""

    email: str
    handle: str
    code: str


DEFAULT_LABELS: AccountLabels = {
    "email": "Email address",
    "handle": "Handle",
    "code": "Enter the 6-digit code",
}
"""The default field labels: the only words this helper brings (GOV.UK /
One Login wording)."""


class _RegisterInputRequired(TypedDict):
    email: str
    handle: str


class RegisterInput(_RegisterInputRequired, total=False):
    """Input of the register start call (``POST /v1/register``)."""

    display_name: str
    description: str


class _RecoverInputRequired(TypedDict):
    email: str


class RecoverInput(_RecoverInputRequired, total=False):
    """Input of the recover start call (``POST /v1/agents/recover``). Pass
    ``handle`` whenever it is known."""

    handle: str


AnswerKind = Literal["value", "new_code", "cancel"]


@dataclass(frozen=True)
class AccountAnswer:
    """The person's answer to an :class:`AccountQuestion`:
    ``AccountAnswer("value", "123456")``, ``AccountAnswer("new_code")`` or
    ``AccountAnswer("cancel")``."""

    answer: AnswerKind
    value: str | None = None
    """``value`` only: what the person typed."""


@dataclass(frozen=True)
class AccountQuestion:
    """One question :func:`run` puts to the person."""

    field: Literal["email", "handle", "code", "new_code"]
    """``email`` / ``handle``: enter that field. ``code``: enter the emailed
    code. ``new_code``: the code is gone (expired, or too many wrong tries);
    send a new one?"""
    label: str | None
    """The field's label (:data:`DEFAULT_LABELS` unless overridden);
    ``None`` for ``new_code``."""
    answers: tuple[AnswerKind, ...]
    """The answers allowed: ``value`` (typed input), ``new_code``,
    ``cancel`` (always)."""
    flow: SignupFlow | None
    """The flow the question comes from (``None`` before the first call)."""
    handles: tuple[str, ...] | None = None
    """After ``HANDLE_REQUIRED``: the handles to choose from."""


@dataclass(frozen=True)
class StepResult:
    """One step's answer. ``flow`` is plain JSON: save it between a
    :func:`start` and a :func:`submit_code` that run in different processes."""

    flow: SignupFlow
    """The server's flow object; for a legacy server, today's answer read
    into the same shape."""
    message: str
    """The server's sentence. Show it as is."""
    action: SignupAction
    """What to do next. Never ``legacy``: a legacy answer is read into a flow first."""
    http_status: int
    """HTTP status of the answer."""
    called_step: str
    """The step that was called (``register``, ``verify``, ``recover``,
    ``recover_verify``): :func:`action_of` reads it."""
    legacy: bool
    """True when the server sent no flow object and the answer was read from its code."""
    code: str | None = None
    """The error code, for a non-2xx answer."""
    api_key: str | None = field(default=None, repr=False)
    """On ``done``: the API key. Shown once; save it."""
    handle: str | None = None
    """On ``done``: the handle the server returned. Save this one, not the typed one."""
    handles: tuple[str, ...] | None = None
    """``HANDLE_REQUIRED``: the handles on this email (``details.handles``), to choose from."""
    reachability: Reachability | None = None
    """On ``done``, when the server sent it: whether AgentChat can start the
    agent's runs (contract v1 §3.12). For a new account ``state == "none"``
    and ``text`` the ways to be woken; show ``text`` to the person or the
    agent as it is. ``action`` stays ``done`` whatever it says."""


RunOutcome = Literal["done", "cancelled", "stopped"]


@dataclass(frozen=True)
class RunResult:
    """How :func:`run` ended: ``done`` (``api_key`` and ``handle`` set; save
    both; ``reachability`` when the server sent it), ``cancelled``, or
    ``stopped`` (``code`` and ``message`` set; the message has been shown)."""

    outcome: RunOutcome
    api_key: str | None = field(default=None, repr=False)
    handle: str | None = None
    code: str | None = None
    """``stopped``: the error code, or ``None`` when a 2xx answer stopped it."""
    message: str | None = None
    reachability: Reachability | None = None
    """``done``: as on :class:`StepResult`; show its ``text`` as it is."""


@dataclass(frozen=True)
class CheckKeyResult:
    """``ok`` with the handle the key belongs to, or not ``ok`` with the
    server's ``code`` and ``message``."""

    ok: bool
    handle: str | None = None
    code: str | None = None
    message: str | None = None


# ─────────────── what the client does next (pure) ───────────────

_START_STEPS = frozenset({"register", "recover", "rotate_key"})
_VERIFY_STEPS = frozenset({"verify", "recover_verify", "rotate_key_verify"})
_KNOWN_STEPS = _START_STEPS | _VERIFY_STEPS | {"done"}


def action_of(flow: SignupFlow | None, http_status: int, called_step: str) -> SignupAction:
    """What the client does next (the table on :data:`SignupAction`), from
    the flow object, the answer's HTTP status and the step that was called
    (``register``, ``verify``, ``recover``, ``recover_verify``, ...). Pure;
    first match wins:

    - ``legacy``: no flow object.
    - ``done``: ``next_step`` is ``done``, or ``get_my_status``.
    - ``connect``: ``next_step`` is ``connect``.
    - ``stop``: a ``next_step`` this release does not know, or any 5xx (the
      account may exist after a verify 500: never steer to a second one).
    - ``ask_code``: a verify step, no ``retry_after_s``.
    - ``ask_param``: a start step with ``param``.
    - ``offer_new_code``: a start step, no ``param``, the code is gone
      (``status`` ``expired`` / ``max_attempts_reached``, or a 429 to a
      code), and no ``retry_after_s`` or one of at most 60 s (waited out
      only if the person says yes). Any other 4xx to a code (a malformed
      body, ``AGENT_NOT_FOUND`` after proof) is ``stop``.
    - ``wait_then_retry``: a 429 with ``retry_after_s`` <= 60 that names the
      step that was called (repeating it sends nothing new).
    - ``stop``: anything else.
    """
    if flow is None:
        return "legacy"
    step = flow.get("next_step")
    if step in ("done", "get_my_status"):
        return "done"
    if step == "connect":
        return "connect"
    if step not in _KNOWN_STEPS:
        return "stop"
    if http_status >= 500:
        return "stop"
    wait = whole_seconds(flow.get("retry_after_s"))
    if step in _VERIFY_STEPS and wait is None:
        return "ask_code"
    if step in _START_STEPS:
        if flow.get("param") is not None:
            return "ask_param"
        code_gone = flow.get("status") in ("expired", "max_attempts_reached")
        turned_away = http_status == 429 and called_step in _VERIFY_STEPS
        if (code_gone or turned_away) and (wait is None or wait <= MAX_WAIT_S):
            return "offer_new_code"
    if http_status == 429 and wait is not None and wait <= MAX_WAIT_S and step == called_step:
        return "wait_then_retry"
    return "stop"


# ─────────────── reading one answer ───────────────


@dataclass(frozen=True)
class _Steps:
    start: str
    verify: str
    start_path: str
    verify_path: str


_STEPS: Mapping[str, _Steps] = {
    "register": _Steps("register", "verify", "/v1/register", "/v1/register/verify"),
    "recover": _Steps(
        "recover", "recover_verify", "/v1/agents/recover", "/v1/agents/recover/verify"
    ),
}

_StepCalled = Literal["start", "verify"]


@dataclass(frozen=True)
class _Answer:
    """What one call answered, before it is read."""

    http_status: int
    body: Any
    error: AgentChatError | None


def _text(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _key_and_handle(body: Any) -> tuple[str | None, str | None]:
    if not isinstance(body, Mapping):
        return None, None
    record = cast("Mapping[str, Any]", body)
    agent = record.get("agent")
    nested = cast("Mapping[str, Any]", agent).get("handle") if isinstance(agent, Mapping) else None
    return _text(record.get("api_key")), _text(record.get("handle")) or _text(nested)


def _handles_of(error: AgentChatError | None) -> tuple[str, ...] | None:
    raw = error.details.get("handles") if error is not None and error.details is not None else None
    if not isinstance(raw, list):
        return None
    items: list[Any] = raw
    return tuple(h for h in items if isinstance(h, str))


def _legacy_wait_seconds(error: AgentChatError) -> int | None:
    """Seconds to wait on a 429, from wherever an older server put it."""
    if error.retry_after_s is not None:
        return error.retry_after_s
    if isinstance(error, RateLimitedError) and error.retry_after_ms is not None:
        return max(0, math.ceil(error.retry_after_ms / 1000))
    return None


def _legacy_field_of(error: AgentChatError) -> str | None:
    """The zod field an older server's ``VALIDATION_ERROR`` names, when it
    names exactly one this helper asks for."""
    field_errors = error.details.get("fieldErrors") if error.details is not None else None
    if not isinstance(field_errors, Mapping):
        return None
    named = [name for name in ("email", "handle", "code") if name in field_errors]
    return named[0] if len(named) == 1 else None


def _legacy_flow(
    kind: str, called: _StepCalled, answer: _Answer, submitted: SignupFlow | None
) -> SignupFlow:
    """Today's answers, read into the flow shape: a server older than the
    flow contract sends ``{code, message}`` (or ``{pending_id, message}``)
    and nothing more. The reading follows what the integrations did with
    those codes; the message stays the server's own."""
    steps = _STEPS[kind]

    def start_flow(**extra: Any) -> SignupFlow:
        flow = {"version": SIGNUP_FLOW_VERSION, "flow": kind, "next_step": steps.start, **extra}
        return cast("SignupFlow", flow)

    def pending_again(**extra: Any) -> SignupFlow:
        flow: dict[str, Any] = {
            "version": SIGNUP_FLOW_VERSION,
            "flow": kind,
            "next_step": steps.verify,
            "status": "pending",
        }
        if submitted is not None and "pending_id" in submitted:
            flow["pending_id"] = submitted["pending_id"]
        flow["code_length"] = submitted.get("code_length", 6) if submitted is not None else 6
        flow.update(extra)
        return cast("SignupFlow", flow)

    error, body = answer.error, answer.body
    if error is None:
        # A 2xx without what it should carry reads as a bare start step: stop.
        if called == "start":
            pending_id = (
                _text(cast("Mapping[str, Any]", body).get("pending_id"))
                if isinstance(body, Mapping)
                else None
            )
            if pending_id is None:
                return start_flow()
            sent = {
                "version": SIGNUP_FLOW_VERSION,
                "flow": kind,
                "next_step": steps.verify,
                "status": "pending",
                "pending_id": pending_id,
                "code_length": 6,
            }
            return cast("SignupFlow", sent)
        api_key, _handle = _key_and_handle(body)
        if api_key is None:
            return start_flow()
        done = {"version": SIGNUP_FLOW_VERSION, "flow": kind, "next_step": "done", "status": "approved"}
        return cast("SignupFlow", done)

    if error.status == 429:
        if error.code == "OTP_ATTEMPTS_EXHAUSTED":
            return start_flow(status="max_attempts_reached")
        # A 429 that names no wait is a bare start step: stop.
        wait = _legacy_wait_seconds(error)
        if wait is None:
            return start_flow()
        return pending_again(retry_after_s=wait) if called == "verify" else start_flow(retry_after_s=wait)
    code = error.code
    if code in ("HANDLE_TAKEN", "INVALID_HANDLE", "HANDLE_REQUIRED"):
        return start_flow(param="handle")
    if code in ("EMAIL_LIMIT_REACHED", "EMAIL_TAKEN", "EMAIL_EXHAUSTED"):
        return start_flow(param="email")
    if code == "INVALID_CODE":
        return pending_again(param="code")
    if code == "EXPIRED":
        return start_flow(status="expired")
    if code == "VALIDATION_ERROR":
        named = _legacy_field_of(error)
        if called == "verify":
            return pending_again(param="code") if named == "code" else start_flow()
        return start_flow(param=named) if named in ("email", "handle") else start_flow()
    return start_flow()


def _read(
    kind: str, called: _StepCalled, answer: _Answer, submitted: SignupFlow | None
) -> StepResult:
    error, body = answer.error, answer.body
    served = flow_of(error if error is not None else body)
    legacy = served is None
    flow = served if served is not None else _legacy_flow(kind, called, answer, submitted)
    # An answer to a code that sends the client back to the same verify step
    # without naming the pending id (a malformed code, a per-IP 429) still
    # refers to the one submitted, which stays alive.
    if (
        not legacy
        and submitted is not None
        and "pending_id" in submitted
        and "pending_id" not in flow
        and flow.get("next_step") == submitted.get("next_step")
    ):
        flow = cast("SignupFlow", {**flow, "pending_id": submitted["pending_id"]})
    steps = _STEPS[kind]
    called_step = steps.start if called == "start" else steps.verify
    action = action_of(flow, answer.http_status, called_step)
    api_key, handle = _key_and_handle(body) if error is None else (None, None)
    # ``done`` means the key is in the body; without one there is nothing to save.
    if action == "done" and (api_key is None or handle is None):
        action = "stop"
    # A code step without a pending id cannot be answered.
    if action == "ask_code" and "pending_id" not in flow:
        action = "stop"
    if error is not None:
        message = str(error)
    else:
        raw = cast("Mapping[str, Any]", body).get("message") if isinstance(body, Mapping) else None
        message = raw if isinstance(raw, str) else ""
    is_done = action == "done"
    reachability = (
        reachability_of(cast("Mapping[str, Any]", body).get("reachability"))
        if is_done and isinstance(body, Mapping)
        else None
    )
    return StepResult(
        flow=flow,
        message=message,
        action=action,
        http_status=answer.http_status,
        called_step=called_step,
        legacy=legacy,
        code=error.code if error is not None else None,
        api_key=api_key if is_done else None,
        handle=handle if is_done else None,
        handles=_handles_of(error),
        reachability=reachability,
    )


def _start_call(kind: Any, input: Any) -> tuple[_Steps, dict[str, Any]]:
    steps = _STEPS.get(kind) if isinstance(kind, str) else None
    if steps is None:
        raise ValueError(f"integration.account.start: unknown kind {kind!r}")
    if not isinstance(input, Mapping):
        raise TypeError("integration.account.start: input must be a mapping")
    values = cast("Mapping[str, Any]", input)
    names = (
        ("email", "handle", "display_name", "description")
        if kind == "register"
        else ("email", "handle")
    )
    # Optional fields are sent only when set: the server validates a present
    # field, so None must be absent, never null.
    return steps, {name: values[name] for name in names if values.get(name) is not None}


def _verify_call(flow: Any, code: Any) -> tuple[str, _Steps, dict[str, Any]]:
    record = cast("Mapping[str, Any]", flow) if isinstance(flow, Mapping) else None
    if record is None or not isinstance(record.get("next_step"), str):
        raise TypeError("integration.account.submit_code: flow must be a sign-up flow object")
    step = record["next_step"]
    kind = "register" if step == "verify" else "recover" if step == "recover_verify" else None
    if kind is None:
        raise ValueError(
            "integration.account.submit_code: the flow is not waiting for a code "
            f"(next_step {step!r}); submit only to next_step 'verify' or 'recover_verify' "
            "(key rotation: AgentChatClient.rotate_key_verify)"
        )
    pending_id = record.get("pending_id")
    if not isinstance(pending_id, str) or not pending_id:
        raise ValueError("integration.account.submit_code: the flow carries no pending_id")
    if not isinstance(code, str):
        raise TypeError("integration.account.submit_code: code must be a str")
    return kind, _STEPS[kind], {"pending_id": pending_id, "code": code}


def _options(
    api_base: str,
    client_identity: AgentChatClientIdentity | None,
    timeout_ms: int | None,
    api_key: str | None = None,
) -> HttpTransportOptions:
    options = _static_options(api_base, client_identity, timeout_ms)
    if api_key is not None:
        options.api_key = api_key
    return options


def _call(
    options: HttpTransportOptions,
    http_client: httpx.Client | None,
    method: Literal["GET", "POST"],
    path: str,
    body: Any = None,
) -> _Answer:
    transport = HttpTransport(options, client=http_client)
    try:
        res: HttpResponse = transport.request(method, path, body=body, retry="never")
    except AgentChatError as err:
        # An answer from the server is an outcome; anything else (no answer)
        # is the caller's to handle.
        return _Answer(err.status, None, err)
    finally:
        transport.close()
    return _Answer(res.status, res.data, None)


async def _call_async(
    options: HttpTransportOptions,
    http_client: httpx.AsyncClient | None,
    method: Literal["GET", "POST"],
    path: str,
    body: Any = None,
) -> _Answer:
    async with AsyncHttpTransport(options, client=http_client) as transport:
        try:
            res = await transport.request(method, path, body=body, retry="never")
        except AgentChatError as err:
            return _Answer(err.status, None, err)
    return _Answer(res.status, res.data, None)


# ─────────────── the steps ───────────────


def start(
    kind: AccountKind,
    input: RegisterInput | RecoverInput,
    *,
    client_identity: AgentChatClientIdentity | None,
    api_base: str = DEFAULT_BASE_URL,
    timeout_ms: int | None = None,
    http_client: httpx.Client | None = None,
) -> StepResult:
    """Start a flow: ``register`` sends a code to ``input["email"]`` for a new
    agent ``input["handle"]``; ``recover`` sends a code to the email the
    agent was registered with. One call, never retried (a second code would
    be sent).

    Returns every answer the server gives; raises
    :class:`~agentchatme.ConnectionError` only when there is no answer
    (network, timeout). A given ``http_client`` is never closed here.
    """
    steps, body = _start_call(kind, input)
    options = _options(api_base, client_identity, timeout_ms)
    answer = _call(options, http_client, "POST", steps.start_path, body)
    return _read(kind, "start", answer, None)


def submit_code(
    flow: SignupFlow,
    code: str,
    *,
    client_identity: AgentChatClientIdentity | None,
    api_base: str = DEFAULT_BASE_URL,
    timeout_ms: int | None = None,
    http_client: httpx.Client | None = None,
) -> StepResult:
    """Submit the emailed code for a pending flow (a flow whose
    ``next_step`` is ``verify`` or ``recover_verify``, as :func:`start` or an
    earlier :func:`submit_code` returned it, or as saved to disk). One call,
    never retried. On ``done``, ``api_key`` and ``handle`` are set: save both.
    Raises ``ValueError`` for a flow that is not waiting for a code."""
    kind, steps, body = _verify_call(flow, code)
    options = _options(api_base, client_identity, timeout_ms)
    answer = _call(options, http_client, "POST", steps.verify_path, body)
    return _read(kind, "verify", answer, flow)


async def start_async(
    kind: AccountKind,
    input: RegisterInput | RecoverInput,
    *,
    client_identity: AgentChatClientIdentity | None,
    api_base: str = DEFAULT_BASE_URL,
    timeout_ms: int | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> StepResult:
    """:func:`start` on an event loop."""
    steps, body = _start_call(kind, input)
    options = _options(api_base, client_identity, timeout_ms)
    answer = await _call_async(options, http_client, "POST", steps.start_path, body)
    return _read(kind, "start", answer, None)


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
    kind, steps, body = _verify_call(flow, code)
    options = _options(api_base, client_identity, timeout_ms)
    answer = await _call_async(options, http_client, "POST", steps.verify_path, body)
    return _read(kind, "verify", answer, flow)


def _read_me(answer: _Answer) -> CheckKeyResult:
    if answer.error is not None:
        return CheckKeyResult(ok=False, code=answer.error.code, message=str(answer.error))
    body = answer.body
    handle = (
        _text(cast("Mapping[str, Any]", body).get("handle")) if isinstance(body, Mapping) else None
    )
    if handle is None:
        raise ConnectionError(
            f"integration.account.check_key: GET /v1/agents/me answered {answer.http_status} "
            "without a handle"
        )
    return CheckKeyResult(ok=True, handle=handle)


def check_key(
    api_key: str,
    *,
    client_identity: AgentChatClientIdentity | None,
    api_base: str = DEFAULT_BASE_URL,
    timeout_ms: int | None = None,
    http_client: httpx.Client | None = None,
) -> CheckKeyResult:
    """Does the server accept this key? One ``GET /v1/agents/me``, never
    retried. ``ok`` with the handle the key belongs to, or not ``ok`` with
    the server's code and message. Raises
    :class:`~agentchatme.ConnectionError` when no usable answer came (no
    answer, or a 2xx without a handle): then nothing is known about the key."""
    options = _options(api_base, client_identity, timeout_ms, api_key)
    return _read_me(_call(options, http_client, "GET", "/v1/agents/me"))


async def check_key_async(
    api_key: str,
    *,
    client_identity: AgentChatClientIdentity | None,
    api_base: str = DEFAULT_BASE_URL,
    timeout_ms: int | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> CheckKeyResult:
    """:func:`check_key` on an event loop."""
    options = _options(api_base, client_identity, timeout_ms, api_key)
    return _read_me(await _call_async(options, http_client, "GET", "/v1/agents/me"))


# ─────────────── the conversation ───────────────

_FIELD_ANSWERS: tuple[AnswerKind, ...] = ("value", "cancel")
_CODE_ANSWERS: tuple[AnswerKind, ...] = ("value", "new_code", "cancel")
_NEW_CODE_ANSWERS: tuple[AnswerKind, ...] = ("new_code", "cancel")


@dataclass(frozen=True)
class _Start:
    kind: AccountKind
    input: Mapping[str, Any]


@dataclass(frozen=True)
class _Submit:
    flow: SignupFlow
    code: str


@dataclass(frozen=True)
class _Ask:
    question: AccountQuestion


@dataclass(frozen=True)
class _Show:
    message: str


@dataclass(frozen=True)
class _Sleep:
    seconds: float


_Effect = Union[_Start, _Submit, _Ask, _Show, _Sleep]
_Machine = Generator[_Effect, Any, RunResult]


def _now() -> float:
    """Seconds on a monotonic clock (tests replace it with fake time)."""
    return time.monotonic()


def _wall_now() -> float:
    """Seconds since the epoch (tests replace it with fake time)."""
    return time.time()


MIN_WAIT_S = 1
"""The shortest automatic wait :func:`run` makes (no tight retry loop on a
zero wait)."""

MAX_AUTO_WAITS = 3
"""Automatic waits :func:`run` makes in a row before it asks the person."""


def _expiry_of(flow: SignupFlow, received_at: float) -> float | None:
    """When the flow's code stops working, on the monotonic clock:
    ``expires_in_s`` from the answer's receipt (no clock skew), else
    ``expires_at`` against this machine's wall clock; ``None`` when neither."""
    seconds = whole_seconds(flow.get("expires_in_s"))
    if seconds is not None:
        return received_at + seconds
    text = flow.get("expires_at")
    if isinstance(text, str):
        try:
            moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if moment.tzinfo is None:
            moment = moment.replace(tzinfo=timezone.utc)
        return received_at + (moment.timestamp() - _wall_now())
    return None


@dataclass(frozen=True)
class _Held:
    flow: SignupFlow
    received_at: float
    expires_at: float | None
    """Monotonic seconds; ``None`` when the answers did not say."""


class _Stop:
    """The run ends on the last answer (shown already)."""


_STOP = _Stop()
_Next = Union[_Start, _Submit, _Stop, None]


def _checked(question: AccountQuestion, answer: Any) -> AccountAnswer:
    if not isinstance(answer, AccountAnswer) or answer.answer not in question.answers:
        allowed = ", ".join(question.answers)
        raise TypeError(
            f"integration.account.run: ask answered {answer!r} to a {question.field} question "
            f"(allowed: {allowed})"
        )
    if answer.answer == "value" and not isinstance(answer.value, str):
        raise TypeError("integration.account.run: a value answer carries a str value")
    return answer


def _stopped(result: StepResult) -> RunResult:
    return RunResult(outcome="stopped", code=result.code, message=result.message)


def _machine(
    kind: AccountKind, input: Mapping[str, Any] | None, labels: AccountLabels
) -> _Machine:
    """:func:`run` without I/O: it yields what to do and is sent the outcome
    (a :class:`StepResult` for a call, an :class:`AccountAnswer` for a
    question, ``None`` otherwise)."""
    values: dict[str, Any] = dict(input or {})
    # The code the person holds (the flow of the last code question).
    held: _Held | None = None
    # When the email may next get a new code: the email, and the monotonic
    # time from the latest answer that said (resend_after_s from its receipt).
    resend_at: tuple[str, float] | None = None
    # Automatic waits since the person last answered.
    auto_waits = 0

    def holds_a_working_code() -> bool:
        return held is not None and held.expires_at is not None and held.expires_at > _now()

    def asked(question: AccountQuestion) -> Generator[_Effect, Any, AccountAnswer]:
        nonlocal auto_waits
        answer = _checked(question, (yield _Ask(question)))
        auto_waits = 0
        return answer

    def pause(seconds: float) -> Generator[_Effect, Any, None]:
        """One automatic wait, floored at :data:`MIN_WAIT_S`."""
        nonlocal auto_waits
        auto_waits += 1
        yield _Sleep(max(float(MIN_WAIT_S), seconds))

    def ask_field(
        name: Literal["email", "handle"],
        flow: SignupFlow | None,
        handles: tuple[str, ...] | None,
    ) -> Generator[_Effect, Any, bool]:
        """Ask for a field; False when the person cancelled."""
        question = AccountQuestion(
            field=name, label=labels[name], answers=_FIELD_ANSWERS, flow=flow, handles=handles or None
        )
        answer = yield from asked(question)
        if answer.answer != "value":
            return False
        values[name] = str(answer.value).strip()
        return True

    def start_call() -> _Start:
        email = values.get("email") or ""
        if kind == "recover":
            handle = values.get("handle")
            return _Start(kind, {"email": email, **({"handle": handle} if handle else {})})
        call = {"email": email, "handle": values.get("handle") or ""}
        for name in ("display_name", "description"):
            if values.get(name) is not None:
                call[name] = values[name]
        return _Start(kind, call)

    def start_when_allowed() -> Generator[_Effect, Any, _Next]:
        """A start call no sooner than the email may get a new code: after
        what is left of the last answer's ``resend_after_s`` for this email
        (a start inside that window is turned away or, for recover, answered
        like any other with no email sent: the decoy design). Up to 60 s is
        waited out; longer is :data:`_STOP`."""
        if resend_at is not None and resend_at[0] == (values.get("email") or ""):
            left = resend_at[1] - _now()
            if left > MAX_WAIT_S:
                return _STOP
            if left > 0:
                yield from pause(left)
        return start_call()

    def offer_new_code(flow: SignupFlow) -> Generator[_Effect, Any, _Next]:
        """Ask before a new code; on a yes, sit out the answer's
        ``retry_after_s`` and what is left of the resend window."""
        question = AccountQuestion(field="new_code", label=None, answers=_NEW_CODE_ANSWERS, flow=flow)
        if (yield from asked(question)).answer != "new_code":
            return None
        retry = whole_seconds(flow.get("retry_after_s"))
        if retry is not None and 0 < retry <= MAX_WAIT_S:
            yield from pause(float(retry))
        return (yield from start_when_allowed())

    def ask_code(flow: SignupFlow) -> Generator[_Effect, Any, _Next]:
        """Ask for the code of ``flow``: the next call, ``None`` when the
        person cancelled, or :data:`_STOP`. A new code waits for the resend
        window; when that is over 60 s the person is asked for the code they
        hold again while it still works."""
        while True:
            question = AccountQuestion(field="code", label=labels["code"], answers=_CODE_ANSWERS, flow=flow)
            answer = yield from asked(question)
            if answer.answer == "cancel":
                return None
            if answer.answer == "value":
                return _Submit(flow, str(answer.value).strip())
            following = yield from start_when_allowed()
            if isinstance(following, _Stop) and holds_a_working_code():
                continue
            return following

    def after_waits(flow: SignupFlow, repeat: _Start | _Submit) -> Generator[_Effect, Any, _Next]:
        """``wait_then_retry``: wait ``retry_after_s``, then repeat the call
        that was turned away. After :data:`MAX_AUTO_WAITS` automatic waits in
        a row the person is asked first: the code again (after a code), or
        whether to send a new code (after a start)."""
        if auto_waits >= MAX_AUTO_WAITS:
            if isinstance(repeat, _Submit):
                return (yield from ask_code(held.flow if held is not None else flow))
            question = AccountQuestion(field="new_code", label=None, answers=_NEW_CODE_ANSWERS, flow=flow)
            if (yield from asked(question)).answer != "new_code":
                return None
        yield from pause(float(whole_seconds(flow.get("retry_after_s")) or 0))
        return repeat

    required: tuple[Literal["email", "handle"], ...] = (
        ("email", "handle") if kind == "register" else ("email",)
    )
    for name in required:
        current = values.get(name)
        if isinstance(current, str) and current.strip():
            continue
        if not (yield from ask_field(name, None, None)):
            return RunResult(outcome="cancelled")

    repeat: _Start | _Submit = start_call()
    result: StepResult = yield repeat
    received_at = _now()
    while True:
        resend = whole_seconds(result.flow.get("resend_after_s"))
        if resend is not None:
            resend_at = (values.get("email") or "", received_at + resend)
        if result.message:
            yield _Show(result.message)
        flow = result.flow
        action = result.action
        next_call: _Next = _STOP

        if action == "done":
            return RunResult(
                outcome="done",
                api_key=result.api_key,
                handle=result.handle,
                reachability=result.reachability,
            )

        if action == "ask_code":
            expires_at = _expiry_of(flow, received_at)
            if (
                expires_at is None
                and held is not None
                and held.flow.get("pending_id") == flow.get("pending_id")
            ):
                expires_at = held.expires_at
            held = _Held(flow, received_at, expires_at)
            next_call = yield from ask_code(flow)

        elif action == "ask_param":
            held = None
            param = flow.get("param")
            if param in ("email", "handle"):
                field_name: Literal["email", "handle"] = "email" if param == "email" else "handle"
                if not (yield from ask_field(field_name, flow, result.handles)):
                    return RunResult(outcome="cancelled")
                next_call = yield from start_when_allowed()

        elif action == "offer_new_code":
            held = None
            next_call = yield from offer_new_code(flow)

        elif action == "wait_then_retry":
            next_call = yield from after_waits(flow, repeat)

        elif (
            isinstance(repeat, _Start)
            and held is not None
            and result.http_status == 429
            and holds_a_working_code()
        ):
            # A new code asked for and refused for longer than run waits
            # ("Enter the last code you received, or try again in ..."):
            # while the code the person holds still works, go back to it.
            next_call = yield from ask_code(held.flow)

        if next_call is None:
            return RunResult(outcome="cancelled")
        if isinstance(next_call, _Stop):
            return _stopped(result)
        repeat = next_call
        result = yield repeat
        received_at = _now()


def _run_kind(kind: Any) -> AccountKind:
    if kind == "register":
        return "register"
    if kind == "recover":
        return "recover"
    raise ValueError(f"integration.account.run: unknown kind {kind!r}")


def _labels(overrides: Mapping[str, str] | None) -> AccountLabels:
    chosen = overrides or {}

    def pick(name: str, default: str) -> str:
        value = chosen.get(name)
        return value if isinstance(value, str) else default

    return {
        "email": pick("email", DEFAULT_LABELS["email"]),
        "handle": pick("handle", DEFAULT_LABELS["handle"]),
        "code": pick("code", DEFAULT_LABELS["code"]),
    }


def run(
    *,
    kind: AccountKind,
    ask: Callable[[AccountQuestion], AccountAnswer],
    show: Callable[[str], Any],
    client_identity: AgentChatClientIdentity | None,
    input: Mapping[str, Any] | None = None,
    sleep: Callable[[float], Any] | None = None,
    labels: Mapping[str, str] | None = None,
    api_base: str = DEFAULT_BASE_URL,
    timeout_ms: int | None = None,
    http_client: httpx.Client | None = None,
) -> RunResult:
    """The whole sign-up conversation, for an interactive setup.

    It follows ``next_step``: shows every server ``message`` (through
    ``show``), asks only for what the server names (``param``, or the code),
    and lets the person enter the code, ask for a new code, or cancel. It
    never sends a new code unasked, nor before the server allows one: every
    start call after a send in the same run waits out what is left of that
    send's ``resend_after_s`` (a recover asked too soon is answered like any
    other and sends nothing), and after a yes to a new code it also waits
    out the flow's ``retry_after_s``. It waits out a ``retry_after_s`` of up
    to :data:`MAX_WAIT_S` seconds (the message says how long) and stops on a
    longer one, except that when a new code the person asked for is refused
    (any 429, or a resend wait over 60 s) while the code they hold still
    works (``expires_in_s`` from its answer), it shows the message and asks
    for that code again. Every automatic wait is at least
    :data:`MIN_WAIT_S`; after :data:`MAX_AUTO_WAITS` in a row the person is
    asked before the next (the code again, or "send a new code?"). No
    attempt caps of its own: the server's are the only ones.

    ``input`` is what is known up front (:class:`RegisterInput` /
    :class:`RecoverInput`); a missing ``email`` (or, to register, ``handle``)
    is asked first. ``sleep`` takes seconds (default :func:`time.sleep`).

    Returns ``done`` with ``api_key`` and ``handle`` (save both; show
    ``reachability.text``, when present, as it is),
    ``cancelled``, or ``stopped`` with ``code`` and ``message`` (the message
    has been shown). Raises :class:`~agentchatme.ConnectionError` when the
    server cannot be reached, ``TypeError`` for an answer the question does
    not allow, and whatever ``ask`` / ``show`` / ``sleep`` raise.
    """
    machine = _machine(_run_kind(kind), input, _labels(labels))
    pause: Callable[[float], Any] = sleep if sleep is not None else time.sleep
    calls: dict[str, Any] = {
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
                outcome = start(effect.kind, cast("RegisterInput", effect.input), **calls)
            elif isinstance(effect, _Submit):
                outcome = submit_code(effect.flow, effect.code, **calls)
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


async def _resolved(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


async def run_async(
    *,
    kind: AccountKind,
    ask: Callable[[AccountQuestion], Any],
    show: Callable[[str], Any],
    client_identity: AgentChatClientIdentity | None,
    input: Mapping[str, Any] | None = None,
    sleep: Callable[[float], Any] | None = None,
    labels: Mapping[str, str] | None = None,
    api_base: str = DEFAULT_BASE_URL,
    timeout_ms: int | None = None,
    http_client: httpx.AsyncClient | None = None,
) -> RunResult:
    """:func:`run` on an event loop. ``ask``, ``show`` and ``sleep`` may be
    plain functions or coroutine functions; ``sleep`` defaults to
    :func:`asyncio.sleep`."""
    machine = _machine(_run_kind(kind), input, _labels(labels))
    pause: Callable[[float], Any] = sleep if sleep is not None else asyncio.sleep
    calls: dict[str, Any] = {
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
                outcome = await start_async(
                    effect.kind, cast("RegisterInput", effect.input), **calls
                )
            elif isinstance(effect, _Submit):
                outcome = await submit_code_async(effect.flow, effect.code, **calls)
            elif isinstance(effect, _Ask):
                outcome = await _resolved(ask(effect.question))
            elif isinstance(effect, _Show):
                await _resolved(show(effect.message))
            else:
                await _resolved(pause(effect.seconds))
            effect = machine.send(outcome)
    except StopIteration as end:
        return cast("RunResult", end.value)
    finally:
        machine.close()


__all__ = [
    "DEFAULT_LABELS",
    "MAX_AUTO_WAITS",
    "MAX_WAIT_S",
    "MIN_WAIT_S",
    "AccountAnswer",
    "AccountKind",
    "AccountLabels",
    "AccountQuestion",
    "AnswerKind",
    "CheckKeyResult",
    "RecoverInput",
    "RegisterInput",
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
