"""Reading the sign-up flow object out of an answer (contract v1, "Sign-up
flows"). Shared by the client's verify results and
:mod:`agentchatme.integration.account`, which re-exports :func:`flow_of`."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from .errors import AgentChatError
from .types.signup import SignupFlow

_FLOW_STR_FIELDS = ("status", "pending_id", "param", "expires_at")
_FLOW_INT_FIELDS = (
    "expires_in_s",
    "code_length",
    "attempts",
    "max_attempts",
    "resend_after_s",
    "retry_after_s",
)


def whole_seconds(value: Any) -> int | None:
    """A whole number of seconds >= 0, else ``None`` (``bool`` is not one)."""
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    return None


def flow_of(answer: Any) -> SignupFlow | None:
    """The flow object of a sign-up answer, or ``None`` when it has none.

    ``answer`` is a 2xx body (a mapping), an error body, or an
    :class:`~agentchatme.AgentChatError` the SDK raised. The flow is the
    error's ``details``, or the body itself on a 2xx
    (``body["details"] if body.get("code") else body``). Only the fields of
    the contract are kept, and only well-typed ones; ``version``, ``flow``
    and ``next_step`` must be there. Pure.
    """
    raw: Any
    if isinstance(answer, AgentChatError):
        raw = answer.details
    elif isinstance(answer, Mapping):
        body = cast("Mapping[str, Any]", answer)
        raw = body.get("details") if body.get("code") else body
    else:
        return None
    if not isinstance(raw, Mapping):
        return None
    source = cast("Mapping[str, Any]", raw)
    version = source.get("version")
    kind = source.get("flow")
    next_step = source.get("next_step")
    if (
        not isinstance(version, int)
        or isinstance(version, bool)
        or not isinstance(kind, str)
        or not isinstance(next_step, str)
        or not next_step
    ):
        return None
    flow: dict[str, Any] = {"version": version, "flow": kind, "next_step": next_step}
    for name in _FLOW_STR_FIELDS:
        value = source.get(name)
        if isinstance(value, str):
            flow[name] = value
    for name in _FLOW_INT_FIELDS:
        number = whole_seconds(source.get(name))
        if number is not None:
            flow[name] = number
    return cast("SignupFlow", flow)
