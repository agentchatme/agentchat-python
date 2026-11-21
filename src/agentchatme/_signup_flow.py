"""Reading the sign-up flow object out of an answer (contract v1, "Sign-up
flows"). Shared by the client's verify results and
:mod:`agentchatme.integration.account`, which re-exports :func:`flow_of`."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from .errors import AgentChatError
from .types.signup import SIGNUP_FLOW_VERSION, SignupFlow

SIGNUP_FLOW_FIELDS = (
    "version",
    "flow",
    "next_step",
    "status",
    "param",
    "expires_at",
    "expires_in_s",
    "code_length",
    "attempts",
    "max_attempts",
    "resend_after_s",
    "retry_after_s",
    "message",
)
"""The flow fields and ``message`` a 2xx sign-up body carries, by wire name
(the keys the TypeScript SDK copies onto its results)."""

CLAIM_LINK_FIELDS = ("claim_url", "claim_expires_at", "claim_expires_in_s")
"""The claim link a sign-up ``done`` body (register, recover and rotate-key
verify) carries beside the flow fields while the agent is unclaimed, by
wire name. Not part of the flow object: :func:`flow_of` leaves them out."""

_STR_FIELDS = ("status", "pending_id", "param", "expires_at")
_INT_FIELDS = (
    "expires_in_s",
    "code_length",
    "attempts",
    "max_attempts",
    "resend_after_s",
    "retry_after_s",
)


def whole_seconds(value: Any) -> int | None:
    """A whole number >= 0 (``3`` or ``3.0``), else ``None`` (``bool`` is not one)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, float) and not value.is_integer():
        return None
    return int(value) if value >= 0 else None


def _text(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def flow_of(answer: Any) -> SignupFlow | None:
    """The flow object of a sign-up answer: of a 2xx body, of an error body
    (its ``details``), of an :class:`~agentchatme.AgentChatError` the SDK
    raised (its ``details``), or of a verify result
    (:class:`~agentchatme.RegisterVerifyResult`). ``None`` when there is
    none: a server older than the flow contract, or anything else.

    It is a flow only when it has a string ``next_step``. Pure; only the
    contract's fields, each only when it has the contract's type (a
    non-empty string, a whole number >= 0); ``version`` defaults to ``1``
    and ``flow`` to ``""`` when absent.
    """
    source: Any
    if isinstance(answer, AgentChatError):
        source = answer.details
    elif isinstance(answer, Mapping):
        body = cast("Mapping[str, Any]", answer)
        source = body.get("details") if isinstance(body.get("code"), str) else body
    else:
        source = getattr(answer, "_signup_fields", None)
    if not isinstance(source, Mapping):
        return None
    record = cast("Mapping[str, Any]", source)
    next_step = _text(record.get("next_step"))
    if next_step is None:
        return None
    version = record.get("version")
    flow: dict[str, Any] = {
        "version": version
        if isinstance(version, (int, float)) and not isinstance(version, bool)
        else SIGNUP_FLOW_VERSION,
        "flow": _text(record.get("flow")) or "",
        "next_step": next_step,
    }
    for name in _STR_FIELDS:
        text = _text(record.get(name))
        if text is not None:
            flow[name] = text
    for name in _INT_FIELDS:
        number = whole_seconds(record.get(name))
        if number is not None:
            flow[name] = number
    return cast("SignupFlow", flow)
