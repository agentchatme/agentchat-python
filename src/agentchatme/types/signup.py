"""The sign-up flow object (contract v1, "Sign-up flows"; additive).

Every answer of the six sign-up endpoints (``POST /v1/register`` and
``/verify``, ``POST /v1/agents/recover`` and ``/verify``,
``POST /v1/agents/{handle}/rotate-key`` and ``/verify``) carries it: a 2xx
body at its top level, beside the existing fields; an error body inside
``details``, beside the existing ``details.*``. A client reads it with
``flow = body["details"] if body.get("code") else body``
(:func:`agentchatme.integration.account.flow_of`), ignores fields it does
not know, branches only on ``code`` / ``next_step``, and shows the answer's
``message`` verbatim.

The flow is plain JSON data (a :class:`typing.TypedDict`), so a command
line that does one step per run can save it between runs with ``json``.
String fields are typed ``str``: a value a later server adds still parses,
and an unknown ``next_step`` means "stop and show the message". The
``Literal`` aliases document the values this release knows.
"""

from __future__ import annotations

from typing import Literal, TypedDict

SIGNUP_FLOW_VERSION = 1
"""The flow object's shape version. Within v1, changes are additive."""

SignupFlowKind = Literal["register", "recover", "rotate_key"]
"""Which family of calls the answer belongs to."""

SignupNextStep = Literal[
    "register",
    "verify",
    "recover",
    "recover_verify",
    "rotate_key",
    "rotate_key_verify",
    "done",
    "connect",
]
"""The call to make next, named after the endpoint (and SDK method):
``register`` / ``recover`` / ``rotate_key`` start a flow (and send a code),
``verify`` / ``recover_verify`` / ``rotate_key_verify`` submit the code,
``done`` means the API key is in the body. ``connect`` (a sign-up with
``finish="connector"``): add the connector at ``connect.mcp_url``; the
sign-up finishes on AgentChat's page when your person approves it, and no
answer carries a key. A 429 keeps the step it applies to and adds
``retry_after_s``."""

SignupStartStep = Literal["register", "recover", "rotate_key"]
"""The steps that start a flow and send a code."""

SignupVerifyStep = Literal["verify", "recover_verify", "rotate_key_verify"]
"""The steps that submit a code."""

SignupCodeStatus = Literal["pending", "approved", "expired", "max_attempts_reached"]
"""The state of the pending code the answer concerns (Twilio Verify's statuses)."""

SignupParam = Literal["email", "handle", "code"]
"""The one input that must change before the next call."""


class _SignupFlowRequired(TypedDict):
    version: int
    """Shape version: ``1``."""
    flow: str
    """A :data:`SignupFlowKind`."""
    next_step: str
    """A :data:`SignupNextStep`."""


class SignupFlow(_SignupFlowRequired, total=False):
    """The flow object. Only ``version``, ``flow`` and ``next_step`` are
    always present; the rest appear as the contract table says."""

    status: str
    """A :data:`SignupCodeStatus`: present whenever the answer concerns a
    pending code."""
    pending_id: str
    """While the code is alive (``status: "pending"``): echo it to the verify call."""
    param: str
    """A :data:`SignupParam`: the input to ask for again before the next call."""
    expires_at: str
    """While pending: when the code stops working (ISO-8601 UTC)."""
    expires_in_s: int
    """While pending: seconds until ``expires_at``."""
    code_length: int
    """While pending: the code's number of digits (6)."""
    attempts: int
    """Wrong codes so far for this pending id: while pending, and on the
    answer that ends it."""
    max_attempts: int
    """With ``attempts``: how many wrong codes end the pending id."""
    resend_after_s: int
    """While pending: seconds until a new code may be requested for this
    email (``0``: now)."""
    retry_after_s: int
    """On every 429 (with the ``Retry-After`` header): seconds to wait
    before ``next_step``."""


__all__ = [
    "SIGNUP_FLOW_VERSION",
    "SignupCodeStatus",
    "SignupFlow",
    "SignupFlowKind",
    "SignupNextStep",
    "SignupParam",
    "SignupStartStep",
    "SignupVerifyStep",
]
