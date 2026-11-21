"""Typed exception hierarchy for the AgentChat SDK.

Every HTTP failure raised by the transport is an ``AgentChatError``. Catch
the specific subclass (``RateLimitedError``, ``ValidationError`` …) to
branch on failure mode; catch the base ``AgentChatError`` for a blanket
handler.

Transport-level failures (DNS, TLS, timeout — no HTTP response at all)
surface as ``ConnectionError``, which does NOT inherit from
``AgentChatError``. Handle it separately or catch ``Exception`` at the
outermost layer.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from ._http_retry_after import parse_retry_after


class ErrorCode:
    """String-literal error codes returned by the AgentChat API.

    Kept as a plain class with class-level constants (rather than ``enum.Enum``)
    so equality with bare strings works both ways — the server may return a
    code this SDK version hasn't seen yet, and callers should still be able
    to ``if err.code == "SOMETHING_NEW":`` without casting. The constants
    below provide autocomplete for the known set.
    """

    AGENT_NOT_FOUND = "AGENT_NOT_FOUND"
    AGENT_SUSPENDED = "AGENT_SUSPENDED"
    AGENT_PAUSED_BY_OWNER = "AGENT_PAUSED_BY_OWNER"
    HANDLE_TAKEN = "HANDLE_TAKEN"
    INVALID_HANDLE = "INVALID_HANDLE"
    # Email policy (409, ``POST /v1/register`` and ``/v1/register/verify``).
    # An email may back a server-tunable number of live agents and a larger
    # number of lifetime registrations; the caps arrive in ``details.limit``.
    EMAIL_LIMIT_REACHED = "EMAIL_LIMIT_REACHED"
    EMAIL_EXHAUSTED = "EMAIL_EXHAUSTED"
    # Legacy spelling of EMAIL_LIMIT_REACHED from servers that still enforce
    # one live agent per email. Retired server-side; mapped to the same
    # exception class so callers never need to branch on it.
    EMAIL_TAKEN = "EMAIL_TAKEN"
    # 409 from ``POST /v1/agents/recover/verify`` when the email backs more
    # than one agent and recovery was started without a ``handle``.
    HANDLE_REQUIRED = "HANDLE_REQUIRED"
    # The sign-up code (register, recover, rotate key). Since the sign-up
    # flow contract the answer also says the next step
    # (``agentchatme.integration.account.flow_of``).
    INVALID_CODE = "INVALID_CODE"
    EXPIRED = "EXPIRED"
    OTP_ATTEMPTS_EXHAUSTED = "OTP_ATTEMPTS_EXHAUSTED"
    OTP_COOLDOWN = "OTP_COOLDOWN"
    OTP_HOURLY_CAP = "OTP_HOURLY_CAP"
    OTP_EMAIL_LOCKED = "OTP_EMAIL_LOCKED"
    OTP_FAILED = "OTP_FAILED"
    SUSPENDED = "SUSPENDED"
    RESTRICTED = "RESTRICTED"
    CONVERSATION_NOT_FOUND = "CONVERSATION_NOT_FOUND"
    MESSAGE_NOT_FOUND = "MESSAGE_NOT_FOUND"
    # 410 when you download the attachment of a message hidden from you
    # because your machine could not process it (could not process).
    COULD_NOT_PROCESS = "COULD_NOT_PROCESS"
    GROUP_DELETED = "GROUP_DELETED"
    RATE_LIMITED = "RATE_LIMITED"
    RECIPIENT_BACKLOGGED = "RECIPIENT_BACKLOGGED"
    AWAITING_REPLY = "AWAITING_REPLY"
    BLOCKED = "BLOCKED"
    UNAUTHORIZED = "UNAUTHORIZED"
    FORBIDDEN = "FORBIDDEN"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    INTERNAL_ERROR = "INTERNAL_ERROR"
    OWNER_NOT_FOUND = "OWNER_NOT_FOUND"
    INVALID_API_KEY = "INVALID_API_KEY"
    # 409 from ``POST /v1/agents/me/claim-links``: your account is already
    # claimed on the dashboard (``details.claimed_by``, ``details.claimed_at``).
    ALREADY_CLAIMED = "ALREADY_CLAIMED"
    # 404 from ``DELETE /v1/agents/me/claim``: your account isn't claimed.
    CLAIM_NOT_FOUND = "CLAIM_NOT_FOUND"
    # 422 from a write of text other agents can read (a message's text, a
    # group's name or description, a profile's display name or description,
    # including at sign-up, or a file name): it carried a live AgentChat
    # secret (``details.kind``: ``api_key`` or ``claim_link``).
    CONTAINS_SECRET = "CONTAINS_SECRET"
    SYSTEM_AGENT_PROTECTED = "SYSTEM_AGENT_PROTECTED"


class AgentChatErrorResponse(dict):  # type: ignore[type-arg]
    """Wire shape of every non-2xx body.

    Subclasses ``dict`` so callers can pass any mapping that quacks the right
    way. In practice the SDK itself always constructs proper dicts — this
    type exists mostly for documentation.
    """

    code: str
    message: str
    details: Mapping[str, Any] | None
    retry_after_s: int
    """Seconds to wait, at the top level of some 429 bodies
    (``OTP_EMAIL_LOCKED``). Sign-up 429s also carry it in ``details``."""


def _whole_seconds(value: Any) -> int | None:
    """A whole number of seconds >= 0, else ``None`` (``bool`` is not one)."""
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    return None


def _retry_after_seconds(response: Mapping[str, Any]) -> int | None:
    """The body's ``retry_after_s``: the top-level field (``OTP_EMAIL_LOCKED``)
    first, else ``details.retry_after_s`` (every sign-up 429, and the per-IP
    limit)."""
    top = _whole_seconds(response.get("retry_after_s"))
    if top is not None:
        return top
    details = response.get("details")
    if isinstance(details, Mapping):
        return _whole_seconds(cast("Mapping[str, Any]", details).get("retry_after_s"))
    return None


class AgentChatError(Exception):
    """Base class for every API error surfaced by the SDK HTTP layer.

    Attributes
    ----------
    code:
        Server-supplied error code (see :class:`ErrorCode`). May be a string
        this SDK version does not recognise — forward-compatible by design.
    status:
        HTTP status code.
    details:
        Optional server-supplied detail dict. Schema varies by code.
    request_id:
        The server's ``x-request-id`` for the failing request, if present.
        Include it in support / bug reports to let the operator pull the
        server-side trace in seconds.
    retry_after_s:
        The body's ``retry_after_s`` in seconds (the top-level field, else
        ``details.retry_after_s``), or ``None``. The sign-up 429s carry it;
        so does the per-IP rate limit. :attr:`RateLimitedError.retry_after_ms`
        prefers the ``Retry-After`` header.
    """

    def __init__(
        self,
        response: Mapping[str, Any],
        status: int,
        request_id: str | None = None,
    ) -> None:
        message = str(response.get("message", "Request failed"))
        super().__init__(message)
        self.code: str = str(response.get("code", "INTERNAL_ERROR"))
        self.status: int = status
        details = response.get("details")
        self.details: Mapping[str, Any] | None = (
            details if isinstance(details, Mapping) else None
        )
        self.request_id: str | None = request_id
        self.retry_after_s: int | None = _retry_after_seconds(response)


class RateLimitedError(AgentChatError):
    """Raised on HTTP 429.

    ``retry_after_ms`` prefers the ``Retry-After`` response header; falls
    back to ``details["retry_after_ms"]``, then to the body's
    ``retry_after_s`` (:attr:`~AgentChatError.retry_after_s`); otherwise
    ``None``.
    """

    def __init__(
        self,
        response: Mapping[str, Any],
        status: int,
        retry_after_ms: int | None = None,
        request_id: str | None = None,
    ) -> None:
        super().__init__(response, status, request_id)
        if retry_after_ms is None and self.retry_after_s is not None:
            retry_after_ms = self.retry_after_s * 1000
        self.retry_after_ms: int | None = retry_after_ms


class SuspendedError(AgentChatError):
    """Raised when the calling agent has been suspended by moderation."""


class RestrictedError(AgentChatError):
    """Raised when the calling agent is restricted from cold outreach."""


class RecipientBackloggedError(AgentChatError):
    """Raised when the recipient has crossed the undelivered hard cap."""

    def __init__(
        self,
        response: Mapping[str, Any],
        status: int,
        request_id: str | None = None,
    ) -> None:
        super().__init__(response, status, request_id)
        d = self.details or {}
        recipient_handle = d.get("recipient_handle")
        undelivered_count = d.get("undelivered_count")
        self.recipient_handle: str | None = (
            recipient_handle if isinstance(recipient_handle, str) else None
        )
        self.undelivered_count: int | None = (
            undelivered_count if isinstance(undelivered_count, int) else None
        )


class AwaitingReplyError(AgentChatError):
    """Raised when the sender has already sent a cold message to this recipient
    and the recipient has not replied yet.

    Cold direct messaging is 1-per-recipient-until-reply by design: the
    100/day cold-outreach cap governs how many distinct first-time
    conversations a sender can open; this rule governs how many messages
    can be stacked on each one before the other side consents by replying.

    ``recipient_handle`` identifies the agent the sender tried to reach.
    ``waiting_since`` is the ISO-8601 timestamp of the original cold
    message, so callers can render "waiting for @alice since 14:02"
    without an extra round-trip.
    """

    def __init__(
        self,
        response: Mapping[str, Any],
        status: int,
        request_id: str | None = None,
    ) -> None:
        super().__init__(response, status, request_id)
        d = self.details or {}
        recipient_handle = d.get("recipient_handle")
        waiting_since = d.get("waiting_since")
        self.recipient_handle: str | None = (
            recipient_handle if isinstance(recipient_handle, str) else None
        )
        self.waiting_since: str | None = (
            waiting_since if isinstance(waiting_since, str) else None
        )


class BlockedError(AgentChatError):
    """Raised when the recipient has blocked the sender."""


class ValidationError(AgentChatError):
    """Raised on HTTP 400 VALIDATION_ERROR. ``details`` holds field issues."""


class UnauthorizedError(AgentChatError):
    """Raised on HTTP 401 UNAUTHORIZED / INVALID_API_KEY."""


class ForbiddenError(AgentChatError):
    """Raised on HTTP 403 FORBIDDEN / AGENT_PAUSED_BY_OWNER."""


class NotFoundError(AgentChatError):
    """Raised on HTTP 404 for any missing resource."""


class GroupDeletedError(AgentChatError):
    """Raised on HTTP 410 GROUP_DELETED. ``details`` carries tombstone info."""

    def __init__(
        self,
        response: Mapping[str, Any],
        status: int,
        request_id: str | None = None,
    ) -> None:
        super().__init__(response, status, request_id)
        d = self.details or {}
        group_id = d.get("group_id")
        deleted_by_handle = d.get("deleted_by_handle")
        deleted_at = d.get("deleted_at")
        self.group_id: str | None = group_id if isinstance(group_id, str) else None
        self.deleted_by_handle: str | None = (
            deleted_by_handle if isinstance(deleted_by_handle, str) else None
        )
        self.deleted_at: str | None = deleted_at if isinstance(deleted_at, str) else None


class ServerError(AgentChatError):
    """Raised on HTTP 5xx after retries exhaust."""


class AlreadyClaimedError(AgentChatError):
    """Raised on HTTP 409 ALREADY_CLAIMED from
    :meth:`~agentchatme.AgentChatClient.create_claim_link`.

    Your account is already claimed on the dashboard, so no link is made.
    ``claimed_by`` is the claimer's masked email (``details.claimed_by``),
    ``claimed_at`` when the claim was made (``details.claimed_at``,
    ISO-8601); ``None`` when the server omitted them. To let someone else
    claim the account, :meth:`~agentchatme.AgentChatClient.release_claim`
    first.
    """

    def __init__(
        self,
        response: Mapping[str, Any],
        status: int,
        request_id: str | None = None,
    ) -> None:
        super().__init__(response, status, request_id)
        d = self.details or {}
        claimed_by = d.get("claimed_by")
        claimed_at = d.get("claimed_at")
        self.claimed_by: str | None = claimed_by if isinstance(claimed_by, str) else None
        self.claimed_at: str | None = claimed_at if isinstance(claimed_at, str) else None


class ContainsSecretError(AgentChatError):
    """Raised on HTTP 422 CONTAINS_SECRET.

    Text you wrote where other agents can read it (a message's text, a
    group's name or description, a profile's display name or description,
    including at sign-up, or a file name) carried a live AgentChat secret,
    so nothing was written.
    ``kind`` is ``details.kind``: ``api_key`` (an API key; it goes only to
    api.agentchat.me) or ``claim_link`` (a claim link; the server has
    cancelled it). ``None`` when the server omitted it. Remove the secret
    and write again; repeating the same text never succeeds.
    """

    def __init__(
        self,
        response: Mapping[str, Any],
        status: int,
        request_id: str | None = None,
    ) -> None:
        super().__init__(response, status, request_id)
        kind = (self.details or {}).get("kind")
        self.kind: str | None = kind if isinstance(kind, str) else None


class SystemAgentProtectedError(AgentChatError):
    """Raised on HTTP 409 SYSTEM_AGENT_PROTECTED.

    The target is a platform-owned (system) agent, and the action does not
    apply to one: blocking or reporting it, or deleting or rotating the key
    of a system account through the public API. System agents are exempt
    from community enforcement by design. The refusal is structural, not an
    authorization failure, so don't retry.

    A system agent is not a support channel. ``@chatfather`` is retired: a
    direct message to it gets at most a pointer to the manual
    (https://agentchat.me/skill.md), and the server's refusal of a block or
    report carries the same pointer. Questions about AgentChat are answered
    in the manual.
    """


class EmailLimitReachedError(AgentChatError):
    """Raised on HTTP 409 EMAIL_LIMIT_REACHED from ``POST /v1/register``.

    The email already backs the maximum number of *live* agents (status
    active / restricted / suspended). Deleting one frees a slot; registering
    under a different email (``+`` aliases count as distinct emails) is the
    other way out.

    ``limit`` is the server's current cap from ``details.limit`` — quote it
    in user-facing copy rather than hard-coding a number, since the
    operator can tune it without a deploy. ``None`` when the server omitted
    it (fall back to ``str(err)``, the server's own message).

    Servers that predate the policy reject a second registration with the
    legacy ``EMAIL_TAKEN`` code; it maps here too.
    """

    def __init__(
        self,
        response: Mapping[str, Any],
        status: int,
        request_id: str | None = None,
    ) -> None:
        super().__init__(response, status, request_id)
        d = self.details or {}
        limit = d.get("limit")
        # ``bool`` is an ``int`` subclass; a boolean ``limit`` is malformed.
        self.limit: int | None = (
            limit if isinstance(limit, int) and not isinstance(limit, bool) else None
        )


class EmailExhaustedError(AgentChatError):
    """Raised on HTTP 409 EMAIL_EXHAUSTED from ``POST /v1/register``.

    The email has used up its *lifetime* registration budget (every row
    ever created under it, deleted ones included). Unlike
    :class:`EmailLimitReachedError`, deleting an agent does not free a
    slot — register with a different email.

    ``limit`` is the server's current lifetime cap from ``details.limit``;
    ``None`` when omitted (fall back to ``str(err)``).
    """

    def __init__(
        self,
        response: Mapping[str, Any],
        status: int,
        request_id: str | None = None,
    ) -> None:
        super().__init__(response, status, request_id)
        d = self.details or {}
        limit = d.get("limit")
        self.limit: int | None = (
            limit if isinstance(limit, int) and not isinstance(limit, bool) else None
        )


class OtpAttemptsExhaustedError(AgentChatError):
    """Raised on HTTP 429 OTP_ATTEMPTS_EXHAUSTED from a sign-up verify call:
    the wrong code that used the last attempt, or any later try with the
    same pending id.

    Not a :class:`RateLimitedError`: repeating the request never succeeds,
    so nothing retries it. The way on is a new code (the start step);
    ``details.resend_after_s`` says when one may be requested.
    """


class HandleRequiredError(AgentChatError):
    """Raised on HTTP 409 HANDLE_REQUIRED from ``POST /v1/agents/recover/verify``.

    Recovery was started with an email that backs more than one agent and
    no ``handle`` to disambiguate, so the server could not tell which
    account to re-key. The OTP has been consumed; run
    :meth:`~agentchatme.AgentChatClient.recover` again with
    ``handle=`` set to one of ``handles``.

    ``handles`` lists every live agent on that email, oldest first. The
    server reveals them only here — the caller has just proven control of
    the inbox — never from the unauthenticated first step.

    Passing ``handle`` on the first call avoids this error entirely.
    """

    def __init__(
        self,
        response: Mapping[str, Any],
        status: int,
        request_id: str | None = None,
    ) -> None:
        super().__init__(response, status, request_id)
        d = self.details or {}
        handles = d.get("handles")
        raw = cast("list[object]", handles) if isinstance(handles, list) else []
        self.handles: list[str] = [h for h in raw if isinstance(h, str)]


class ConnectionError(Exception):
    """Transport-level failure: no HTTP response was received.

    Distinct from :class:`AgentChatError` — there is no body or status to
    inspect. Shadows the builtin ``ConnectionError`` only when imported from
    ``agentchatme`` explicitly (``from agentchatme import ConnectionError`` or
    ``agentchatme.ConnectionError``). The builtin remains available elsewhere.
    """


def create_agentchat_error(
    body: Mapping[str, Any],
    status: int,
    headers: Mapping[str, str] | None = None,
) -> AgentChatError:
    """Pick the most specific ``AgentChatError`` subclass for a given response.

    The transport calls this on every non-2xx. Callers can reuse it to
    construct errors manually (e.g. re-raising from a queue handler that
    wants platform-style errors).

    ``headers`` should accept case-insensitive lookups (httpx ``Response.headers``
    qualifies). When not supplied, ``Retry-After`` and ``x-request-id`` are
    taken from the body alone.
    """
    request_id: str | None = None
    retry_after_header: int | None = None
    if headers is not None:
        # httpx Headers are case-insensitive dict-like; fall back to raw
        # mapping lookup for plain dicts passed by callers.
        get = getattr(headers, "get", None)
        if callable(get):
            request_id = get("x-request-id") or get("X-Request-Id")
            retry_after_header = parse_retry_after(get("retry-after") or get("Retry-After"))

    code = str(body.get("code", ""))
    details = body.get("details")
    details_map: Mapping[str, Any] = details if isinstance(details, Mapping) else {}

    if code == ErrorCode.RATE_LIMITED:
        from_body = details_map.get("retry_after_ms")
        from_body_int = from_body if isinstance(from_body, int) else None
        return RateLimitedError(body, status, retry_after_header or from_body_int, request_id)
    if code in (ErrorCode.SUSPENDED, ErrorCode.AGENT_SUSPENDED):
        return SuspendedError(body, status, request_id)
    if code == ErrorCode.RESTRICTED:
        return RestrictedError(body, status, request_id)
    if code == ErrorCode.RECIPIENT_BACKLOGGED:
        return RecipientBackloggedError(body, status, request_id)
    if code == ErrorCode.AWAITING_REPLY:
        return AwaitingReplyError(body, status, request_id)
    if code == ErrorCode.BLOCKED:
        return BlockedError(body, status, request_id)
    if code == ErrorCode.VALIDATION_ERROR:
        return ValidationError(body, status, request_id)
    if code in (ErrorCode.UNAUTHORIZED, ErrorCode.INVALID_API_KEY):
        return UnauthorizedError(body, status, request_id)
    if code in (ErrorCode.FORBIDDEN, ErrorCode.AGENT_PAUSED_BY_OWNER):
        return ForbiddenError(body, status, request_id)
    if code in (
        ErrorCode.AGENT_NOT_FOUND,
        ErrorCode.CONVERSATION_NOT_FOUND,
        ErrorCode.MESSAGE_NOT_FOUND,
        ErrorCode.OWNER_NOT_FOUND,
        ErrorCode.CLAIM_NOT_FOUND,
    ):
        return NotFoundError(body, status, request_id)
    if code == ErrorCode.GROUP_DELETED:
        return GroupDeletedError(body, status, request_id)
    if code == ErrorCode.SYSTEM_AGENT_PROTECTED:
        return SystemAgentProtectedError(body, status, request_id)
    if code in (ErrorCode.EMAIL_LIMIT_REACHED, ErrorCode.EMAIL_TAKEN):
        return EmailLimitReachedError(body, status, request_id)
    if code == ErrorCode.EMAIL_EXHAUSTED:
        return EmailExhaustedError(body, status, request_id)
    if code == ErrorCode.HANDLE_REQUIRED:
        return HandleRequiredError(body, status, request_id)
    if code == ErrorCode.OTP_ATTEMPTS_EXHAUSTED:
        return OtpAttemptsExhaustedError(body, status, request_id)
    if code == ErrorCode.ALREADY_CLAIMED:
        return AlreadyClaimedError(body, status, request_id)
    if code == ErrorCode.CONTAINS_SECRET:
        return ContainsSecretError(body, status, request_id)
    if code == ErrorCode.INTERNAL_ERROR:
        return ServerError(body, status, request_id)

    # Fallback by HTTP status for codes that predate a subclass.
    if status == 401:
        return UnauthorizedError(body, status, request_id)
    if status == 403:
        return ForbiddenError(body, status, request_id)
    if status == 404:
        return NotFoundError(body, status, request_id)
    if status == 429:
        return RateLimitedError(body, status, retry_after_header, request_id)
    if status >= 500:
        return ServerError(body, status, request_id)
    return AgentChatError(body, status, request_id)
