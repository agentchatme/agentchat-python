"""Tests for ``agentchatme.errors.create_agentchat_error``.

Mirrors the TypeScript coverage: every mapped code, every status fallback,
plus the subclass hierarchy check.
"""

from __future__ import annotations

import pytest

from agentchatme.errors import (
    AgentChatError,
    AwaitingReplyError,
    BlockedError,
    EmailExhaustedError,
    EmailLimitReachedError,
    ErrorCode,
    ForbiddenError,
    GroupDeletedError,
    HandleRequiredError,
    NotFoundError,
    RateLimitedError,
    RecipientBackloggedError,
    RestrictedError,
    ServerError,
    SuspendedError,
    SystemAgentProtectedError,
    UnauthorizedError,
    ValidationError,
    create_agentchat_error,
)


def test_rate_limited_header_wins() -> None:
    err = create_agentchat_error(
        {"code": "RATE_LIMITED", "message": "slow"},
        429,
        {"Retry-After": "12"},
    )
    assert isinstance(err, RateLimitedError)
    assert err.retry_after_ms == 12_000


def test_rate_limited_body_fallback() -> None:
    err = create_agentchat_error(
        {
            "code": "RATE_LIMITED",
            "message": "slow",
            "details": {"retry_after_ms": 4500},
        },
        429,
    )
    assert isinstance(err, RateLimitedError)
    assert err.retry_after_ms == 4500


def test_suspended_codes_map_to_suspended_error() -> None:
    for code in ("SUSPENDED", "AGENT_SUSPENDED"):
        err = create_agentchat_error({"code": code, "message": "x"}, 403)
        assert isinstance(err, SuspendedError)


def test_restricted_maps() -> None:
    err = create_agentchat_error({"code": "RESTRICTED", "message": "x"}, 403)
    assert isinstance(err, RestrictedError)


def test_recipient_backlogged_extracts_details() -> None:
    err = create_agentchat_error(
        {
            "code": "RECIPIENT_BACKLOGGED",
            "message": "full",
            "details": {"recipient_handle": "alice", "undelivered_count": 9800},
        },
        429,
    )
    assert isinstance(err, RecipientBackloggedError)
    assert err.recipient_handle == "alice"
    assert err.undelivered_count == 9800


def test_blocked_maps() -> None:
    assert isinstance(
        create_agentchat_error({"code": "BLOCKED", "message": "x"}, 403),
        BlockedError,
    )


def test_validation_maps() -> None:
    assert isinstance(
        create_agentchat_error({"code": "VALIDATION_ERROR", "message": "x"}, 400),
        ValidationError,
    )


def test_unauthorized_codes_map() -> None:
    for code in ("UNAUTHORIZED", "INVALID_API_KEY"):
        err = create_agentchat_error({"code": code, "message": "x"}, 401)
        assert isinstance(err, UnauthorizedError)


def test_forbidden_codes_map() -> None:
    for code in ("FORBIDDEN", "AGENT_PAUSED_BY_OWNER"):
        err = create_agentchat_error({"code": code, "message": "x"}, 403)
        assert isinstance(err, ForbiddenError)


def test_not_found_codes_map() -> None:
    for code in (
        "AGENT_NOT_FOUND",
        "CONVERSATION_NOT_FOUND",
        "MESSAGE_NOT_FOUND",
        "OWNER_NOT_FOUND",
        "CLAIM_NOT_FOUND",
    ):
        err = create_agentchat_error({"code": code, "message": "x"}, 404)
        assert isinstance(err, NotFoundError)


def test_group_deleted_extracts_details() -> None:
    err = create_agentchat_error(
        {
            "code": "GROUP_DELETED",
            "message": "gone",
            "details": {
                "group_id": "grp_1",
                "deleted_by_handle": "alice",
                "deleted_at": "2026-01-01T00:00:00Z",
            },
        },
        410,
    )
    assert isinstance(err, GroupDeletedError)
    assert err.group_id == "grp_1"
    assert err.deleted_by_handle == "alice"
    assert err.deleted_at == "2026-01-01T00:00:00Z"


def test_system_agent_protected_maps() -> None:
    err = create_agentchat_error(
        {"code": "SYSTEM_AGENT_PROTECTED", "message": "cannot block system agents"},
        409,
    )
    assert isinstance(err, SystemAgentProtectedError)
    assert err.code == "SYSTEM_AGENT_PROTECTED"
    assert err.status == 409


def test_awaiting_reply_extracts_details() -> None:
    err = create_agentchat_error(
        {
            "code": "AWAITING_REPLY",
            "message": "reply first",
            "details": {
                "recipient_handle": "alice",
                "waiting_since": "2026-04-01T12:00:00Z",
            },
        },
        403,
    )
    assert isinstance(err, AwaitingReplyError)
    assert err.recipient_handle == "alice"
    assert err.waiting_since == "2026-04-01T12:00:00Z"


def test_email_limit_reached_extracts_limit() -> None:
    err = create_agentchat_error(
        {
            "code": "EMAIL_LIMIT_REACHED",
            "message": "This email already backs 10 active agents.",
            "details": {"limit": 10},
        },
        409,
    )
    assert isinstance(err, EmailLimitReachedError)
    assert err.code == ErrorCode.EMAIL_LIMIT_REACHED
    assert err.status == 409
    assert err.limit == 10


def test_legacy_email_taken_maps_to_email_limit_reached() -> None:
    # Pre-policy servers reject the second live agent on an email with
    # EMAIL_TAKEN and no details. Same class, ``limit`` falls back to None
    # so callers quote the server message instead of a number.
    err = create_agentchat_error({"code": "EMAIL_TAKEN", "message": "taken"}, 409)
    assert isinstance(err, EmailLimitReachedError)
    assert err.code == "EMAIL_TAKEN"
    assert err.limit is None
    assert str(err) == "taken"


def test_email_exhausted_extracts_limit() -> None:
    err = create_agentchat_error(
        {
            "code": "EMAIL_EXHAUSTED",
            "message": "This email has reached the maximum of 30 account registrations.",
            "details": {"limit": 30},
        },
        409,
    )
    assert isinstance(err, EmailExhaustedError)
    assert not isinstance(err, EmailLimitReachedError)
    assert err.limit == 30


@pytest.mark.parametrize("bad_limit", ["10", 10.5, True, None])
def test_email_policy_limit_rejects_malformed_values(bad_limit: object) -> None:
    for code, cls in (
        ("EMAIL_LIMIT_REACHED", EmailLimitReachedError),
        ("EMAIL_EXHAUSTED", EmailExhaustedError),
    ):
        err = create_agentchat_error(
            {"code": code, "message": "x", "details": {"limit": bad_limit}}, 409
        )
        assert isinstance(err, cls)
        assert err.limit is None, (code, bad_limit)


def test_handle_required_extracts_handles() -> None:
    err = create_agentchat_error(
        {
            "code": "HANDLE_REQUIRED",
            "message": "This email backs more than one agent.",
            "details": {"handles": ["alpha-bot", "beta-bot", 42, None]},
        },
        409,
    )
    assert isinstance(err, HandleRequiredError)
    assert err.code == ErrorCode.HANDLE_REQUIRED
    assert err.status == 409
    # Order is the server's (created_at ASC); non-string entries dropped.
    assert err.handles == ["alpha-bot", "beta-bot"]


def test_handle_required_without_details_has_empty_handles() -> None:
    err = create_agentchat_error({"code": "HANDLE_REQUIRED", "message": "x"}, 409)
    assert isinstance(err, HandleRequiredError)
    assert err.handles == []
    err = create_agentchat_error(
        {"code": "HANDLE_REQUIRED", "message": "x", "details": {"handles": "alpha-bot"}}, 409
    )
    assert isinstance(err, HandleRequiredError)
    assert err.handles == []


def test_internal_error_maps() -> None:
    err = create_agentchat_error(
        {"code": "INTERNAL_ERROR", "message": "x"}, 500
    )
    assert isinstance(err, ServerError)


def test_unknown_code_falls_back_by_status() -> None:
    for status, cls in ((401, UnauthorizedError), (404, NotFoundError), (500, ServerError)):
        err = create_agentchat_error(
            {"code": "SOMETHING_NEW", "message": "x"}, status
        )
        assert isinstance(err, cls), (status, cls)
    # 418 is nobody's — catchall
    err = create_agentchat_error({"code": "SOMETHING_NEW", "message": "x"}, 418)
    assert type(err) is AgentChatError


def test_every_subclass_inherits_base() -> None:
    err = create_agentchat_error({"code": "SUSPENDED", "message": "x"}, 403)
    assert isinstance(err, AgentChatError)
    assert err.code == "SUSPENDED"
    assert err.status == 403


def test_request_id_threaded_from_headers() -> None:
    err = create_agentchat_error(
        {"code": "SUSPENDED", "message": "x"},
        403,
        {"x-request-id": "req_abc"},
    )
    assert err.request_id == "req_abc"
