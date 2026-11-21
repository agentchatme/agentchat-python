"""Regression tests for capabilities intentionally omitted from the public SDK."""

import inspect

import pytest

import agentchatme
import agentchatme.types as public_types


def test_internal_webhooks_are_not_publicly_exposed() -> None:
    assert not hasattr(agentchatme, "verify_webhook")
    assert not hasattr(agentchatme, "VerifyWebhookOptions")
    assert not hasattr(agentchatme, "WebhookVerificationError")

    for method in (
        "create_webhook",
        "list_webhooks",
        "get_webhook",
        "delete_webhook",
    ):
        assert not hasattr(agentchatme.AgentChatClient, method)
        assert not hasattr(agentchatme.AsyncAgentChatClient, method)

    for type_name in (
        "CreateWebhookRequest",
        "WebhookConfig",
        "WebhookEvent",
        "WebhookPayload",
    ):
        assert not hasattr(public_types, type_name)


def test_a_send_cannot_carry_the_system_type() -> None:
    # The server refuses type="system" on POST /v1/messages (400
    # VALIDATION_ERROR): system messages are the platform's own.
    from typing import get_args

    from pydantic import ValidationError

    assert set(get_args(public_types.SendableMessageType)) == {"text", "structured", "file"}
    assert set(get_args(public_types.MessageType)) == {"text", "structured", "file", "system"}  # received
    for sendable in ("text", "structured", "file"):
        request = public_types.SendMessageRequest(client_msg_id="c", content={"text": "x"}, to="@a", type=sendable)
        assert request.type == sendable
    with pytest.raises(ValidationError):
        public_types.SendMessageRequest(client_msg_id="c", content={"text": "x"}, to="@a", type="system")  # pyright: ignore[reportArgumentType]


def test_the_wake_check_sits_beside_the_wake_webhook() -> None:
    with agentchatme.AgentChatClient(api_key="ac_test", base_url="https://api.test") as client:
        assert callable(client.wake_check.set)
        assert callable(client.wake_check.clear)
    assert public_types.WakeCheckStatus.__name__ == "WakeCheckStatus"
    assert public_types.ClearWakeCheckResult.__name__ == "ClearWakeCheckResult"
    assert agentchatme.ErrorCode.WAKE_DESTINATION_EXISTS == "WAKE_DESTINATION_EXISTS"
    assert agentchatme.ErrorCode.WAKE_CHECK_UNAVAILABLE == "WAKE_CHECK_UNAVAILABLE"


@pytest.mark.asyncio
async def test_the_async_client_has_the_wake_check_too() -> None:
    async with agentchatme.AsyncAgentChatClient(api_key="ac_test", base_url="https://api.test") as client:
        assert inspect.iscoroutinefunction(client.wake_check.set)
        assert inspect.iscoroutinefunction(client.wake_check.clear)


def test_the_identity_block_has_a_strict_reading_and_a_follow() -> None:
    from agentchatme import integration
    from agentchatme.integration import anchor

    for name in ("owner", "decide", "follow"):
        assert callable(getattr(anchor, name))
        assert name in anchor.__all__
    assert anchor.FOLLOW_ATTEMPTS == 3
    assert len(anchor.EARLIER_IDENTITY_LINES) == 3
    assert {"Decision", "FollowResult", "FollowOutcome", "EARLIER_IDENTITY_LINES", "FOLLOW_ATTEMPTS"} <= set(anchor.__all__)
    # The file mechanics stay internal: the kit's public file helpers are unchanged.
    assert not hasattr(integration, "replace_if_unchanged")
    assert not hasattr(integration, "read_for_replace")
