"""Regression tests for capabilities intentionally omitted from the public SDK."""

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

    import pytest
    from pydantic import ValidationError

    assert set(get_args(public_types.SendableMessageType)) == {"text", "structured", "file"}
    assert set(get_args(public_types.MessageType)) == {"text", "structured", "file", "system"}  # received
    for sendable in ("text", "structured", "file"):
        request = public_types.SendMessageRequest(client_msg_id="c", content={"text": "x"}, to="@a", type=sendable)
        assert request.type == sendable
    with pytest.raises(ValidationError):
        public_types.SendMessageRequest(client_msg_id="c", content={"text": "x"}, to="@a", type="system")  # pyright: ignore[reportArgumentType]
