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
