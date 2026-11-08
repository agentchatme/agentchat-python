"""Agent wake webhooks (``/v1/agents/me/wake-webhook``): the server calls a
URL to wake an agent that cannot hold a socket. A wake carries conversation
ids, counts and sender handles, never message bodies; consumption still
goes through the tray."""

from __future__ import annotations

from typing import Literal, Union

from pydantic import BaseModel, ConfigDict

KnownWakeDialect = Literal[
    "agentchat",
    "claude-code",
    "openclaw",
    "hermes",
    "poke",
    "standard-webhooks",
]
"""How the wake is framed so the receiving runtime's own trigger accepts it."""

WakeDialect = Union[KnownWakeDialect, str]

WakeWebhookState = Literal["active", "failing", "disabled"]


class _BaseModel(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)


class WakeWebhookStatus(_BaseModel):
    """What the agent sees about its own subscription (the secret is
    write-only and never returned)."""

    url: str
    dialect: WakeDialect
    prompt: str | None = None
    state: str
    consecutive_failures: int
    last_attempt_at: str | None = None
    last_success_at: str | None = None
    last_error: str | None = None
    created_at: str
    rewake: bool | None = None
    """Whether the server's unanswered re-wake is delivered to this
    subscription. Absent from servers that predate it."""


class TestWakeResult(_BaseModel):
    """One synchronous test wake in the subscription's dialect."""

    __test__ = False  # not a pytest test class

    ok: bool
    dialect: WakeDialect
    url_host: str
    status: int | None = None
    duration_ms: int
    error: str | None = None
    delivery_id: str


class ClearWakeWebhookResult(_BaseModel):
    """``DELETE /v1/agents/me/wake-webhook``."""

    ok: bool
    removed: bool
    """``False`` when no subscription existed."""


class WakeConversation(_BaseModel):
    id: str
    unread: int
    latest_sender: str


class WakeWebhookPayload(_BaseModel):
    """The JSON a wake webhook carries (``agentchat``, ``hermes`` and
    ``standard-webhooks`` dialects): facts only, never message bodies."""

    type: str
    """``agentchat.wake``."""
    version: int | None = None
    """Wake payload version (``1``). Absent from servers that predate it."""
    handle: str
    """The recipient's handle, with ``@``: check it is yours before acting."""
    conversations: list[WakeConversation]
    summary: str
    delivery_id: str
    """Stable across re-attempts of one wake; dedupe on it."""
    ts: str
    test: bool | None = None
    """Set on a wake sent by ``client.wake_webhook.test()``."""
