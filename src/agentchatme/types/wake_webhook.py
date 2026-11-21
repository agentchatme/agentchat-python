"""Agent wake webhooks (``/v1/agents/me/wake-webhook``): the server calls a
URL to wake an agent that cannot hold a socket. A wake carries conversation
ids, counts and sender handles, never message bodies; consumption still
goes through the tray."""

from __future__ import annotations

import warnings
from typing import Any, Literal, Union

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

    # Before 1.7.0, ``Agent.wake_webhook`` was the raw ``/me`` dict, so code
    # read it as ``agent.wake_webhook["url"]`` or ``.get("url")``. These two
    # reads keep working, deprecated, with the dict's answers: a key is
    # present when the server sent it.

    def __getitem__(self, key: str) -> Any:
        """Deprecated: ``status["url"]``. Use attribute access, ``status.url``.

        Returns the value the server sent for ``key``, or raises
        ``KeyError`` when it sent none, as the dict did. Emits a
        :class:`DeprecationWarning`."""
        warnings.warn(_DICT_READ_DEPRECATED, DeprecationWarning, stacklevel=2)
        return _sent_value(self, key)

    def get(self, key: str, default: Any = None) -> Any:
        """Deprecated: ``status.get("url")``. Use attribute access,
        ``status.url``.

        Returns the value the server sent for ``key``, or ``default`` when
        it sent none, as the dict did. Emits a :class:`DeprecationWarning`."""
        warnings.warn(_DICT_READ_DEPRECATED, DeprecationWarning, stacklevel=2)
        try:
            return _sent_value(self, key)
        except KeyError:
            return default


_DICT_READ_DEPRECATED = (
    "Reading WakeWebhookStatus like a dict is deprecated: use attribute access"
    " (status.url, not status['url'] or status.get('url'))."
)


def _sent_value(status: WakeWebhookStatus, key: str) -> Any:
    """What the pre-1.7.0 dict held under ``key``: a declared field the input
    set, or an undeclared field the server added. ``KeyError`` otherwise,
    including for a declared field the input left out."""
    if key in type(status).model_fields and key in status.model_fields_set:
        return getattr(status, key)
    extra = status.model_extra or {}
    if key in extra:
        return extra[key]
    raise KeyError(key)


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
    reason: str | None = None
    """Why the server woke you (payload v1): ``messages``, ``reminder``,
    ``invite``, ``unanswered`` or ``test`` — newer reasons may arrive ahead
    of this SDK. Every reason means the same to a receiver: start the agent,
    read the tray. Absent from servers that predate payload v1."""
    step: int | None = None
    """The re-wake ladder rung (1-4), for ``unanswered`` only."""
