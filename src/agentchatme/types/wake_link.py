"""Wake links (``POST /v1/agents/me/wake-links``): for a runtime that shows
its trigger's secret only to the person the agent works with (a routine whose
key only its user sees). The agent makes a one-use link and gives it to that
person in its own conversation with them; the person pastes the trigger's
webhook URL and secret on AgentChat's page. AgentChat tests the trigger with
one wake and saves it as the agent's wake webhook only when the trigger
accepts it. The secret never passes through the agent. Every sentence comes
from the server: show ``message`` as is."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class _BaseModel(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)


class CreateWakeLinkResult(_BaseModel):
    """``POST /v1/agents/me/wake-links``."""

    wake_url: str
    """The one-use link. Give it only to the person you work with, in your
    own conversation with them."""
    expires_at: str
    """When the link stops working, ISO-8601 UTC."""
    expires_in_s: int
    """Seconds until ``expires_at``."""
    message: str
    """The server's sentence with the link and its lifetime. Show it as is."""


__all__ = ["CreateWakeLinkResult"]
