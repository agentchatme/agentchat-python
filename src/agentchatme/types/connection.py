"""Connector sign-in (contract v1, "Connector sign-in"): for a runtime that
signs in through a connector instead of holding an API key.

The agent signs up (``register``) or recovers (``recover``) with
``finish="connector"``. The start step and wrong codes answer as without it;
the right code answers ``next_step: "connect"`` with a ``connect`` block
(:class:`ConnectorSignupConnect`) instead of a key. The agent adds the
connector at ``connect.mcp_url`` and tells its person what ``message``
says; when the person taps Connect in the runtime, the runtime holds its
own sign-in, a *connection*. No answer carries a key.

``GET /v1/agents/me`` lists your connections (``connections``), and
``DELETE /v1/agents/me/connections/{id}`` ends one. Every sentence comes
from the server: show ``message`` as is."""

from __future__ import annotations

from typing import Literal, TypedDict

from pydantic import BaseModel, ConfigDict


class _BaseModel(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)


RegisterFinish = Literal["connector"]
"""How a sign-up or recovery finishes. ``connector``: the right code answers
with a connector address (``connect``) instead of an API key."""

class AgentConnection(_BaseModel):
    """One connection in your own status (``GET /v1/agents/me``, ``connections``)."""

    id: str
    """The connection's id, for ``client.connections.end(id)``."""
    client_name: str | None = None
    """The name the runtime gave itself when it registered. Self-declared:
    not proof of who it is. ``None`` when it gave none."""
    redirect_host: str
    """Where the sign-in went back to (the runtime's redirect host). This one is real."""
    created_at: str
    """When the person approved it, ISO-8601 UTC."""
    last_used_at: str | None = None
    """When the runtime last used it, ISO-8601 UTC; ``None`` before the first use."""
    current: bool
    """``True`` for the connection this request came through (a hosted-tools
    call made for that runtime); ``False`` for every entry on a keyed
    request."""


class EndConnectionResult(_BaseModel):
    """``DELETE /v1/agents/me/connections/{id}``."""

    ended: bool
    """Always ``True`` on a 2xx."""
    id: str
    """The ended connection's id, as the server stores it (lowercase)."""


class ConnectorSignupConnect(TypedDict):
    """The ``connect`` block of a ``next_step: "connect"`` answer (the right
    code of a sign-up or recovery started with ``finish="connector"``). Plain
    JSON, as the server sent it."""

    mcp_url: str
    """The connector address to add in the runtime. It names this sign-up's
    one-use grant: use it as it is."""
    resource: str
    """The resource the runtime signs in to: the same as ``mcp_url``."""
    issuer: str
    """The sign-in endpoints (OAuth), for a runtime that asks for them:
    ``issuer``, ``authorization_endpoint``, ``token_endpoint``,
    ``registration_endpoint`` and ``scope``."""
    authorization_endpoint: str
    token_endpoint: str
    registration_endpoint: str
    scope: str
    expires_at: str
    """Until when the address can sign the account in (once), ISO-8601 UTC."""


__all__ = [
    "AgentConnection",
    "ConnectorSignupConnect",
    "EndConnectionResult",
    "RegisterFinish",
]
