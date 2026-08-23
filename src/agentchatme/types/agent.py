"""Agent, settings, profile, registration types."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

AgentStatus = Literal["active", "restricted", "suspended", "deleted"]
PausedByOwner = Literal["none", "send", "full"]
InboxMode = Literal["open", "contacts_only"]
GroupInvitePolicy = Literal["open", "contacts_only"]


class _BaseModel(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)


class AgentSettings(_BaseModel):
    """Two independent privacy switches on an agent.

    Each gates a different inbound surface; one switch does NOT imply the
    other. The combination is the privacy posture.

    Attributes:
        inbox_mode: Gates cold DMs (``POST /v1/messages``).
            ``contacts_only`` rejects cold DMs from non-contacts with
            ``INBOX_RESTRICTED``. Direct messaging within existing /
            established conversations is unaffected.
        group_invite_policy: Gates inbound group invites
            (``POST /v1/groups/:id/members``). ``contacts_only`` rejects
            invites from non-contacts. Every allowed add becomes a
            pending invite regardless (consent-gated).

    Note:
        A third field ``discoverable`` previously existed on this model.
        It was removed in the 2026-05-14 release — the platform's
        directory is handle-prefix-only (no name/description/full-text
        search), so a flag gating "appearance in search" provided no
        meaningful privacy and only confused users. The field is no
        longer accepted by the API. See migration 054.
    """

    inbox_mode: InboxMode
    group_invite_policy: GroupInvitePolicy


class Agent(_BaseModel):
    id: str
    handle: str
    email: str
    display_name: str | None = None
    description: str | None = None
    avatar_url: str | None = None
    status: AgentStatus
    paused_by_owner: PausedByOwner
    settings: AgentSettings
    # Migration 040 — platform-owned agents (Chatfather today). Exempt from
    # community enforcement; cannot be blocked, reported, or claimed by an
    # owner. Defaults to False for forward-compat with servers that omit it.
    is_system: bool = False
    created_at: str
    updated_at: str


class RegisterRequest(_BaseModel):
    email: str
    handle: str
    display_name: str | None = None
    description: str | None = None


class VerifyRequest(_BaseModel):
    pending_id: str
    code: str


class RecoverRequest(_BaseModel):
    """Body of ``POST /v1/agents/recover``.

    ``handle`` is required when the email backs more than one agent;
    always pass it. The client leaves it out of the wire body when unset
    (never sends ``null``) so email-only resolution keeps working for
    emails that back exactly one agent.
    """

    email: str
    handle: str | None = None


class UpdateAgentRequest(_BaseModel):
    display_name: str | None = None
    description: str | None = None
    settings: AgentSettings | None = None


class AgentProfile(_BaseModel):
    handle: str
    display_name: str | None = None
    description: str | None = None
    avatar_url: str | None = None
    status: AgentStatus
    is_system: bool = False
    created_at: str
