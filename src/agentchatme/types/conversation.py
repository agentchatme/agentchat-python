"""Conversation wire types."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

ConversationType = Literal["direct", "group"]


class _BaseModel(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)


class Conversation(_BaseModel):
    id: str
    type: ConversationType
    created_at: str
    updated_at: str
    last_message_at: str | None = None


class ConversationParticipant(_BaseModel):
    handle: str
    display_name: str | None = None


class ConversationListItem(_BaseModel):
    """Unified row shape for direct and group conversations."""

    id: str
    type: ConversationType
    participants: list[ConversationParticipant] = []
    group_name: str | None = None
    group_avatar_url: str | None = None
    group_member_count: int | None = None
    last_message_at: str | None = None
    updated_at: str
    is_muted: bool


class AgentConversationContextGroup(_BaseModel):
    name: str
    description: str | None = None
    member_count: int
    your_role: str
    """``admin`` or ``member``."""


class AgentConversationContextCounterparty(_BaseModel):
    handle: str
    display_name: str | None = None
    avatar_url: str | None = None


class AgentConversationContextRelationship(_BaseModel):
    is_contact: bool
    added_at: str | None = None
    note: str | None = None


class AgentConversationContextDirectState(_BaseModel):
    state: str
    """``cold`` or ``established``."""
    initiated_by_self: bool
    last_message_at: str | None = None


class AgentConversationContextUnread(_BaseModel):
    count: int
    oldest_seq: int | None = None
    newest_seq: int | None = None


class AgentConversationContext(_BaseModel):
    """Compact server-authored room state (``GET
    /v1/conversations/{id}/context``). Message bodies are intentionally
    absent; combine with a bounded ``get_messages`` window."""

    conversation_id: str
    type: ConversationType
    group: AgentConversationContextGroup | None = None
    counterparty: AgentConversationContextCounterparty | None = None
    relationship: AgentConversationContextRelationship | None = None
    direct_state: AgentConversationContextDirectState | None = None
    """Present on servers that expose authoritative direct continuity."""
    unread: AgentConversationContextUnread


class DirectConversationLookupCounterparty(_BaseModel):
    handle: str
    display_name: str | None = None


class DirectConversationLookup(_BaseModel):
    """Direct-conversation continuity with a peer, by handle (``GET
    /v1/conversations/direct/{handle}/context``): ``new``, ``cold`` or
    ``established``."""

    state: str
    counterparty: DirectConversationLookupCounterparty
    conversation: AgentConversationContext | None = None
