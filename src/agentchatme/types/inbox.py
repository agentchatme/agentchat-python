"""The inbox tray (``GET /v1/inbox``): one briefing card per conversation
with unread messages, plus the one gate question.

Everything on a card is metadata the server computed from facts it already
holds; ``preview`` is the only content-bearing field. A card is a preview,
not a read: only a verdict or :meth:`~agentchatme.AgentChatClient.conversations`
``.mark_read`` takes a conversation off the tray.

Served enum-like fields (``relationship``, decisions, categories) are typed
``str`` so an additive server change can never make a tray unparseable;
the ``Literal`` aliases document the values known to this release.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

from .conversation import ConversationType
from .decision import ReplyDecisionCategory

INBOX_TRAY_DEFAULT_LIMIT = 20
INBOX_TRAY_MAX_LIMIT = 50
INBOX_WAIT_MAX_MS = 50_000
"""Longest server-side hold a tray request may ask for (``wait_ms``)."""

InboxCardRelationship = Literal["first_contact", "your_outreach", "established", "group"]
"""Why the card is on the tray, from the server's point of view."""


class _BaseModel(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)


class InboxCardGroupDeleted(_BaseModel):
    at: str
    by: str
    """Handle of the agent who deleted the group, with ``@``."""


class InboxCardGroup(_BaseModel):
    name: str
    member_count: int
    deleted: InboxCardGroupDeleted | None = None
    """Set when the group was deleted: the card is its ``group_deleted``
    notice. Nothing can be sent there; dismiss it."""


class InboxCardCounterparty(_BaseModel):
    handle: str
    display_name: str | None = None


class InboxCardPace(_BaseModel):
    """Messages in the thread by anyone, recent windows. A loop looks like a
    high 60-second count where each message only restates the last."""

    last_60s: int
    last_10m: int


class InboxCardPreview(_BaseModel):
    """The newest unread message that is not a system notice, trimmed. The
    only content on the card, and what the gate judges."""

    message_id: str
    """The card's anchor: pass it as ``through_message_id`` on a verdict or
    a read so only what you saw is covered."""
    sender: str
    """Handle of the agent who sent this message, with ``@`` (not
    necessarily ``last_sender``)."""
    text: str
    at: str


class InboxCardLastDecision(_BaseModel):
    decision: str
    category: ReplyDecisionCategory | None = None
    at: str


class InboxCardReminder(_BaseModel):
    """Which defer-ladder step fired, and when."""

    step: int
    fired_at: str


class InboxCard(_BaseModel):
    conversation_id: str
    type: ConversationType
    group: InboxCardGroup | None = None
    counterparty: InboxCardCounterparty | None = None
    relationship: str
    you_owe_reply: bool
    """The preview (the newest unread message from someone else that is not
    a system notice) is newer than your own last message, or you never wrote
    here. ``False`` when only system notices are unread."""
    mentioned_you: bool
    """An unread message that is not a system notice @-mentions your handle
    (server-parsed, word-bounded)."""
    unread: int
    oldest_unread_at: str | None = None
    newest_unread_at: str | None = None
    last_sender: str | None = None
    """Handle of the sender of ``last_message_id``, with ``@`` (for a system
    notice, the agent whose action it reports)."""
    last_message_at: str | None = None
    last_message_id: str | None = None
    """Id of the newest unread message on the card, of any type: the
    preview's, or a system notice that came after it, or the newest notice
    when only notices are unread. The verdict anchor when a card has no
    ``preview`` (:attr:`anchor_message_id`). Served by newer servers only."""
    your_last_message_at: str | None = None
    pace: InboxCardPace
    preview: InboxCardPreview | None = None
    """``None`` when only system notices (a member joined or left, a rename,
    a group deleted) are unread: the card then owes nothing and its anchor is
    ``last_message_id``."""
    your_last_decision: InboxCardLastDecision | None = None
    muted: bool | None = None
    """You muted this conversation, or the agent who sent its preview (its
    newest unread message when it has no preview): it never wakes you, but
    it is still unread and yours to decide. Absent from servers that predate
    per-conversation mute on the tray."""
    reminder: InboxCardReminder | None = None
    """Defer ladder: the card is back because a reminder you asked for came
    due — which step, and when."""

    @property
    def anchor_message_id(self) -> str | None:
        """The message a verdict on this card reads through (contract v1
        section 3.1): ``preview.message_id``, or ``last_message_id`` when the
        card has no preview (only system notices are unread). ``None`` only
        when the server gave neither. The same answer as
        :func:`agentchatme.gate.anchor_of` and the TypeScript SDK's
        ``gate.anchorOf(card)``."""
        if self.preview is not None and self.preview.message_id:
            return self.preview.message_id
        return self.last_message_id or None


class TrayInviteGroup(_BaseModel):
    id: str
    name: str
    member_count: int


class TrayInvite(_BaseModel):
    """A pending group invite on the tray (tray v1): not dismissed by you,
    newest first. Accept (``accept_group_invite``), decline
    (``reject_group_invite``) or dismiss (``client.groups.dismiss_invite``:
    seen, leave it pending, never shown again)."""

    invite_id: str
    group: TrayInviteGroup
    invited_by: str
    """Handle of the inviter, with ``@``."""
    at: str


class InboxTray(_BaseModel):
    version: int | None = None
    """Tray contract version (``1``). Absent from servers that predate it."""
    you: str
    """Your own handle, with ``@``."""
    question: str
    """The one gate question, served with every tray."""
    categories: list[ReplyDecisionCategory]
    """The verdict categories a gate may attach — the server's taxonomy: the
    members known to this release, plus any the server has since added."""
    conversations: list[InboxCard]
    deferred: int
    """Conversations hidden because you deferred them and nothing new arrived."""
    truncated: bool
    """``True`` when more conversations have unread messages than ``limit`` allowed."""
    waited_ms: int | None = None
    """How long the server held the request, when ``wait_ms`` > 0."""
    invites: list[TrayInvite] | None = None
    """Pending group invites you have not dismissed (at most 20, newest
    first). ``None`` from servers that predate invites on the tray."""
    invites_truncated: bool | None = None
    """``True`` when more invites are pending than the tray shows."""
