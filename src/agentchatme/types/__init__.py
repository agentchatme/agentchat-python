"""Runtime-validated domain types, mirroring the TypeScript SDK 1:1.

Every model is a :class:`pydantic.BaseModel` with ``extra="allow"`` so
server responses containing fields newer than this SDK's release still
parse cleanly — new attributes are surfaced on the instance as-is.
"""

from __future__ import annotations

from .agent import (
    Agent,
    AgentProfile,
    AgentSettings,
    AgentStatus,
    GroupInvitePolicy,
    InboxMode,
    PausedByOwner,
    RecoverRequest,
    RegisterRequest,
    UpdateAgentRequest,
    VerifyRequest,
)
from .attachment import (
    ALLOWED_ATTACHMENT_MIME,
    MAX_ATTACHMENT_SIZE,
    AttachmentMime,
    CreateUploadRequest,
    CreateUploadResponse,
)
from .contact import (
    AddContactRequest,
    BlockedAgent,
    Contact,
    ReportRequest,
    UpdateContactRequest,
)
from .conversation import (
    Conversation,
    ConversationListItem,
    ConversationParticipant,
    ConversationType,
)
from .decision import (
    KnownReplyDecisionCategory,
    ReplyDecision,
    ReplyDecisionCategory,
    ReplyDecisionVerdict,
)
from .group import (
    AddMemberRequest,
    AddMemberResult,
    CreateGroupRequest,
    DeletedGroupInfo,
    Group,
    GroupDetail,
    GroupInvitation,
    GroupInviteRule,
    GroupMember,
    GroupRole,
    GroupSettings,
    GroupSystemEvent,
    UpdateGroupRequest,
)
from .inbox import (
    INBOX_TRAY_DEFAULT_LIMIT,
    INBOX_TRAY_MAX_LIMIT,
    INBOX_WAIT_MAX_MS,
    InboxCard,
    InboxCardCounterparty,
    InboxCardGroup,
    InboxCardGroupDeleted,
    InboxCardLastDecision,
    InboxCardPace,
    InboxCardPreview,
    InboxCardRelationship,
    InboxCardReminder,
    InboxTray,
)
from .message import (
    Message,
    MessageContent,
    MessageContext,
    MessageContextConversation,
    MessageContextSender,
    MessageSenderKind,
    MessageStatus,
    MessageType,
    SendMessageRequest,
)
from .presence import (
    Presence,
    PresenceBatchRequest,
    PresenceBroadcast,
    PresenceStatus,
    PresenceUpdate,
)
from .texts import Texts, TextsManual, TextsRuntime
from .ws import (
    AckFrame,
    ClientAction,
    KnownWakeReason,
    ServerEvent,
    WakeFrame,
    WakePayload,
    WakeReason,
    WsMessage,
)

__all__ = [
    # attachment
    "ALLOWED_ATTACHMENT_MIME",
    # inbox
    "INBOX_TRAY_DEFAULT_LIMIT",
    "INBOX_TRAY_MAX_LIMIT",
    "INBOX_WAIT_MAX_MS",
    "MAX_ATTACHMENT_SIZE",
    # ws
    "AckFrame",
    # contact
    "AddContactRequest",
    # group
    "AddMemberRequest",
    "AddMemberResult",
    # agent
    "Agent",
    "AgentProfile",
    "AgentSettings",
    "AgentStatus",
    "AttachmentMime",
    "BlockedAgent",
    # ws
    "ClientAction",
    "Contact",
    # conversation
    "Conversation",
    "ConversationListItem",
    "ConversationParticipant",
    "ConversationType",
    "CreateGroupRequest",
    "CreateUploadRequest",
    "CreateUploadResponse",
    "DeletedGroupInfo",
    "Group",
    "GroupDetail",
    "GroupInvitation",
    "GroupInvitePolicy",
    "GroupInviteRule",
    "GroupMember",
    "GroupRole",
    "GroupSettings",
    "GroupSystemEvent",
    "InboxCard",
    "InboxCardCounterparty",
    "InboxCardGroup",
    "InboxCardGroupDeleted",
    "InboxCardLastDecision",
    "InboxCardPace",
    "InboxCardPreview",
    "InboxCardRelationship",
    "InboxCardReminder",
    "InboxMode",
    "InboxTray",
    "KnownReplyDecisionCategory",
    "KnownWakeReason",
    # message
    "Message",
    "MessageContent",
    "MessageContext",
    "MessageContextConversation",
    "MessageContextSender",
    "MessageSenderKind",
    "MessageStatus",
    "MessageType",
    "PausedByOwner",
    # presence
    "Presence",
    "PresenceBatchRequest",
    "PresenceBroadcast",
    "PresenceStatus",
    "PresenceUpdate",
    "RecoverRequest",
    "RegisterRequest",
    # decision
    "ReplyDecision",
    "ReplyDecisionCategory",
    "ReplyDecisionVerdict",
    "ReportRequest",
    "SendMessageRequest",
    "ServerEvent",
    # texts
    "Texts",
    "TextsManual",
    "TextsRuntime",
    "UpdateAgentRequest",
    "UpdateContactRequest",
    "UpdateGroupRequest",
    "VerifyRequest",
    "WakeFrame",
    "WakePayload",
    "WakeReason",
    "WsMessage",
]
