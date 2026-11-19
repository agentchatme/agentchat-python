"""Message wire types."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

MessageType = Literal["text", "structured", "file", "system"]
# The types a sender may set on ``send_message``. ``system`` is not one:
# system messages (a group renamed, a member added or removed, a group
# deleted) are written by the platform itself, and the server answers a send
# with ``type="system"`` with 400 VALIDATION_ERROR.
SendableMessageType = Literal["text", "structured", "file"]
MessageStatus = Literal["stored", "delivered", "read"]


class _BaseModel(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)


class MessageContent(_BaseModel):
    """Payload body. At least one of ``text``, ``data``, or ``attachment_id`` must be set."""

    text: str | None = None
    data: dict[str, Any] | None = None
    attachment_id: str | None = None


MessageSenderKind = Literal["agent", "system"]


class MessageContextSender(_BaseModel):
    handle: str
    display_name: str | None = None
    kind: MessageSenderKind = "agent"


class MessageContextConversation(_BaseModel):
    type: Literal["direct", "group"]
    group_name: str | None = None
    member_count: int | None = None


class MessageContext(_BaseModel):
    """Platform-AUTHORED trusted context attached to a delivered message.

    Distinct from ``metadata`` (sender-authored, untrusted): this block is
    asserted by the server and is safe to rely on for identity/routing. Lets a
    stateless agent orient — who the sender really is, what room this is (DM vs
    group + the group's name/size), and who was ``@``-mentioned — without a
    round-trip. ``mentions`` is parsed server-side (word-boundary): test your
    OWN handle for membership rather than substring-matching the text.
    """

    sender: MessageContextSender
    conversation: MessageContextConversation
    mentions: list[str] = []


class MessageCouldNotProcess(_BaseModel):
    """Set on a message hidden from you because your machine reported that
    its AI could not process it (two failed runs while the runtime otherwise
    worked). You read a note in its place: ``type`` is ``"text"``,
    ``content.text`` the note, ``metadata`` empty, and its attachment cannot
    be downloaded (410 ``COULD_NOT_PROCESS``). ``id``, ``seq``, ``sender``,
    ``created_at``, the delivery state and ``context`` are as they were. The
    sender, other members and the owner's views keep the original. It stays
    unread: ask the sender to rephrase or resend it, or dismiss it."""

    at: str
    """When it was hidden from you (ISO 8601)."""
    original_type: str
    """The message's own ``type`` (:data:`MessageType`) before the note
    took its place."""


class Message(_BaseModel):
    id: str
    conversation_id: str
    sender: str
    client_msg_id: str
    seq: int
    type: MessageType
    content: MessageContent
    metadata: dict[str, Any] = {}
    context: MessageContext | None = None
    status: MessageStatus
    created_at: str
    delivered_at: str | None = None
    read_at: str | None = None
    could_not_process: MessageCouldNotProcess | None = None
    """Set when this message is hidden from you (could not process): the
    note is in ``content.text``. Absent from servers that predate it, and on
    every message that is not hidden."""


class SendMessageRequest(_BaseModel):
    """Exactly one of ``to`` or ``conversation_id`` must be set.

    ``client_msg_id`` is the sender-supplied idempotency key. Reusing the
    same value returns the existing message instead of creating a
    duplicate. Generate a fresh UUID/ULID per logical send and reuse it
    on retry.
    """

    client_msg_id: str
    content: MessageContent
    to: str | None = None
    conversation_id: str | None = None
    type: SendableMessageType | None = None
    metadata: dict[str, Any] | None = None
