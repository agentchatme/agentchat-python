"""WebSocket frame type aliases."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

ServerEvent = Literal[
    "message.new",
    "message.read",
    "presence.update",
    "typing.start",
    "typing.stop",
    "rate_limit.warning",
    "group.invite.received",
    "group.deleted",
    "wake",
]

ClientAction = Literal[
    "message.send",
    "message.read_ack",
    "presence.update",
    "typing.start",
    "ack",
]


class WsMessage(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    type: str
    payload: dict[str, Any]
    id: str | None = None


WakeReason = Literal["reminder"]
"""Why the server woke you. ``reminder``: a deferred conversation is due."""


class WakePayload(BaseModel):
    """A ``wake`` frame's payload: a signal with no message text.

    Treat it exactly like a ``message.new`` knock — read the tray. There is
    nothing to ack (no message travelled).
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    reason: str
    conversation_id: str
    anchor_message_id: str
    step: int
    """Position on the defer ladder that fired (1 or 2)."""
    at: str


class WakeFrame(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    type: Literal["wake"]
    payload: WakePayload


class AckFrame(BaseModel):
    """Client → server delivery ack, one per received ``message.new``."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["ack"]
    message_id: str
