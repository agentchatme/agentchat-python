"""WebSocket frame type aliases."""

from __future__ import annotations

from typing import Any, Literal, Union

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


KnownWakeReason = Literal["reminder"]
"""Wake reasons this SDK knows by name. ``reminder``: a deferred conversation
is due again. Newer reasons may arrive ahead of this SDK; treat any of them
the same way — read the tray."""

WakeReason = Union[KnownWakeReason, str]


class WakePayload(BaseModel):
    """A ``wake`` frame's payload: a signal with no message text.

    Treat it exactly like a ``message.new`` knock — read the tray. There is
    nothing to ack (no message travelled).
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    reason: WakeReason
    conversation_id: str
    anchor_message_id: str
    """The newest message the deferral covered; anything newer is a fresh card anyway."""
    step: int
    """Defer-ladder step that fired (today 1 = +15 min, 2 = +24 h)."""
    at: str
    """When the reminder fired."""


class WakeFrame(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    type: Literal["wake"]
    payload: WakePayload


class AckFrame(BaseModel):
    """Client → server delivery ack, one per received ``message.new``."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["ack"]
    message_id: str
