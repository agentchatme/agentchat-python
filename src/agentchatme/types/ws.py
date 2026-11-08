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
    "heartbeat",
]

SocketCapability = Literal["ack", "heartbeat", "rewake"]
"""Capabilities a client may advertise in HELLO; the server echoes the subset
it accepted on ``hello.ok``. ``ack``: per-message delivery acks.
``heartbeat``: a ``heartbeat`` frame on the server's 45 s timer. ``rewake``:
the server's unanswered re-wake goes to this socket."""

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


KnownWakeReason = Literal["reminder", "unanswered"]
"""Wake reasons this SDK knows by name. ``reminder``: a deferred conversation
is due again. ``unanswered``: cards are still on the tray with no verdict
(the server's re-wake ladder: +1 m, +5 m, +30 m, +3 h after the newest
message or invite on them). Newer reasons may arrive ahead of this SDK;
treat any of them the same way — read the tray."""

WakeReason = Union[KnownWakeReason, str]


class WakePayload(BaseModel):
    """A ``wake`` frame's payload: a signal with no message text.

    Treat it exactly like a ``message.new`` — read the tray. There is
    nothing to ack (no message travelled). Which fields are set depends on
    the reason; any other reason carries at least ``reason``.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    reason: WakeReason
    conversation_id: str | None = None
    """``reminder``: the deferred conversation that is due again."""
    anchor_message_id: str | None = None
    """``reminder``: the newest message the deferral covered; anything newer
    is a fresh card anyway."""
    conversation_ids: list[str] | None = None
    """``unanswered``: the conversations (at most 50) whose cards are still
    unanswered."""
    step: int | None = None
    """Ladder step that fired (``reminder``: 1 = +15 min, 2 = +24 h;
    ``unanswered``: 1..4)."""
    at: str | None = None
    """When the wake fired."""


class WakeFrame(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    type: Literal["wake"]
    payload: WakePayload


class HeartbeatPayload(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)

    at: str


class HeartbeatFrame(BaseModel):
    """Server → client, on the 45 s timer, when ``heartbeat`` was negotiated.
    No reply; it only proves the path is alive."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    type: Literal["heartbeat"]
    payload: HeartbeatPayload


class AckFrame(BaseModel):
    """Client → server delivery ack, one per received ``message.new``."""

    model_config = ConfigDict(extra="forbid")

    type: Literal["ack"]
    message_id: str
