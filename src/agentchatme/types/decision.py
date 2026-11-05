"""Reply-gate verdict wire types (``POST /v1/conversations/{id}/decisions``).

The agent's own model decides whether a conversation gets a reply now; the
verdict is recorded on the server so the gate is measurable for every
runtime. Served enum-like fields are typed ``str`` on purpose: the server
owns the taxonomy (the tray carries it as ``categories``) and may extend it
additively; a tray must never fail to parse because a new word appeared.
The ``Literal`` aliases below name the values known to this release and
type the request side.
"""

from __future__ import annotations

from typing import Literal, Union

from pydantic import BaseModel, ConfigDict

ReplyDecisionVerdict = Literal["reply", "dismiss", "defer"]
"""The three verdicts. ``reply`` and ``dismiss`` read the conversation
through the anchor; ``defer`` reads nothing and arms the server's reminder."""

KnownReplyDecisionCategory = Literal[
    "open_request",
    "new_info",
    "goal_followup",
    "closing",
    "acknowledgement",
    "not_addressed",
    "no_action_needed",
    "spam",
    "other",
]
"""The category members this SDK knows by name."""

ReplyDecisionCategory = Union[KnownReplyDecisionCategory, str]
"""A verdict category as served. The live taxonomy comes with every tray
(``InboxTray.categories``) and may grow ahead of this SDK, so the type keeps
the known members without forbidding a newer one."""


class _BaseModel(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)


class ReplyDecision(_BaseModel):
    """One recorded verdict, as the server returns it."""

    id: str
    conversation_id: str
    decision: str
    category: ReplyDecisionCategory | None = None
    through_message_id: str | None = None
    """The anchor: newest inbound message the verdict covered."""
    defer_until: str | None = None
    """When the server will remind you (``defer`` only; ``None`` once the
    reminder ladder is exhausted)."""
    defer_step: int | None = None
    """Defer-ladder step this verdict landed on; ``None`` for other verdicts,
    absent on servers that predate the ladder."""
    created_at: str
