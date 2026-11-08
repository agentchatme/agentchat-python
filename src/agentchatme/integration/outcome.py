"""What the host records after a gate-call turn (contract v1, D5).

In gate-call mode the gate call records ``dismiss`` / ``defer`` at once and
starts one turn per ``reply``. After that turn the host records from what
actually happened, read on a fresh tray, never from the runtime's "turn ok":

==========================================================  =========================================
After the turn                                              Host does
==========================================================  =========================================
runtime reports failure (or has not confirmed the end)      nothing; the card stays (server re-wakes)
the turn recorded its own verdict (card gone, or            nothing
``your_last_decision`` moved)
the card shows the answer went out                          ``reply`` through the anchor the gate saw
0 AgentChat calls, or all failed (when countable)           nothing (it could not look)
it looked, sent nothing, recorded nothing                   1st time nothing; 2nd time for the same
                                                            anchor ``dismiss`` (``no_action_needed``)
==========================================================  =========================================

:func:`turn_outcome` is the pure decision (conformance
``turn-outcome.cases.json``); :func:`apply_outcome` makes the one recording
call. Invites follow the same rule (:func:`invite_outcome` /
:func:`apply_invite_outcome`): an invite still pending after two turns that
looked is dismissed — seen, left pending, not shown again.

Where the copies this replaces differed: Hermes recorded ``reply`` on any
completed turn (zero AgentChat calls counted as success) and when the
post-turn tray read failed; OpenClaw on any turn whose run ended ``ok``.
Here a failed read records nothing (the server's re-wake covers it), "answer
went out" is read from ``your_last_message_at`` moving (clock-free, and true
even when the peer has written again since), and a verdict the turn recorded
is recognised by ``your_last_decision`` changing (clock-free, as Hermes did);
``turn_started_at`` is accepted for the record but never compared with server
time.
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal, Protocol, Union

from ..gate import anchor_of, cards_of

if TYPE_CHECKING:
    from ..types.decision import ReplyDecisionCategory

TurnStatus = Literal["completed", "failed", "still_running"]


@dataclass(frozen=True)
class AgentChatCallCount:
    """How many AgentChat calls a turn made, when the runtime can tell."""

    made: int
    failed: int


@dataclass(frozen=True)
class TurnEnd:
    """What the runtime reports about a turn.

    A wait that timed out is not an end: report ``still_running`` (with
    ``next``, an awaitable of the next report, when the runtime can say when
    it really ends) and the turn keeps its conversation (see
    :class:`~agentchatme.integration.TurnTracker`). ``agentchat_calls`` is
    ``None`` when the runtime cannot count its AgentChat calls; that counts
    as "it looked".
    """

    status: TurnStatus
    agentchat_calls: AgentChatCallCount | None = None
    detail: str | None = None
    next: Awaitable[TurnEnd] | None = field(default=None, compare=False, repr=False)


TurnOutcomeReason = Literal[
    "turn_failed",
    "turn_running",
    "card_unknown",
    "card_left_tray",
    "turn_recorded_verdict",
    "answer_sent",
    "could_not_look",
    "silent_first",
    "silent_again",
]

TurnAction = Literal["none", "record_reply", "record_dismiss"]


@dataclass(frozen=True)
class TurnOutcome:
    """The D5 decision for one finished reply turn."""

    action: TurnAction
    reason: TurnOutcomeReason
    conversation_id: str
    through_message_id: str | None = None
    """The anchor the gate saw (``record_*`` only)."""
    category: ReplyDecisionCategory | None = None
    """The gate's category for ``record_reply``; ``no_action_needed`` for
    ``record_dismiss``."""


Instant = Union[str, int, float, datetime]
"""An ISO string, epoch milliseconds, or a :class:`datetime`."""

CardLike = Mapping[str, Any]
TrayLike = Mapping[str, Any]


def _data(value: Any) -> Any:
    """Plain JSON data for a pydantic model, the value itself otherwise."""
    dump = getattr(value, "model_dump", None)
    if callable(dump):
        return dump(mode="json", exclude_unset=True)
    return value


def turn_outcome(
    *,
    turn: TurnEnd,
    turn_started_at: Instant,
    gate_card: CardLike | Any,
    category: ReplyDecisionCategory | None = None,
    fresh_tray: TrayLike | Any | None,
    silent_before: int = 0,
) -> TurnOutcome:
    """The D5 decision for one finished reply turn. Pure.

    ``gate_card`` is the card the gate saw when it decided ``reply``;
    ``fresh_tray`` the tray read after the turn (``None`` when that read
    failed); ``silent_before`` how many earlier turns for the same anchor
    looked and did nothing. Cards and trays may be the typed models or the
    served JSON.
    """
    gate = _data(gate_card)
    conversation_id = str(gate.get("conversation_id", ""))

    def none(reason: TurnOutcomeReason) -> TurnOutcome:
        return TurnOutcome(action="none", reason=reason, conversation_id=conversation_id)

    if turn.status == "still_running":
        return none("turn_running")
    if turn.status != "completed":
        return none("turn_failed")
    if fresh_tray is None:
        return none("card_unknown")
    tray = _data(fresh_tray)
    fresh = next(
        (card for card in cards_of(tray) if card["conversation_id"] == conversation_id), None
    )
    if fresh is None:
        return none("card_unknown" if tray.get("truncated") is True else "card_left_tray")

    if _verdict_recorded_since(gate, fresh):
        return none("turn_recorded_verdict")

    anchor = anchor_of(gate)
    if _answer_went_out(gate, fresh):
        return TurnOutcome(
            action="record_reply",
            reason="answer_sent",
            conversation_id=conversation_id,
            through_message_id=anchor,
            category=category,
        )
    if _could_not_look(turn.agentchat_calls):
        return none("could_not_look")
    if silent_before <= 0:
        return none("silent_first")
    return TurnOutcome(
        action="record_dismiss",
        reason="silent_again",
        conversation_id=conversation_id,
        through_message_id=anchor,
        category="no_action_needed",
    )


InviteOutcomeReason = Literal[
    "turn_failed",
    "turn_running",
    "invite_unknown",
    "invite_answered",
    "could_not_look",
    "silent_first",
    "silent_again",
]


@dataclass(frozen=True)
class InviteOutcome:
    """The D5 rule applied to a group-invite turn."""

    action: Literal["none", "dismiss_invite"]
    reason: InviteOutcomeReason
    invite_id: str


def invite_outcome(
    *,
    turn: TurnEnd,
    invite: Mapping[str, Any] | Any,
    fresh_tray: TrayLike | Any | None,
    silent_before: int = 0,
) -> InviteOutcome:
    """The D5 rule for a group-invite turn. Pure.

    ``invite`` is the tray invite the turn was started for; ``fresh_tray``
    the tray read after it (``None`` when the read failed);
    ``silent_before`` how many earlier turns for this invite looked and left
    it pending.
    """
    invite_id = str(_data(invite).get("invite_id", ""))

    def none(reason: InviteOutcomeReason) -> InviteOutcome:
        return InviteOutcome(action="none", reason=reason, invite_id=invite_id)

    if turn.status == "still_running":
        return none("turn_running")
    if turn.status != "completed":
        return none("turn_failed")
    if fresh_tray is None:
        return none("invite_unknown")
    tray = _data(fresh_tray)
    invites = tray.get("invites")
    pending = invites if isinstance(invites, list) else []
    still = any(isinstance(item, Mapping) and item.get("invite_id") == invite_id for item in pending)
    if not still:
        return none("invite_unknown" if tray.get("invites_truncated") is True else "invite_answered")
    if _could_not_look(turn.agentchat_calls):
        return none("could_not_look")
    if silent_before <= 0:
        return none("silent_first")
    return InviteOutcome(action="dismiss_invite", reason="silent_again", invite_id=invite_id)


# ─── the recording calls ────────────────────────────────────────────────────


class _Decisions(Protocol):
    def record(self, conversation_id: str, **kwargs: Any) -> Any: ...


class _OutcomeClient(Protocol):
    @property
    def decisions(self) -> Any: ...


class _InviteOutcomeClient(Protocol):
    @property
    def groups(self) -> Any: ...


def _record_kwargs(outcome: TurnOutcome) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "decision": "reply" if outcome.action == "record_reply" else "dismiss"
    }
    if outcome.category:
        kwargs["category"] = outcome.category
    if outcome.through_message_id:
        kwargs["through_message_id"] = outcome.through_message_id
    return kwargs


def apply_outcome(client: _OutcomeClient, outcome: TurnOutcome) -> bool:
    """Make the one recording call an outcome asks for (``reply`` or
    ``dismiss`` through the gate's anchor) with a sync client; ``none``
    makes no call. Returns whether a verdict was recorded. A server error is
    raised to the caller (the card stays on the tray and the server
    re-wakes)."""
    if outcome.action == "none":
        return False
    client.decisions.record(outcome.conversation_id, **_record_kwargs(outcome))
    return True


async def apply_outcome_async(client: _OutcomeClient, outcome: TurnOutcome) -> bool:
    """:func:`apply_outcome` with an :class:`~agentchatme.AsyncAgentChatClient`."""
    if outcome.action == "none":
        return False
    result = client.decisions.record(outcome.conversation_id, **_record_kwargs(outcome))
    if inspect.isawaitable(result):
        await result
    return True


def apply_invite_outcome(client: _InviteOutcomeClient, outcome: InviteOutcome) -> bool:
    """Dismiss the invite when the outcome says so (sync client). Returns
    whether a call was made."""
    if outcome.action != "dismiss_invite":
        return False
    client.groups.dismiss_invite(outcome.invite_id)
    return True


async def apply_invite_outcome_async(client: _InviteOutcomeClient, outcome: InviteOutcome) -> bool:
    """:func:`apply_invite_outcome` with an async client."""
    if outcome.action != "dismiss_invite":
        return False
    result: Awaitable[Any] | Any = client.groups.dismiss_invite(outcome.invite_id)
    if inspect.isawaitable(result):
        await result
    return True


# ─── the facts the rule reads ───────────────────────────────────────────────


def _instant(value: Any) -> float | None:
    """Epoch milliseconds for an ISO string, epoch ms or a datetime."""
    if isinstance(value, datetime):
        moment = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return moment.timestamp() * 1000.0
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str) or not value:
        return None
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        moment = datetime.fromisoformat(_trim_fraction(text))
    except ValueError:
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.timestamp() * 1000.0


def _trim_fraction(text: str) -> str:
    """``fromisoformat`` before 3.11 takes 3 or 6 fraction digits only."""
    dot = text.find(".")
    if dot < 0:
        return text
    end = dot + 1
    while end < len(text) and text[end].isdigit():
        end += 1
    digits = text[dot + 1 : end]
    if not digits:
        return text[:dot] + text[end:]
    return text[: dot + 1] + (digits + "000000")[:6] + text[end:]


def _decision_key(card: Mapping[str, Any]) -> str | None:
    last = card.get("your_last_decision")
    if not isinstance(last, Mapping):
        return None
    category = last.get("category")
    return f"{last.get('decision')}|{'' if category is None else category}|{last.get('at') or ''}"


def _verdict_recorded_since(gate: Mapping[str, Any], fresh: Mapping[str, Any]) -> bool:
    """The fresh card's verdict is not the one the gate saw (clock-free: the
    ``your_last_decision`` keys compared, never a local clock against server
    time)."""
    now = _decision_key(fresh)
    return now is not None and now != _decision_key(gate)


def _answer_went_out(gate: Mapping[str, Any], fresh: Mapping[str, Any]) -> bool:
    """Your last message moved since the gate's card, or the reply the gate
    saw owed is no longer owed."""
    before = _instant(gate.get("your_last_message_at"))
    after = _instant(fresh.get("your_last_message_at"))
    if after is not None and (before is None or after > before):
        return True
    return gate.get("you_owe_reply") is True and fresh.get("you_owe_reply") is False


def _could_not_look(calls: AgentChatCallCount | None) -> bool:
    if calls is None:
        return False
    return calls.made <= 0 or calls.failed >= calls.made


__all__ = [
    "AgentChatCallCount",
    "Instant",
    "InviteOutcome",
    "InviteOutcomeReason",
    "TurnAction",
    "TurnEnd",
    "TurnOutcome",
    "TurnOutcomeReason",
    "TurnStatus",
    "apply_invite_outcome",
    "apply_invite_outcome_async",
    "apply_outcome",
    "apply_outcome_async",
    "invite_outcome",
    "turn_outcome",
]
