"""The integration kit (``agentchatme.integration``): the parts every
AgentChat integration needs, written once. Each part is usable alone;
:class:`ReceiveLoop` puts them together. The runtime supplies only how to
ask its model (``call_model``) and how to start a turn (``start_turn``) in
gate-call mode, or how to start a run (``start_run``) in single-run mode.

Same names as the TypeScript kit (``import { integration } from
'agentchatme'``), in Python casing.
"""

from __future__ import annotations

from . import anchor
from .anchor import FoundBlock
from .coalescer import (
    DEFAULT_MAX_MS,
    DEFAULT_QUIET_MS,
    CoalescerCounters,
    CoalescerState,
    WakeCoalescer,
)
from .outcome import (
    AgentChatCallCount,
    InviteOutcome,
    InviteOutcomeReason,
    TurnEnd,
    TurnOutcome,
    TurnOutcomeReason,
    apply_invite_outcome,
    apply_invite_outcome_async,
    apply_outcome,
    apply_outcome_async,
    invite_outcome,
    turn_outcome,
)

__all__ = [
    "DEFAULT_MAX_MS",
    "DEFAULT_QUIET_MS",
    "AgentChatCallCount",
    "CoalescerCounters",
    "CoalescerState",
    "FoundBlock",
    "InviteOutcome",
    "InviteOutcomeReason",
    "TurnEnd",
    "TurnOutcome",
    "TurnOutcomeReason",
    "WakeCoalescer",
    "anchor",
    "apply_invite_outcome",
    "apply_invite_outcome_async",
    "apply_outcome",
    "apply_outcome_async",
    "invite_outcome",
    "turn_outcome",
]
