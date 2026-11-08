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
from ._files import rename_with_retry, write_atomic
from .anchor import FoundBlock
from .coalescer import (
    DEFAULT_MAX_MS,
    DEFAULT_QUIET_MS,
    CoalescerCounters,
    CoalescerState,
    WakeCoalescer,
)
from .manual_sync import (
    MANUAL_FETCH_TIMEOUT_MS,
    MANUAL_MAX_BYTES,
    MANUAL_MAX_REDIRECTS,
    MANUAL_SKILL_NAME,
    ManualSync,
    ManualSyncResult,
    ManualSyncState,
    ServedManual,
    manual_front_matter,
    manual_version,
    sync_manual,
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
from .probe import (
    HOSTED_MCP_PATH,
    REQUIRED_HOSTED_TOOLS,
    ProbeHostedToolsResult,
    hosted_mcp_url,
    probe_hosted_tools,
    probe_hosted_tools_async,
)
from .receive_loop import (
    CycleOutcome,
    CycleSummary,
    Readiness,
    ReceiveLoop,
    ReceiveLoopEvent,
    ReceiveLoopMode,
    ReceiveLoopSocketOptions,
    ReceiveLoopStatus,
    RunEnd,
    RunRequest,
    TurnAdmission,
    TurnRequest,
    WakeSource,
    reply_prompt,
)
from .texts_cache import (
    TEXTS_FETCH_TIMEOUT_MS,
    TEXTS_REFRESH_MS,
    TEXTS_RETRY_MAX_MS,
    TEXTS_RETRY_MS,
    TextsCache,
    TextsCacheSource,
    TextsListener,
    is_texts,
)
from .turns import RunningTurn, TrackedTurn, TurnHandle, TurnTracker

__all__ = [
    "DEFAULT_MAX_MS",
    "DEFAULT_QUIET_MS",
    "HOSTED_MCP_PATH",
    "MANUAL_FETCH_TIMEOUT_MS",
    "MANUAL_MAX_BYTES",
    "MANUAL_MAX_REDIRECTS",
    "MANUAL_SKILL_NAME",
    "REQUIRED_HOSTED_TOOLS",
    "TEXTS_FETCH_TIMEOUT_MS",
    "TEXTS_REFRESH_MS",
    "TEXTS_RETRY_MAX_MS",
    "TEXTS_RETRY_MS",
    "AgentChatCallCount",
    "CoalescerCounters",
    "CoalescerState",
    "CycleOutcome",
    "CycleSummary",
    "FoundBlock",
    "InviteOutcome",
    "InviteOutcomeReason",
    "ManualSync",
    "ManualSyncResult",
    "ManualSyncState",
    "ProbeHostedToolsResult",
    "Readiness",
    "ReceiveLoop",
    "ReceiveLoopEvent",
    "ReceiveLoopMode",
    "ReceiveLoopSocketOptions",
    "ReceiveLoopStatus",
    "RunEnd",
    "RunRequest",
    "RunningTurn",
    "ServedManual",
    "TextsCache",
    "TextsCacheSource",
    "TextsListener",
    "TrackedTurn",
    "TurnAdmission",
    "TurnEnd",
    "TurnHandle",
    "TurnOutcome",
    "TurnOutcomeReason",
    "TurnRequest",
    "TurnTracker",
    "WakeCoalescer",
    "WakeSource",
    "anchor",
    "apply_invite_outcome",
    "apply_invite_outcome_async",
    "apply_outcome",
    "apply_outcome_async",
    "hosted_mcp_url",
    "invite_outcome",
    "is_texts",
    "manual_front_matter",
    "manual_version",
    "probe_hosted_tools",
    "probe_hosted_tools_async",
    "rename_with_retry",
    "reply_prompt",
    "sync_manual",
    "turn_outcome",
    "write_atomic",
]
