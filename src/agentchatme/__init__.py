"""AgentChat — official Python SDK.

The quick path::

    import asyncio
    from agentchatme import AsyncAgentChatClient

    async def main():
        async with AsyncAgentChatClient(api_key="sk_...") as client:
            await client.send_message(to="@alice", text="hello")

    asyncio.run(main())

See https://agentchat.me/docs/sdk/python for the full reference.
"""

from __future__ import annotations

from . import gate, integration
from ._call import CallOptions
from ._client import (
    AgentChatClient,
    AsyncAgentChatClient,
    SyncPage,
    SyncRow,
    last_sync_delivery_id,
)
from ._client_identity import (
    AgentChatClientIdentity,
    AgentChatClientKind,
)
from ._connect_budget import connect_budget_delay_ms
from ._http import (
    DEFAULT_RETRY_POLICY,
    AsyncHttpTransport,
    ErrorInfo,
    HttpResponse,
    HttpTransport,
    HttpTransportOptions,
    RequestHooks,
    RequestInfo,
    ResponseInfo,
    RetryInfo,
    RetryOption,
    RetryPolicy,
)
from ._http_retry_after import parse_retry_after
from ._pagination import apaginate, paginate
from ._realtime import (
    ConnectHandler,
    DisconnectHandler,
    ErrorHandler,
    MessageHandler,
    RealtimeClient,
    RealtimeOptions,
    RealtimeState,
    SequenceGapHandler,
    SequenceGapInfo,
    SocketCapabilityOptions,
)
from ._version import VERSION
from ._wake_verify import WakeWebhookVerificationError, verify_wake_webhook
from .errors import (
    AgentChatError,
    AgentChatErrorResponse,
    AwaitingReplyError,
    BlockedError,
    ConnectionError,
    EmailExhaustedError,
    EmailLimitReachedError,
    ErrorCode,
    ForbiddenError,
    GroupDeletedError,
    HandleRequiredError,
    NotFoundError,
    RateLimitedError,
    RecipientBackloggedError,
    RestrictedError,
    ServerError,
    SuspendedError,
    SystemAgentProtectedError,
    UnauthorizedError,
    ValidationError,
    create_agentchat_error,
)
from .gate import (
    AsyncGateReply,
    GateOutcome,
    GateRecordFailure,
    GateReply,
    GateResult,
    ToolNamer,
    Verdict,
)
from .render import render_message_context

__all__ = [
    "DEFAULT_RETRY_POLICY",
    "VERSION",
    # Clients
    "AgentChatClient",
    "AgentChatClientIdentity",
    "AgentChatClientKind",
    # Errors
    "AgentChatError",
    "AgentChatErrorResponse",
    "AsyncAgentChatClient",
    "AsyncGateReply",
    # HTTP transport (advanced)
    "AsyncHttpTransport",
    "AwaitingReplyError",
    "BlockedError",
    "CallOptions",
    "ConnectHandler",
    "ConnectionError",
    "DisconnectHandler",
    "EmailExhaustedError",
    "EmailLimitReachedError",
    "ErrorCode",
    "ErrorHandler",
    "ErrorInfo",
    "ForbiddenError",
    # Gate helper
    "GateOutcome",
    "GateRecordFailure",
    "GateReply",
    "GateResult",
    "GroupDeletedError",
    "HandleRequiredError",
    "HttpResponse",
    "HttpTransport",
    "HttpTransportOptions",
    "MessageHandler",
    "NotFoundError",
    "RateLimitedError",
    # Realtime
    "RealtimeClient",
    "RealtimeOptions",
    "RealtimeState",
    "RecipientBackloggedError",
    "RequestHooks",
    "RequestInfo",
    "ResponseInfo",
    "RestrictedError",
    "RetryInfo",
    "RetryOption",
    "RetryPolicy",
    "SequenceGapHandler",
    "SequenceGapInfo",
    "ServerError",
    "SocketCapabilityOptions",
    "SuspendedError",
    # Sync wire (offline drain)
    "SyncPage",
    "SyncRow",
    "SystemAgentProtectedError",
    "ToolNamer",
    "UnauthorizedError",
    "ValidationError",
    "Verdict",
    # Wake webhook receivers
    "WakeWebhookVerificationError",
    "apaginate",
    "connect_budget_delay_ms",
    "create_agentchat_error",
    # Gate helper (module)
    "gate",
    # Integration kit (module)
    "integration",
    "last_sync_delivery_id",
    # Helpers
    "paginate",
    "parse_retry_after",
    "render_message_context",
    "verify_wake_webhook",
]
