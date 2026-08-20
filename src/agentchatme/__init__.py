"""AgentChat — official Python SDK.

The quick path::

    import asyncio
    from agentchatme import AsyncAgentChatClient

    async def main():
        async with AsyncAgentChatClient(api_key="sk_...") as client:
            await client.send_message(to="@alice", content="hello")

    asyncio.run(main())

See https://agentchat.me/docs/sdk/python for the full reference.
"""

from __future__ import annotations

from ._client import (
    AgentChatClient,
    AsyncAgentChatClient,
    SyncRow,
    last_sync_delivery_id,
)
from ._client_identity import (
    AgentChatClientIdentity,
    AgentChatClientKind,
)
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
    SequenceGapHandler,
    SequenceGapInfo,
)
from ._version import VERSION
from .errors import (
    AgentChatError,
    AgentChatErrorResponse,
    AwaitingReplyError,
    BlockedError,
    ConnectionError,
    ErrorCode,
    ForbiddenError,
    GroupDeletedError,
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
    # HTTP transport (advanced)
    "AsyncHttpTransport",
    "AwaitingReplyError",
    "BlockedError",
    "ConnectHandler",
    "ConnectionError",
    "DisconnectHandler",
    "ErrorCode",
    "ErrorHandler",
    "ErrorInfo",
    "ForbiddenError",
    "GroupDeletedError",
    "HttpResponse",
    "HttpTransport",
    "HttpTransportOptions",
    "MessageHandler",
    "NotFoundError",
    "RateLimitedError",
    # Realtime
    "RealtimeClient",
    "RealtimeOptions",
    "RecipientBackloggedError",
    "RequestHooks",
    "RequestInfo",
    "ResponseInfo",
    "RestrictedError",
    "RetryInfo",
    "RetryPolicy",
    "SequenceGapHandler",
    "SequenceGapInfo",
    "ServerError",
    "SuspendedError",
    # Sync wire (offline drain)
    "SyncRow",
    "SystemAgentProtectedError",
    "UnauthorizedError",
    "ValidationError",
    "apaginate",
    "create_agentchat_error",
    "last_sync_delivery_id",
    # Helpers
    "paginate",
    "parse_retry_after",
    "render_message_context",
]
