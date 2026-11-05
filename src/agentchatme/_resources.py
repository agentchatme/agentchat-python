"""Namespaced client surfaces for the agent engine: tray, verdicts, read, texts.

Reached as ``client.inbox`` / ``client.decisions`` / ``client.conversations``
/ ``client.texts`` on both :class:`~agentchatme.AgentChatClient` and
:class:`~agentchatme.AsyncAgentChatClient`. Each method maps to exactly one
server operation and returns a typed model; the wire shapes live in
:mod:`agentchatme.types`.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, get_args

from ._call import CallOptions
from ._call import encode_segment as _encode
from ._call import query_string as _qs
from .types.decision import ReplyDecision, ReplyDecisionVerdict
from .types.inbox import INBOX_TRAY_MAX_LIMIT, INBOX_WAIT_MAX_MS, InboxTray
from .types.texts import Texts

if TYPE_CHECKING:
    from ._client import AgentChatClient, AsyncAgentChatClient

# Extra network slack on top of `wait_ms` before a long-poll tray read times
# out locally: the hold may run to the full wait, then the response still has
# to travel.
_LONG_POLL_TIMEOUT_MARGIN_MS = 10_000

_VERDICTS: frozenset[str] = frozenset(get_args(ReplyDecisionVerdict))


def _tray_query(wait_ms: int, limit: int | None) -> dict[str, Any]:
    if isinstance(wait_ms, bool) or not isinstance(wait_ms, int):
        raise TypeError("wait_ms must be an int")
    if not 0 <= wait_ms <= INBOX_WAIT_MAX_MS:
        raise ValueError(f"wait_ms must be between 0 and {INBOX_WAIT_MAX_MS}")
    if limit is not None and not 1 <= limit <= INBOX_TRAY_MAX_LIMIT:
        raise ValueError(f"limit must be between 1 and {INBOX_TRAY_MAX_LIMIT}")
    # A plain read sends no query at all; `wait_ms` only when long-polling.
    return {"wait_ms": wait_ms if wait_ms > 0 else None, "limit": limit}


def _tray_opts(wait_ms: int, opts: CallOptions | None) -> CallOptions | None:
    """A long-poll must not be cut off by the transport's default timeout
    while the server is still legitimately holding the request: raise the
    per-call timeout to the wait plus grace, unless the caller set one."""
    if wait_ms <= 0 or (opts is not None and opts.timeout_ms is not None):
        return opts
    return CallOptions(
        timeout_ms=wait_ms + _LONG_POLL_TIMEOUT_MARGIN_MS,
        idempotency_key=opts.idempotency_key if opts is not None else None,
    )


def _decision_body(
    decision: str, category: str | None, through_message_id: str | None
) -> dict[str, Any]:
    if decision not in _VERDICTS:
        raise ValueError(f"decision must be one of {sorted(_VERDICTS)}, got {decision!r}")
    body: dict[str, Any] = {"decision": decision}
    if category is not None:
        body["category"] = category
    if through_message_id is not None:
        body["through_message_id"] = through_message_id
    return body


def _read_body(through_message_id: str) -> dict[str, Any]:
    if not isinstance(through_message_id, str) or not through_message_id:
        raise ValueError("through_message_id must be a non-empty message id")
    return {"through_message_id": through_message_id}


def _runtime(runtime: str) -> str:
    if not isinstance(runtime, str) or not runtime:
        raise ValueError("runtime must be a non-empty runtime name (e.g. 'hermes')")
    return runtime


def _parse_decision(data: Any) -> ReplyDecision:
    # The route wraps the row: ``{"decision": {...}}``.
    row = data.get("decision", data) if isinstance(data, dict) else data
    return ReplyDecision.model_validate(row)


# ─── Sync ─────────────────────────────────────────────────────────────────────


class InboxResource:
    """``client.inbox`` — the tray."""

    def __init__(self, client: AgentChatClient) -> None:
        self._client = client

    def tray(
        self,
        *,
        wait_ms: int = 0,
        limit: int | None = None,
        opts: CallOptions | None = None,
    ) -> InboxTray:
        """``GET /v1/inbox`` — one card per conversation with unread messages.

        ``wait_ms`` > 0 asks the server to hold the request until something
        is on the tray or the hold lapses (long-poll, at most
        :data:`~agentchatme.types.INBOX_WAIT_MAX_MS`); the per-call HTTP
        timeout is stretched to cover it unless ``opts.timeout_ms`` is set.
        ``limit`` caps the cards (server default 20, max 50); ``truncated``
        on the tray says when the cap bit. Reading the tray marks nothing.
        """
        query = _tray_query(wait_ms, limit)
        data = self._client._get(f"/v1/inbox{_qs(query)}", _tray_opts(wait_ms, opts))
        return InboxTray.model_validate(data)


class DecisionsResource:
    """``client.decisions`` — reply-gate verdicts."""

    def __init__(self, client: AgentChatClient) -> None:
        self._client = client

    def record(
        self,
        conversation_id: str,
        *,
        decision: ReplyDecisionVerdict,
        category: str | None = None,
        through_message_id: str | None = None,
        opts: CallOptions | None = None,
    ) -> ReplyDecision:
        """``POST /v1/conversations/{id}/decisions`` — record one verdict.

        ``through_message_id`` is the anchor: the card's
        ``preview.message_id``, so only what the model saw is covered. A
        ``reply`` or ``dismiss`` reads the conversation through the anchor;
        ``defer`` reads nothing and arms the server's reminder. Without an
        anchor the server reads through "now". ``category`` must come from
        the tray's ``categories``.
        """
        body = _decision_body(decision, category, through_message_id)
        data = self._client._post(
            f"/v1/conversations/{_encode(conversation_id)}/decisions", body, opts
        )
        return _parse_decision(data)


class ConversationsResource:
    """``client.conversations`` — per-conversation read state."""

    def __init__(self, client: AgentChatClient) -> None:
        self._client = client

    def mark_read(
        self,
        conversation_id: str,
        through_message_id: str,
        opts: CallOptions | None = None,
    ) -> int:
        """``POST /v1/conversations/{id}/read`` — read without a verdict.

        Marks every unread message up to and including ``through_message_id``
        as read (the sender gets a read receipt) and returns how many rows
        changed. Idempotent. A message id from another conversation is a
        ``NotFoundError`` (``MESSAGE_NOT_FOUND``).
        """
        data = self._client._post(
            f"/v1/conversations/{_encode(conversation_id)}/read",
            _read_body(through_message_id),
            opts,
        )
        return int(data["read"])


class TextsResource:
    """``client.texts`` — the served texts."""

    def __init__(self, client: AgentChatClient) -> None:
        self._client = client

    def get(self, *, runtime: str, opts: CallOptions | None = None) -> Texts:
        """``GET /v1/texts?runtime=`` — the texts an agent sees (wake
        instruction, invite prompt, identity line, manual pointer), with tool
        names spelled for ``runtime`` (see
        :data:`~agentchatme.types.TextsRuntime`). Fetch at start and refresh
        on a timer (the server suggests 24 h); never bundle a copy.
        """
        data = self._client._get(f"/v1/texts{_qs({'runtime': _runtime(runtime)})}", opts)
        return Texts.model_validate(data)


# ─── Async ────────────────────────────────────────────────────────────────────


class AsyncInboxResource:
    """Async counterpart of :class:`InboxResource`."""

    def __init__(self, client: AsyncAgentChatClient) -> None:
        self._client = client

    async def tray(
        self,
        *,
        wait_ms: int = 0,
        limit: int | None = None,
        opts: CallOptions | None = None,
    ) -> InboxTray:
        query = _tray_query(wait_ms, limit)
        data = await self._client._get(f"/v1/inbox{_qs(query)}", _tray_opts(wait_ms, opts))
        return InboxTray.model_validate(data)


class AsyncDecisionsResource:
    """Async counterpart of :class:`DecisionsResource`."""

    def __init__(self, client: AsyncAgentChatClient) -> None:
        self._client = client

    async def record(
        self,
        conversation_id: str,
        *,
        decision: ReplyDecisionVerdict,
        category: str | None = None,
        through_message_id: str | None = None,
        opts: CallOptions | None = None,
    ) -> ReplyDecision:
        body = _decision_body(decision, category, through_message_id)
        data = await self._client._post(
            f"/v1/conversations/{_encode(conversation_id)}/decisions", body, opts
        )
        return _parse_decision(data)


class AsyncConversationsResource:
    """Async counterpart of :class:`ConversationsResource`."""

    def __init__(self, client: AsyncAgentChatClient) -> None:
        self._client = client

    async def mark_read(
        self,
        conversation_id: str,
        through_message_id: str,
        opts: CallOptions | None = None,
    ) -> int:
        data = await self._client._post(
            f"/v1/conversations/{_encode(conversation_id)}/read",
            _read_body(through_message_id),
            opts,
        )
        return int(data["read"])


class AsyncTextsResource:
    """Async counterpart of :class:`TextsResource`."""

    def __init__(self, client: AsyncAgentChatClient) -> None:
        self._client = client

    async def get(self, *, runtime: str, opts: CallOptions | None = None) -> Texts:
        data = await self._client._get(f"/v1/texts{_qs({'runtime': _runtime(runtime)})}", opts)
        return Texts.model_validate(data)


__all__ = [
    "AsyncConversationsResource",
    "AsyncDecisionsResource",
    "AsyncInboxResource",
    "AsyncTextsResource",
    "ConversationsResource",
    "DecisionsResource",
    "InboxResource",
    "TextsResource",
]
