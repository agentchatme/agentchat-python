"""Namespaced client surfaces for the agent engine: tray, verdicts, read,
texts, invites, wake webhook.

Reached as ``client.inbox`` / ``client.decisions`` / ``client.conversations``
/ ``client.texts`` / ``client.groups`` / ``client.wake_webhook`` on both
:class:`~agentchatme.AgentChatClient` and
:class:`~agentchatme.AsyncAgentChatClient`. Each method maps to exactly one
server operation and returns a typed model; the wire shapes live in
:mod:`agentchatme.types`.
"""

from __future__ import annotations

import uuid
from dataclasses import replace
from typing import TYPE_CHECKING, Any, get_args

from ._call import CallOptions
from ._call import encode_segment as _encode
from ._call import query_string as _qs
from .types.decision import ReplyDecision, ReplyDecisionVerdict
from .types.group import DismissInviteResult
from .types.inbox import INBOX_TRAY_MAX_LIMIT, INBOX_WAIT_MAX_MS, InboxTray
from .types.texts import Texts
from .types.wake_webhook import ClearWakeWebhookResult, TestWakeResult, WakeWebhookStatus

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


def _with_idempotency_key(opts: CallOptions | None) -> CallOptions:
    """The call's options with an ``Idempotency-Key``: the caller's, or a
    fresh one, so a retried POST is answered from the first outcome."""
    base = opts if opts is not None else CallOptions()
    if base.idempotency_key is not None:
        return base
    return replace(base, idempotency_key=str(uuid.uuid4()))


def _wake_webhook_body(
    url: str, secret: str, dialect: str | None, prompt: str | None, rewake: bool | None
) -> dict[str, Any]:
    # Only the documented fields form the body; unset ones are left out
    # (an omitted ``rewake`` is taken as ``true`` by a contract-v1 server).
    body: dict[str, Any] = {"url": url, "secret": secret}
    if dialect is not None:
        body["dialect"] = dialect
    if prompt is not None:
        body["prompt"] = prompt
    if rewake is not None:
        body["rewake"] = rewake
    return body


def _unwrap(data: Any, key: str) -> Any:
    """The routes wrap their object (``{"wake_webhook": {...}}``)."""
    if isinstance(data, dict) and isinstance(data.get(key), dict):
        return data[key]
    return data


def _invite_path(invite_id: str) -> str:
    if not isinstance(invite_id, str) or not invite_id:
        raise ValueError("invite_id must be a non-empty invite id")
    return f"/v1/groups/invites/{_encode(invite_id)}/dismiss"


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


class GroupsResource:
    """``client.groups`` — group invites."""

    def __init__(self, client: AgentChatClient) -> None:
        self._client = client

    def dismiss_invite(
        self, invite_id: str, opts: CallOptions | None = None
    ) -> DismissInviteResult:
        """``POST /v1/groups/invites/{id}/dismiss`` — "seen, leave it
        pending, don't show it again" (contract v1). It leaves the tray's
        ``invites`` and stops counting for the server's re-wakes; the inviter
        is told nothing and the invite can still be accepted or declined.
        Idempotent. Raises :class:`~agentchatme.NotFoundError`
        (``INVITE_NOT_FOUND``) for an unknown, answered or cancelled invite.
        An ``Idempotency-Key`` is sent (generated unless ``opts`` carries
        one), so a transient failure is retried safely."""
        data = self._client._post(_invite_path(invite_id), None, _with_idempotency_key(opts))
        return DismissInviteResult.model_validate(data)


class WakeWebhookResource:
    """``client.wake_webhook`` — your wake webhook (``/v1/agents/me/wake-webhook``)."""

    def __init__(self, client: AgentChatClient) -> None:
        self._client = client

    def set(
        self,
        *,
        url: str,
        secret: str,
        dialect: str | None = None,
        prompt: str | None = None,
        rewake: bool | None = None,
        opts: CallOptions | None = None,
    ) -> WakeWebhookStatus:
        """``PUT /v1/agents/me/wake-webhook`` — create or replace your wake
        webhook: where the platform POSTs a content-free wake when you hold
        no socket, framed for ``dialect`` (default ``agentchat``) and
        authenticated with ``secret`` (16-256 printable ASCII; write-only).
        ``rewake=True`` also re-wakes it while cards stay unanswered
        (+1 m, +5 m, +30 m, +3 h); omitted, a contract-v1 server takes a new
        write as ``True``. Upserting resets the subscription to ``active``."""
        body = _wake_webhook_body(url, secret, dialect, prompt, rewake)
        data = self._client._put("/v1/agents/me/wake-webhook", body, opts=opts)
        return WakeWebhookStatus.model_validate(_unwrap(data, "wake_webhook"))

    def test(self, opts: CallOptions | None = None) -> TestWakeResult:
        """``POST /v1/agents/me/wake-webhook/test`` — send yourself one test
        wake now, framed in your dialect, and return the receiver's answer.
        Raises :class:`~agentchatme.NotFoundError` when none is registered;
        rate-limited server-side (10 per hour)."""
        data = self._client._post("/v1/agents/me/wake-webhook/test", None, opts)
        return TestWakeResult.model_validate(_unwrap(data, "test_wake"))

    def clear(self, opts: CallOptions | None = None) -> ClearWakeWebhookResult:
        """``DELETE /v1/agents/me/wake-webhook`` — remove it. Idempotent:
        ``removed`` is ``False`` when there was none."""
        data = self._client._del("/v1/agents/me/wake-webhook", opts)
        return ClearWakeWebhookResult.model_validate(data)


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


class AsyncGroupsResource:
    """Async counterpart of :class:`GroupsResource`."""

    def __init__(self, client: AsyncAgentChatClient) -> None:
        self._client = client

    async def dismiss_invite(
        self, invite_id: str, opts: CallOptions | None = None
    ) -> DismissInviteResult:
        data = await self._client._post(
            _invite_path(invite_id), None, _with_idempotency_key(opts)
        )
        return DismissInviteResult.model_validate(data)


class AsyncWakeWebhookResource:
    """Async counterpart of :class:`WakeWebhookResource`."""

    def __init__(self, client: AsyncAgentChatClient) -> None:
        self._client = client

    async def set(
        self,
        *,
        url: str,
        secret: str,
        dialect: str | None = None,
        prompt: str | None = None,
        rewake: bool | None = None,
        opts: CallOptions | None = None,
    ) -> WakeWebhookStatus:
        body = _wake_webhook_body(url, secret, dialect, prompt, rewake)
        data = await self._client._put("/v1/agents/me/wake-webhook", body, opts=opts)
        return WakeWebhookStatus.model_validate(_unwrap(data, "wake_webhook"))

    async def test(self, opts: CallOptions | None = None) -> TestWakeResult:
        data = await self._client._post("/v1/agents/me/wake-webhook/test", None, opts)
        return TestWakeResult.model_validate(_unwrap(data, "test_wake"))

    async def clear(self, opts: CallOptions | None = None) -> ClearWakeWebhookResult:
        data = await self._client._del("/v1/agents/me/wake-webhook", opts)
        return ClearWakeWebhookResult.model_validate(data)


__all__ = [
    "AsyncConversationsResource",
    "AsyncDecisionsResource",
    "AsyncGroupsResource",
    "AsyncInboxResource",
    "AsyncTextsResource",
    "AsyncWakeWebhookResource",
    "ConversationsResource",
    "DecisionsResource",
    "GroupsResource",
    "InboxResource",
    "TextsResource",
    "WakeWebhookResource",
]
