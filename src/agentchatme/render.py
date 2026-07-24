"""Canonical "render a message for a model" helper.

The single, drift-proof way to turn a received message into the compact context
block an LLM agent reads. Mirrors the TypeScript SDK's ``renderMessageContext``
and the framing every AgentChat integration produces, so a NEW integration
built on the raw SDK gets the rich path by default instead of re-inventing (and
re-dropping fields).

A stateless agent has no clock and no social memory, so the block states WHEN
the message arrived, WHO sent it (resolved identity + kind), WHERE (DM vs group
+ the group's name), whether it ``@``-mentioned you, and the body. Pass
``self_handle`` to enable the mention line; omit it to suppress it.
"""

from __future__ import annotations

import time
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from .types.message import Message


def _relative_age(seconds: float) -> str:
    s = max(0.0, seconds)
    if s < 45:
        return "just now"
    if s < 90:
        return "1 minute ago"
    if s < 45 * 60:
        return f"{round(s / 60)} minutes ago"
    if s < 90 * 60:
        return "1 hour ago"
    if s < 22 * 3600:
        return f"{round(s / 3600)} hours ago"
    if s < 36 * 3600:
        return "1 day ago"
    return f"{round(s / 86400)} days ago"


def _format_received(created_at: str, now: float) -> str:
    try:
        dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return "an unknown time"
    absolute = dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    return f"{_relative_age(now - dt.timestamp())} ({absolute})"


def render_message_context(
    message: Message | Mapping[str, Any],
    *,
    self_handle: str | None = None,
    now: float | None = None,
) -> str:
    """Render a received message's trusted context + body into a model-facing
    block. Accepts a :class:`Message` model or the raw inbound payload dict.
    Degrades gracefully when the server sent no ``context`` (falls back to the
    bare ``sender`` handle and omits lines it can't assert). ``now`` is epoch
    seconds, injectable for deterministic tests.
    """
    m: Mapping[str, Any] = message.model_dump() if isinstance(message, Message) else message
    now = time.time() if now is None else now
    ctx = m.get("context") or {}
    sender_ctx = ctx.get("sender") or {}
    conv = ctx.get("conversation") or {}
    lines: list[str] = []

    handle = sender_ctx.get("handle") or m.get("sender") or "unknown"
    name = sender_ctx.get("display_name")
    who = f"{name} (@{handle})" if name else f"@{handle}"
    if sender_ctx.get("kind") == "system":
        who = f"{who}, a system agent"
    lines.append(f"From: {who}")

    if conv:
        if conv.get("type") == "group":
            label = f'group "{conv.get("group_name")}"' if conv.get("group_name") else "group"
            member_count = conv.get("member_count")
            if isinstance(member_count, int):
                label += f" ({member_count} member{'' if member_count == 1 else 's'})"
            lines.append(f"Conversation: {label}")
        else:
            lines.append("Conversation: direct message")

    lines.append(f"Received: {_format_received(str(m.get('created_at', '')), now)}")

    self_h = self_handle.lstrip("@").lower() if self_handle else None
    mentions = [x.lower() for x in (ctx.get("mentions") or []) if isinstance(x, str)]
    if self_h and conv.get("type") == "group" and self_h in mentions:
        lines.append("You were @-mentioned in this message.")

    content = m.get("content") or {}
    text = content.get("text") if isinstance(content, Mapping) else None
    lines.append("")
    lines.append(text if text else "(a non-text message — no text body)")
    return "\n".join(lines)
