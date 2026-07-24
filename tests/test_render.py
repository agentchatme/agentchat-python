"""Tests for the canonical message renderer."""

from __future__ import annotations

from datetime import datetime, timezone

from agentchatme import render_message_context

# 2026-07-24 15:00 UTC as epoch seconds, for deterministic relative time.
NOW = datetime(2026, 7, 24, 15, 0, tzinfo=timezone.utc).timestamp()


def _msg(**over: object) -> dict[str, object]:
    base: dict[str, object] = {
        "id": "msg_1",
        "conversation_id": "grp_ops",
        "sender": "bob",
        "content": {"text": "ship it?"},
        "created_at": "2026-07-24T14:57:00Z",
    }
    base.update(over)
    return base


def test_renders_identity_room_time_mention_and_body() -> None:
    out = render_message_context(
        _msg(
            context={
                "sender": {"handle": "bob", "display_name": "Bob Builder", "kind": "agent"},
                "conversation": {"type": "group", "group_name": "Ops", "member_count": 5},
                "mentions": ["me"],
            }
        ),
        self_handle="@me",
        now=NOW,
    )
    assert "From: Bob Builder (@bob)" in out
    assert 'Conversation: group "Ops" (5 members)' in out
    assert "Received: 3 minutes ago (2026-07-24 14:57 UTC)" in out
    assert "You were @-mentioned in this message." in out
    assert "ship it?" in out


def test_flags_system_sender_and_omits_mention_when_not_named() -> None:
    out = render_message_context(
        _msg(
            context={
                "sender": {"handle": "chatfather", "display_name": "Chatfather", "kind": "system"},
                "conversation": {"type": "group", "group_name": "Ops", "member_count": 5},
                "mentions": ["someone-else"],
            }
        ),
        self_handle="me",
        now=NOW,
    )
    assert "From: Chatfather (@chatfather), a system agent" in out
    assert "@-mentioned" not in out


def test_degrades_without_context_block() -> None:
    out = render_message_context(_msg(), now=NOW)
    assert "From: @bob" in out
    assert "Conversation:" not in out
    assert "ship it?" in out


def test_accepts_a_message_model() -> None:
    from agentchatme.types import Message

    model = Message.model_validate(
        _msg(
            client_msg_id="c1",
            seq=1,
            type="text",
            metadata={},
            status="stored",
            context={
                "sender": {"handle": "bob", "display_name": "Bob", "kind": "agent"},
                "conversation": {"type": "direct", "group_name": None, "member_count": None},
                "mentions": [],
            },
        )
    )
    out = render_message_context(model, now=NOW)
    assert "From: Bob (@bob)" in out
    assert "Conversation: direct message" in out
