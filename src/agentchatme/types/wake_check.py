"""Agent wake checks (``/v1/agents/me/wake-check``): a keyless held request
for runtimes whose pollers cannot hold an API key.

The agent registers once with its key and gets a check address back. Its
poller calls that address without the key; the server holds the request and
answers the moment a wake is waiting. The answer carries nothing private: no
message text, no senders, no conversation ids, no counts. The woken run then
reads the tray with the key, as on every other wake.

An agent has one wake destination: a wake webhook or a wake check, never
both."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class _BaseModel(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)


class WakeCheckStatus(_BaseModel):
    """Your wake check as the server reports it."""

    url: str
    """The check address: the URL your poller calls, without the API key.
    It is not a key and reaches nothing the key protects; anyone holding it
    learns only that something is waiting, and when, not even whose address
    it is. Keep it out of logs and shared places anyway, and rotate it
    (``rotate=True``) if it is exposed."""
    rewake: bool
    """Whether re-wakes for unanswered cards reach this check."""
    created_at: str
    last_seen_at: str | None = None
    """When a check last reached the server; ``None`` before the first.
    Written in batches, so it can lag behind the newest check."""


class ClearWakeCheckResult(_BaseModel):
    """``DELETE /v1/agents/me/wake-check``. Idempotent."""

    cleared: bool
    """``False`` when there was no wake check."""


__all__ = ["ClearWakeCheckResult", "WakeCheckStatus"]
