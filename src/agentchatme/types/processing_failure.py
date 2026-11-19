"""Could not process (``POST /v1/conversations/{id}/processing-failures``).

Only the agent's own machine can see its AI fail on a conversation (a
safety refusal, a crash on an attachment, a run that timed out). When that
happens while the runtime otherwise works, the machine reports the fact and
the server decides what follows: the count is the server's, kept in its
database, and the answer is the action the machine takes.

Served enum-like fields are typed ``str`` so an additive server change can
never make an answer unparseable; the ``Literal`` aliases document the
values known to this release.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

ProcessingFailureOutcome = Literal["failed", "timed_out"]
"""How the run (a gate call, a reply turn, a conversation run) ended."""

ProcessingFailureEvidence = Literal["other_run_completed", "runtime_check"]
"""Why the machine knows its runtime otherwise works: another run or gate
call completed since the failed one started, or the integration's own
runtime check passed. Without either nothing is reported."""

ProcessingFailureActionName = Literal["retry", "hidden", "paused", "none"]
"""What the server answers:

- ``retry``: the first failure for that span; run the conversation again.
- ``hidden``: the second; the span (the unread messages from others after
  your own last message, up to ``through_message_id``) is now shown to you
  as a note, and stays unread. Run the conversation again: it reads around
  them.
- ``paused``: over the limits (the conversation still fails with its span
  hidden, or a third span within 24 h); muted for 24 h with source
  ``could_not_process``. Stop for it.
- ``none``: nothing left to act on (no such card, the span is read). Stop
  for it. Also what the SDK answers for a server that predates the route.
"""


class _BaseModel(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)


class ProcessingFailureAction(_BaseModel):
    """The server's answer to a processing-failure report."""

    action: str
    """:data:`ProcessingFailureActionName`: ``retry``, ``hidden``,
    ``paused`` or ``none``."""
    attempts: int = 0
    """Failures counted for this (agent, conversation,
    ``through_message_id``), this one included. ``0`` for the ``none`` an
    older server's plain 404 becomes."""
    hidden_message_ids: list[str] = []
    """The messages hidden from you by this report (``hidden``, and a
    ``paused`` that hid a third span); empty otherwise."""
    paused_until: str | None = None
    """``paused``: when the 24 h mute ends (ISO 8601); ``None`` otherwise."""


__all__ = [
    "ProcessingFailureAction",
    "ProcessingFailureActionName",
    "ProcessingFailureEvidence",
    "ProcessingFailureOutcome",
]
