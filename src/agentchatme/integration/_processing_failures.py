"""Could not process, the machine's side: which failed runs the receive loop
may report, and the evidence that the runtime otherwise works.

Only the agent's machine sees its AI fail on a conversation. A failed or
timed-out run (a single-card gate call, a reply turn, a conversation run) is
reported only with evidence that the runtime otherwise works: work of the
same kind for another conversation in this loop completed after the failed
one started (``other_run_completed``: a gate call for a gate call, a turn
for a turn, a run for a run, since each kind takes its own path through the
runtime), or the integration's own runtime check passed (``runtime_check``)
while this conversation was the only one of its kind failing since that
kind last completed (several failing together is how a broken runtime
looks). The check must be a real, minimal model call through the same path
as the work, never a login or version check. Without evidence nothing
is reported: a broken runtime (logged out, out of credits) fails every run,
and that is the ``runs_failing`` path, never a reason to hide anyone's
messages.

The ledger is in memory and orders work by a counter, never by a clock: a
run that started before another one completed is known exactly. The loop's
clock bounds only how stale evidence may be (:data:`EVIDENCE_WINDOW_MS`), so
a runtime that was broken and is fixed later cannot turn its old failures
into reports.
"""

from __future__ import annotations

from collections import OrderedDict, deque
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Callable, Literal

EVIDENCE_WINDOW_MS = 10 * 60_000
"""How soon after a failed run ended the evidence must come: the run that
completed must have started within this window, or the runtime check run
within it (10 min: one run may take its whole run budget). Older failures
are not reported."""
MAX_FAILURE_REPORTS = 3
"""At most this many reports for one conversation in one cycle (its failed
run and the runs started again after ``retry`` / ``hidden``). The server's
answers end the chain sooner (``retry``, ``hidden``, then ``paused``)."""
_PARKED_LIFETIME_MS = EVIDENCE_WINDOW_MS + 30 * 60_000
"""A failure waiting for evidence is kept this long: evidence may be a run
that started within the window and completes up to a run budget later."""
_PARKED_MEMORY = 1_000
_COMPLETIONS_MEMORY = 256

FailureKind = Literal["gate", "turn", "run"]
"""``gate``: a single-card gate call; ``turn``: a reply turn (gate-call
mode); ``run``: a conversation run (single-run mode)."""


@dataclass(frozen=True)
class Work:
    """One piece of work as the ledger saw it start: its place in the order
    and the loop's clock then."""

    seq: int
    at: float


@dataclass
class Failure:
    """A run for one conversation that failed or timed out while the card
    it started from was still owed."""

    kind: FailureKind
    conversation_id: str
    through_message_id: str
    """The anchor of the card the failed run started from."""
    outcome: Literal["failed", "timed_out"]
    work: Work
    ended_at: float
    card: dict[str, Any]
    category: str | None = None
    """``turn``: the gate's category for the reply."""
    chain: int = 0
    """Reports already made for this conversation in this cycle (its
    earlier runs): a run started again after ``retry`` / ``hidden`` is one
    more link."""
    runtime_checked: bool = False
    """A runtime check ran for it and did not pass: only a completed run
    can be its evidence from now on."""
    detail: str | None = None


@dataclass(frozen=True)
class _Completion:
    seq: int
    """The counter when it completed."""
    started_at: float
    kind: FailureKind
    keys: frozenset[str]
    """What it covered: conversation ids, ``invite:<id>`` for an invite."""


class Ledger:
    """Work started and completed in one loop, the conversations failing
    since the last completion of each kind, and the failures waiting for
    evidence (at most one per conversation and kind: the newest)."""

    def __init__(self, now: Callable[[], float]) -> None:
        self._now = now
        self._seq = 0
        self._completions: deque[_Completion] = deque(maxlen=_COMPLETIONS_MEMORY)
        self._parked: OrderedDict[str, Failure] = OrderedDict()
        self._failing: dict[FailureKind, set[str]] = {"gate": set(), "turn": set(), "run": set()}

    def begin(self) -> Work:
        self._seq += 1
        return Work(seq=self._seq, at=self._now())

    def completed(self, work: Work, kind: FailureKind, keys: Iterable[str]) -> None:
        """Work of ``kind`` that completed (the runtime worked through it)."""
        self._seq += 1
        self._completions.append(
            _Completion(seq=self._seq, started_at=work.at, kind=kind, keys=frozenset(keys))
        )
        self._failing[kind].clear()

    def failed(self, failure: Failure) -> None:
        """A failed run, seen: its conversation is failing until work of
        its kind completes again."""
        self._failing[failure.kind].add(failure.conversation_id)

    def isolated(self, failure: Failure) -> bool:
        """No other conversation's work of the same kind has failed since
        that kind last completed: a runtime check may then speak for the
        failure."""
        return self._failing[failure.kind] <= {failure.conversation_id}

    def has_evidence(self, failure: Failure) -> bool:
        """Another conversation's work of the same kind completed after the
        failed run started, and that work started no later than the window
        after the failed run ended."""
        for done in reversed(self._completions):
            if done.seq <= failure.work.seq:
                return False
            if (
                done.kind == failure.kind
                and done.keys - {failure.conversation_id}
                and done.started_at - failure.ended_at <= EVIDENCE_WINDOW_MS
            ):
                return True
        return False

    def fresh_for_check(self, failure: Failure) -> bool:
        """Whether a runtime check run now may speak for the failure: not
        checked before, still within the window, and isolated."""
        return (
            not failure.runtime_checked
            and self._now() - failure.ended_at <= EVIDENCE_WINDOW_MS
            and self.isolated(failure)
        )

    def park(self, failure: Failure) -> bool:
        """Keep a failure until evidence comes. ``True`` when it is new (no
        earlier one for its conversation and kind was waiting)."""
        key = f"{failure.kind}:{failure.conversation_id}"
        new = key not in self._parked
        self._parked.pop(key, None)
        self._parked[key] = failure
        while len(self._parked) > _PARKED_MEMORY:
            self._parked.popitem(last=False)
        return new

    def take(self, kinds: Iterable[FailureKind]) -> list[Failure]:
        """The waiting failures of these kinds, oldest first, removed (a
        caller parks again what still has no evidence). Expired ones are
        dropped."""
        wanted = set(kinds)
        now = self._now()
        out: list[Failure] = []
        for key in list(self._parked):
            failure = self._parked[key]
            if now - failure.ended_at > _PARKED_LIFETIME_MS:
                del self._parked[key]
            elif failure.kind in wanted:
                del self._parked[key]
                out.append(failure)
        return out

    def clear(self, conversation_id: str, kinds: Iterable[FailureKind]) -> None:
        """Later work of these kinds for the conversation ended (completed,
        or failed and is the newest failure now): its waiting failures say
        nothing any more."""
        for kind in kinds:
            self._parked.pop(f"{kind}:{conversation_id}", None)

    @property
    def parked(self) -> int:
        return len(self._parked)


def newest_per_conversation(failures: Iterable[Failure]) -> list[Failure]:
    """One failure per conversation, the newest (by when it started), in
    the order the survivors started: a burst of failures for one
    conversation is reported once per evaluation, so a runtime that was
    broken for a while cannot hide a span on its own."""
    newest: dict[str, Failure] = {}
    for failure in failures:
        held = newest.get(failure.conversation_id)
        if held is None or failure.work.seq > held.work.seq:
            newest[failure.conversation_id] = failure
    return sorted(newest.values(), key=lambda failure: failure.work.seq)


def runtime_ok(answer: Any) -> bool:
    """What a runtime check answered: ``True``, or anything whose ``ok`` /
    ``ready`` is true (a :class:`~agentchatme.integration.Readiness`, a
    mapping)."""
    if isinstance(answer, bool):
        return answer
    for name in ("ok", "ready"):
        value = getattr(answer, name, None)
        if value is None and isinstance(answer, Mapping):
            value = answer.get(name)
        if value is not None:
            return value is True
    return False


__all__ = [
    "EVIDENCE_WINDOW_MS",
    "MAX_FAILURE_REPORTS",
    "Failure",
    "FailureKind",
    "Ledger",
    "Work",
    "newest_per_conversation",
    "runtime_ok",
]
