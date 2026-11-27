"""The listener report, kept current (contract v1 §3.13): a resident listener
(a program that holds the agent's socket and starts its runs) tells
AgentChat when it cannot do its job, so the agent's ``reachability`` and its
person's dashboard say so instead of a socket that looks healthy while every
run fails, or a silence that looks like a closed laptop.

The integration says what is true (``set(state, reason_kind=None)``); the
reporter decides when to send it:

- on every change of state (or reason), about a second after the first
  change of a burst (``coalesce_ms``), so a burst is one report, of its latest;
- at most one attempt a minute (``min_interval_ms``), so a flapping state
  stays far under the server's 120 reports an hour;
- again every 30 minutes while the state is not ``ok`` (``resend_ms``): the
  server counts a report fresh for 45;
- a change back to what the server already holds before it was sent is no
  report at all;
- a failure is retried after 1 minute, doubling to 15 (``retry_ms``,
  ``retry_max_ms``), or after the wait the server names, at most an hour (a
  429's ``Retry-After``, a 429 / 503 body's ``retry_after_s``).

It never raises into the caller: a failure goes to ``on_error`` (once per new
failure, not once per retry; default: a warning on the ``agentchat.integration``
logger). It runs on an asyncio loop: call :meth:`ListenerReporter.set` and
:meth:`~ListenerReporter.stop` from the loop's thread, with an
:class:`~agentchatme.AsyncAgentChatClient` or an
:class:`~agentchatme.AgentChatClient` (a sync client's call runs in a worker
thread: a report under way when :meth:`~ListenerReporter.stop` is called
then still finishes there, within the client's timeout and retries). Report
from the process that holds the socket only (a standby reports nothing).
When the listener is switched off or uninstalled, report ``stopped`` before
it closes its socket: ``set("stopped")``, ``await flush()``, then ``stop()``,
then close the socket.

:func:`listener_instance_id` makes the install's id once and reuses it.

The TypeScript kit's ``ListenerReporter`` is the twin, with the same names in
TypeScript casing.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import re
import secrets
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from ..errors import AgentChatError, RateLimitedError
from ..types.reachability import LISTENER_REPORT_RESEND_MS, ListenerReasonKind, ListenerState
from ._files import create_once, write_atomic
from .coalescer import SetTimer, TimerHandle, _loop_timer

_log = logging.getLogger("agentchat.integration")

LISTENER_REPORT_COALESCE_MS = 1_000
"""How long after a change the report goes out, so a burst of changes is one report."""
LISTENER_REPORT_MIN_INTERVAL_MS = 60_000
"""The shortest time between two attempts (a change, a re-send or a retry)."""
LISTENER_REPORT_RETRY_MS = 60_000
"""The first retry after a failed attempt; doubles per failure up to
:data:`LISTENER_REPORT_RETRY_MAX_MS`."""
LISTENER_REPORT_RETRY_MAX_MS = 15 * 60_000

_SERVER_WAIT_MAX_MS = 60 * 60_000
"""The longest wait a server's answer can set (its report budget is counted per hour)."""

_STATES = frozenset({"ok", "runs_failing", "not_ready", "stopped"})
_REASONS = frozenset(
    {"not_signed_in", "runtime_missing", "runtime_outdated", "runtime_check_failed", "other"}
)
_INSTANCE_ID = re.compile(r"^[A-Za-z0-9_-]{8,64}$")


def is_listener_instance_id(value: object) -> bool:
    """Whether ``value`` is a listener instance id the server takes: 8 to 64
    of ``A-Z a-z 0-9 _ -``."""
    return isinstance(value, str) and _INSTANCE_ID.fullmatch(value) is not None


@dataclass(frozen=True)
class _Report:
    state: str
    reason_kind: str | None


@dataclass(frozen=True)
class AcceptedReport:
    """The last report the server accepted, and when (this machine's clock)."""

    state: str
    reason_kind: str | None
    at: str
    """ISO-8601 UTC."""


@dataclass(frozen=True)
class ListenerReporterStatus:
    """A snapshot for a status command."""

    instance_id: str
    state: str | None
    """What :meth:`ListenerReporter.set` last said; ``None`` before the first call."""
    reason_kind: str | None
    """Its reason: ``None`` for ``ok`` and ``stopped``, ``other`` when a
    failing state named none."""
    reported: AcceptedReport | None
    pending: bool
    """``True`` while the server does not hold what ``set()`` last said."""
    failures: int
    """Failed attempts since the last accepted report."""
    last_error: str | None
    """The last failure, cleared by the next accepted report."""
    next_attempt_at: str | None
    """When the next attempt is due (ISO-8601 UTC), or ``None`` when none is."""
    stopped: bool


def _reason_of(state: str, reason_kind: str | None) -> str | None:
    """A reason belongs to a state that is not ok; a failing state without
    one is ``other``, as the server records it."""
    if state in ("ok", "stopped"):
        return None
    return reason_kind or "other"


def _server_wait_ms(err: BaseException) -> float | None:
    """The wait the server named for this failure, in ms (at most an hour), or ``None``."""
    ms: float | None = None
    if isinstance(err, RateLimitedError) and err.retry_after_ms is not None:
        ms = float(err.retry_after_ms)
    elif (
        isinstance(err, AgentChatError)
        and err.status in (429, 503)
        and err.retry_after_s is not None
    ):
        ms = float(err.retry_after_s * 1000)
    return None if ms is None else min(float(_SERVER_WAIT_MAX_MS), max(0.0, ms))


def _failure_text(err: BaseException) -> str:
    if isinstance(err, AgentChatError):
        return f"{err.status} {err.code}: {err}"
    return str(err) or type(err).__name__


def _iso(epoch_ms: float) -> str:
    moment = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def _now_ms() -> float:
    return time.time() * 1000.0


async def _outcome(task: asyncio.Task[bool]) -> bool:
    """The send's answer, waited for without being cancelled by it: a send
    that :meth:`ListenerReporter.stop` cancelled (even before it started) is
    ``False``, never a ``CancelledError`` for the caller. Cancelling the
    caller still cancels the wait (not the send)."""
    await asyncio.wait({task})
    if task.cancelled():
        return False
    return task.result()


class ListenerReporter:
    """Keeps this listener's report current; see the module docstring.

    ``client`` is an :class:`~agentchatme.AsyncAgentChatClient` or an
    :class:`~agentchatme.AgentChatClient` (anything with
    ``report_listener``); ``instance_id`` this install's id
    (:func:`listener_instance_id`). ``now`` is the clock in epoch
    milliseconds; ``set_timer(delay_ms, fn)`` arms a timer (default: the
    running loop's ``call_later``).
    """

    def __init__(
        self,
        client: Any,
        *,
        instance_id: str,
        resend_ms: float = LISTENER_REPORT_RESEND_MS,
        coalesce_ms: float = LISTENER_REPORT_COALESCE_MS,
        min_interval_ms: float = LISTENER_REPORT_MIN_INTERVAL_MS,
        retry_ms: float = LISTENER_REPORT_RETRY_MS,
        retry_max_ms: float = LISTENER_REPORT_RETRY_MAX_MS,
        on_error: Callable[[Exception], Any] | None = None,
        now: Callable[[], float] | None = None,
        set_timer: SetTimer | None = None,
    ) -> None:
        if not is_listener_instance_id(instance_id):
            raise ValueError(
                "instance_id must be 8 to 64 of A-Z a-z 0-9 _ - (integration.listener_instance_id)"
            )
        self.instance_id = instance_id
        self._client = client
        self._resend_ms = resend_ms
        self._coalesce_ms = coalesce_ms
        self._min_interval_ms = min_interval_ms
        self._retry_ms = retry_ms
        self._retry_max_ms = max(retry_ms, retry_max_ms)
        self._on_error = on_error
        self._now = now or _now_ms
        self._set_timer: SetTimer = set_timer or _loop_timer

        self._desired: _Report | None = None
        self._changed_at = 0.0
        self._reported: _Report | None = None
        self._reported_at = 0.0
        self._in_flight: asyncio.Task[bool] | None = None
        self._in_flight_report: _Report | None = None
        self._timer: TimerHandle | None = None
        self._due_at: float | None = None
        self._last_attempt_at: float | None = None
        self._failures = 0
        self._retry_at = 0.0
        self._server_wait_until = 0.0
        self._last_error: str | None = None
        self._stopped = False

    # ─── what the integration calls ──────────────────────────────────────

    def set(self, state: ListenerState, reason_kind: ListenerReasonKind | None = None) -> None:
        """Say what is true now. A change of state (or of the reason, for
        ``runs_failing`` and ``not_ready``) is reported about a second later;
        the same state again is nothing. ``reason_kind`` is ignored for
        ``ok`` and ``stopped``. Never raises for a send; raises
        ``ValueError`` only for a state or reason that is not one of the
        contract's. After :meth:`stop` it does nothing."""
        if state not in _STATES:
            raise ValueError(f"ListenerReporter.set: unknown state {state!r}")
        if reason_kind is not None and reason_kind not in _REASONS:
            raise ValueError(f"ListenerReporter.set: unknown reason_kind {reason_kind!r}")
        if self._stopped:
            return
        report = _Report(state, _reason_of(state, reason_kind))
        if report == self._desired:
            # Nothing new; but if nothing is armed or under way (an earlier
            # set() found no running loop), make sure what is due is.
            if self._timer is None and self._in_flight is None:
                self._schedule()
            return
        # The wait runs from the first change the server has not heard of, so
        # a state that keeps changing is still reported, at its latest.
        if not self._pending():
            self._changed_at = self._now()
        self._desired = report
        self._schedule()

    async def flush(self) -> bool:
        """Send what ``set()`` last said now, without the coalescing delay or
        the once-a-minute spacing (a wait the server named is still kept).
        Returns ``True`` once the server holds it (also when it already did,
        or nothing was set), ``False`` when the attempt failed, the server
        asked to wait, or the reporter is stopped. Never raises."""
        while True:
            if self._stopped:
                return False
            if not self._pending():
                return True
            task, sent = self._in_flight, self._in_flight_report
            if task is None:
                if self._now() < self._server_wait_until:
                    return False
                task, sent = self._send(), self._desired
            accepted = await _outcome(task)
            # The send under way was for what set() still says: its answer is the answer.
            if sent == self._desired or self._stopped:
                return accepted

    def stop(self) -> None:
        """Stop: no more reports, its timer cancelled, a report under way
        abandoned. Idempotent."""
        if self._stopped:
            return
        self._stopped = True
        self._cancel_timer()
        if self._in_flight is not None:
            self._in_flight.cancel()

    def status(self) -> ListenerReporterStatus:
        reported = self._reported
        return ListenerReporterStatus(
            instance_id=self.instance_id,
            state=self._desired.state if self._desired is not None else None,
            reason_kind=self._desired.reason_kind if self._desired is not None else None,
            reported=None
            if reported is None
            else AcceptedReport(reported.state, reported.reason_kind, _iso(self._reported_at)),
            pending=self._pending(),
            failures=self._failures,
            last_error=self._last_error,
            next_attempt_at=None if self._due_at is None else _iso(self._due_at),
            stopped=self._stopped,
        )

    # ─── when to send ────────────────────────────────────────────────────

    def _pending(self) -> bool:
        """Whether the server does not (or will not, once the report under way
        lands) hold what ``set()`` said: a report in flight for another state
        counts, since it will replace what the server holds."""
        if self._desired is None:
            return False
        if self._in_flight is not None and self._in_flight_report != self._desired:
            return True
        return self._desired != self._reported

    def _next_due(self) -> float | None:
        """When the next attempt is due, or ``None`` when nothing is to be sent."""
        desired = self._desired
        if desired is None:
            return None
        if desired == self._reported:
            # The server holds it: refresh it while it is not ok, before it goes stale.
            if desired.state == "ok":
                return None
            due = self._reported_at + self._resend_ms
        else:
            due = self._changed_at + self._coalesce_ms
        due = max(due, self._retry_at)
        if self._last_attempt_at is not None:
            due = max(due, self._last_attempt_at + self._min_interval_ms)
        return due

    def _schedule(self) -> None:
        if self._stopped or self._in_flight is not None:
            return
        due = self._next_due()
        if due is None:
            self._cancel_timer()
            return
        if self._timer is not None and self._due_at == due:
            return
        self._cancel_timer()
        try:
            self._timer = self._set_timer(max(0.0, due - self._now()), self._fire)
        except RuntimeError as err:
            # No running loop in this thread: say so; the state stays pending.
            text = f"no running asyncio loop in this thread ({err})"
            self._tell(text, f"listener report: {text}; call set() from the event loop's thread")
            return
        self._due_at = due

    def _fire(self) -> None:
        self._timer = None
        self._due_at = None
        if self._stopped or self._in_flight is not None:
            return
        due = self._next_due()
        if due is None:
            return
        if due > self._now():
            self._schedule()
            return
        self._send()

    def _cancel_timer(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None
        self._due_at = None

    # ─── one attempt ─────────────────────────────────────────────────────

    def _send(self) -> asyncio.Task[bool]:
        """One attempt for what ``set()`` says now, on the running loop."""
        report = self._desired
        assert report is not None
        self._cancel_timer()
        self._last_attempt_at = self._now()
        task = asyncio.ensure_future(self._attempt(report))
        self._in_flight = task
        self._in_flight_report = report
        task.add_done_callback(self._settled)
        return task

    def _settled(self, task: asyncio.Task[bool]) -> None:
        if self._in_flight is task:
            self._in_flight = None
            self._in_flight_report = None
        self._schedule()

    async def _attempt(self, report: _Report) -> bool:
        kwargs: dict[str, Any] = {"instance_id": self.instance_id, "state": report.state}
        if report.reason_kind is not None:
            kwargs["reason_kind"] = report.reason_kind
        try:
            call = self._client.report_listener
            if inspect.iscoroutinefunction(call):
                await call(**kwargs)
            else:
                answer = await asyncio.to_thread(call, **kwargs)
                if inspect.isawaitable(answer):
                    await answer
        except asyncio.CancelledError:
            # Abandoned by stop(): nothing to tell.
            if self._stopped:
                return False
            raise
        except Exception as err:
            if self._stopped:
                return False
            self._failures += 1
            wait = _server_wait_ms(err)
            backoff = min(self._retry_max_ms, self._retry_ms * 2 ** min(self._failures - 1, 20))
            now = self._now()
            self._retry_at = now + (wait if wait is not None else backoff)
            self._server_wait_until = now + wait if wait is not None else 0.0
            text = _failure_text(err)
            self._tell(text, f"listener report ({report.state}): {text}; trying again later")
            return False
        self._reported = report
        self._reported_at = self._now()
        self._failures = 0
        self._retry_at = 0.0
        self._server_wait_until = 0.0
        self._last_error = None
        return True

    def _tell(self, text: str, message: str) -> None:
        """Report a failure once per new condition (``text``), not once per retry."""
        if text == self._last_error:
            return
        self._last_error = text
        self._report(RuntimeError(message))

    def _report(self, err: Exception) -> None:
        if self._on_error is None:
            _log.warning("%s", err)
            return
        try:
            self._on_error(err)
        except Exception:
            _log.debug("listener report: on_error raised", exc_info=True)


# ─── The install's id ──────────────────────────────────────────────────────


def _read_instance_id(path: Path) -> str | None:
    try:
        held = path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeDecodeError):
        return None
    return held if is_listener_instance_id(held) else None


def listener_instance_id(path: str | Path) -> str:
    """This install's listener id, kept in ``path`` (one line, mode 0600):
    read when the file holds one, else made (22 random characters of ``A-Z
    a-z 0-9 _ -``) and written once. Of two processes that start together,
    both get the id that was written first. A file that holds no valid id
    (damaged, edited) is replaced by a new one. Keep ``path`` with the
    install (beside its credentials, say); a new id is a new install to the
    server. Raises :class:`OSError` when the file can be neither read nor
    written."""
    path = Path(path)
    held = _read_instance_id(path)
    if held is not None:
        return held
    new_id = secrets.token_urlsafe(16)
    if create_once(path, f"{new_id}\n", mode=0o600):
        return new_id
    # Another process wrote first (or a file without a valid id is in the way).
    theirs = _read_instance_id(path)
    if theirs is not None:
        return theirs
    write_atomic(path, f"{new_id}\n", mode=0o600)
    return new_id


__all__ = [
    "LISTENER_REPORT_COALESCE_MS",
    "LISTENER_REPORT_MIN_INTERVAL_MS",
    "LISTENER_REPORT_RETRY_MAX_MS",
    "LISTENER_REPORT_RETRY_MS",
    "AcceptedReport",
    "ListenerReporter",
    "ListenerReporterStatus",
    "is_listener_instance_id",
    "listener_instance_id",
]
