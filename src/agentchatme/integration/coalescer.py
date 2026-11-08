"""Wake coalescing: one cycle per burst, never one per frame.

A wake is a hint that the tray may have changed, never the carrier of what
changed: a ``message.new`` frame, a ``wake`` frame, a (re)connect, a webhook
the integration received. A group waking up arrives as many frames seconds
apart; whoever runs the cycle reads the whole tray, so nothing a later frame
announced is missed. The tray is the truth, the wake is not.

Timing (conformance ``coalescer.cases.json``):

- The window is trailing-edge: every wake moves the start to ``quiet_ms``
  after it, but never past ``max_ms`` after the burst's first wake, so a
  steady trickle cannot starve the cycle.
- One cycle at a time. A wake while a cycle runs earns exactly one more
  cycle after it, however many arrive: due ``quiet_ms`` after the last of
  them, capped at ``max_ms`` after the first, never before the cycle ended.

There is no delayed or retry wake here: the server re-wakes unanswered cards
on its own durable ladder (contract v1, D3), so an integration keeps no
retry ladder of its own.

Where the copies this replaces differed (Hermes ``wake.py``, OpenClaw and
agent-core ``integration/wake.ts``): the trailing window with a cap is kept
over the fixed window Hermes used; the "one more cycle" rule each invoker
wrote is kept here, in one place; ``wake_after`` (the local retry ladder) is
gone.

Runs on an asyncio loop: :meth:`WakeCoalescer.wake` must be called from the
loop's thread (from another thread, use ``loop.call_soon_threadsafe``).
``on_cycle`` may be a plain function (the cycle ends when it returns) or
return an awaitable (the cycle ends when it settles, either way).
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Literal, Protocol

DEFAULT_QUIET_MS = 2_000
DEFAULT_MAX_MS = 6_000

CoalescerState = Literal["idle", "waiting", "running", "stopped"]

_log = logging.getLogger("agentchat.integration")


class TimerHandle(Protocol):
    def cancel(self) -> object: ...


SetTimer = Callable[[float, Callable[[], None]], TimerHandle]
"""``set_timer(delay_ms, fn) -> handle``; ``handle.cancel()`` cancels it."""


@dataclass(frozen=True)
class CoalescerCounters:
    wakes: int
    """Every :meth:`WakeCoalescer.wake` accepted (not after stop)."""
    absorbed: int
    """Wakes that joined a window or a running cycle instead of starting one."""
    cycles: int
    """Cycles started."""


def _loop_timer(delay_ms: float, fn: Callable[[], None]) -> TimerHandle:
    return asyncio.get_running_loop().call_later(max(0.0, delay_ms) / 1000.0, fn)


def _monotonic_ms() -> float:
    return time.monotonic() * 1000.0


class WakeCoalescer:
    """Debounce wakes into cycles; see the module docstring for the timing."""

    def __init__(
        self,
        on_cycle: Callable[[], Any],
        *,
        quiet_ms: float = DEFAULT_QUIET_MS,
        max_ms: float = DEFAULT_MAX_MS,
        on_error: Callable[[BaseException], None] | None = None,
        now: Callable[[], float] | None = None,
        set_timer: SetTimer | None = None,
    ) -> None:
        if not isinstance(quiet_ms, (int, float)) or quiet_ms < 0:
            raise ValueError("quiet_ms must be a number >= 0")
        if not isinstance(max_ms, (int, float)) or max_ms < quiet_ms:
            raise ValueError("max_ms must be a number >= quiet_ms")
        self._on_cycle = on_cycle
        self._on_error = on_error
        self._quiet_ms = float(quiet_ms)
        self._max_ms = float(max_ms)
        self._now = now or _monotonic_ms
        self._set_timer: SetTimer = set_timer or _loop_timer

        self._timer: TimerHandle | None = None
        # First wake of the burst the armed timer belongs to.
        self._burst_started_at: float | None = None
        self._running = False
        # Wakes that arrived while the cycle ran: first and last.
        self._pending_first_at: float | None = None
        self._pending_last_at: float | None = None
        self._stopped = False
        self._current: asyncio.Future[Any] | None = None

        self._wakes = 0
        self._absorbed = 0
        self._cycles = 0

    # ─── state ───────────────────────────────────────────────────────────

    @property
    def state(self) -> CoalescerState:
        if self._stopped:
            return "stopped"
        if self._running:
            return "running"
        return "waiting" if self._timer is not None else "idle"

    @property
    def pending(self) -> bool:
        """``True`` while a wake that arrived during the running cycle waits
        for it to end."""
        return self._pending_first_at is not None

    def counters(self) -> CoalescerCounters:
        return CoalescerCounters(wakes=self._wakes, absorbed=self._absorbed, cycles=self._cycles)

    # ─── wakes ───────────────────────────────────────────────────────────

    def wake(self) -> None:
        """A wake. Starts a window, joins the open one, or earns one more
        cycle after the running one."""
        if self._stopped:
            return
        self._wakes += 1
        now = self._now()
        if self._running:
            if self._pending_first_at is None:
                self._pending_first_at = now
            else:
                self._absorbed += 1
            self._pending_last_at = now
            return
        if self._timer is None:
            self._burst_started_at = now
            self._arm(self._quiet_ms)
            return
        self._absorbed += 1
        started = self._burst_started_at if self._burst_started_at is not None else now
        cap_at = started + self._max_ms
        self._arm(max(0.0, min(now + self._quiet_ms, cap_at) - now))

    def stop(self) -> None:
        """Cancel the window and drop pending wakes; a running cycle finishes
        on its own. Later wakes are ignored."""
        self._stopped = True
        self._clear_timer()
        self._pending_first_at = None
        self._pending_last_at = None

    async def idle(self) -> None:
        """Return once the running cycle (if any) has ended."""
        current = self._current
        if current is not None and not current.done():
            await asyncio.wait({current})

    # ─── internals ───────────────────────────────────────────────────────

    def _arm(self, delay_ms: float) -> None:
        self._clear_timer()
        self._timer = self._set_timer(delay_ms, self._fire)

    def _clear_timer(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def _fire(self) -> None:
        self._timer = None
        self._start_cycle()

    def _start_cycle(self) -> None:
        if self._stopped or self._running:
            return
        self._burst_started_at = None
        self._running = True
        self._cycles += 1
        try:
            result = self._on_cycle()
        except Exception as err:
            self._report(err)
            result = None
        if not inspect.isawaitable(result):
            self._end_cycle()
            return
        future = asyncio.ensure_future(result)
        self._current = future
        future.add_done_callback(self._cycle_done)

    def _cycle_done(self, future: asyncio.Future[Any]) -> None:
        if not future.cancelled():
            error = future.exception()
            if error is not None:
                self._report(error)
        self._current = None
        self._end_cycle()

    def _end_cycle(self) -> None:
        self._running = False
        first = self._pending_first_at
        last = self._pending_last_at
        self._pending_first_at = None
        self._pending_last_at = None
        if self._stopped or first is None or last is None:
            return
        now = self._now()
        due_at = max(now, min(last + self._quiet_ms, first + self._max_ms))
        self._burst_started_at = first
        self._arm(due_at - now)

    def _report(self, error: BaseException) -> None:
        if self._on_error is None:
            _log.warning("wake cycle raised", exc_info=(type(error), error, error.__traceback__))
            return
        try:
            self._on_error(error)
        except Exception:
            _log.debug("on_error raised", exc_info=True)


__all__ = [
    "DEFAULT_MAX_MS",
    "DEFAULT_QUIET_MS",
    "CoalescerCounters",
    "CoalescerState",
    "SetTimer",
    "TimerHandle",
    "WakeCoalescer",
]
