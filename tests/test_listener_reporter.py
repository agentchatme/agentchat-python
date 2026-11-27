"""The listener report, kept current (contract v1 §3.13):
``integration.ListenerReporter`` and ``integration.listener_instance_id``.
Mirrors the TypeScript coverage (``tests/integration/listener-reporter.test.ts``)."""

from __future__ import annotations

import asyncio
import os
import stat
import threading
from pathlib import Path
from typing import Any, Callable

import pytest

from agentchatme import integration
from agentchatme.errors import create_agentchat_error
from agentchatme.integration.listener_reporter import (
    LISTENER_REPORT_MIN_INTERVAL_MS,
    ListenerReporter,
    is_listener_instance_id,
    listener_instance_id,
)

ID = "inst_0123456789abcdef"
MIN = 60_000
START_MS = 1_795_867_200_000.0  # 2026-11-28T12:00:00.000Z


class _Due:
    def __init__(self, at_ms: float, fn: Callable[[], None]) -> None:
        self.at_ms = at_ms
        self.fn = fn
        self.cancelled = False

    def cancel(self) -> None:
        self.cancelled = True


async def _settle() -> None:
    for _ in range(10):
        await asyncio.sleep(0)


class _Clock:
    """The reporter's timers and clock, virtual; the test advances them. A
    report the timers start runs on the real loop, settled between firings."""

    def __init__(self) -> None:
        self.now_ms = START_MS
        self._timers: list[_Due] = []

    def now(self) -> float:
        return self.now_ms

    def set_timer(self, delay_ms: float, fn: Callable[[], None]) -> _Due:
        due = _Due(self.now_ms + delay_ms, fn)
        self._timers.append(due)
        return due

    @property
    def pending(self) -> int:
        return sum(1 for due in self._timers if not due.cancelled)

    async def advance(self, ms: float) -> None:
        target = self.now_ms + ms
        await _settle()
        while True:
            ready = [due for due in self._timers if not due.cancelled and due.at_ms <= target]
            if not ready:
                break
            first = min(ready, key=lambda due: due.at_ms)
            self._timers.remove(first)
            self.now_ms = first.at_ms
            first.fn()
            await _settle()
        self.now_ms = target


Answer = Any  # "ok" or an exception to raise


class _Client:
    """An async client whose ``report_listener`` answers in turn (the last
    answer repeats), recording each call."""

    def __init__(self, answers: list[Answer] | None = None) -> None:
        self.answers = answers or ["ok"]
        self.calls: list[dict[str, Any]] = []
        self.hold: asyncio.Event | None = None

    async def report_listener(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        answer = self.answers[min(len(self.calls) - 1, len(self.answers) - 1)]
        if self.hold is not None:
            await self.hold.wait()
        if isinstance(answer, BaseException):
            raise answer
        return {"instance_id": kwargs["instance_id"], "state": kwargs["state"]}

    def sent(self) -> list[dict[str, Any]]:
        return [{k: v for k, v in call.items() if k != "instance_id"} for call in self.calls]


def _reporter(client: Any, clock: _Clock, **kwargs: Any) -> ListenerReporter:
    return ListenerReporter(
        client, instance_id=ID, now=clock.now, set_timer=clock.set_timer, **kwargs
    )


def _api_error(status: int, body: dict[str, Any], headers: dict[str, str] | None = None) -> Any:
    return create_agentchat_error(body, status, headers)


# ─────────────── when it sends ───────────────


async def test_a_change_is_one_report_about_a_second_later() -> None:
    clock, client = _Clock(), _Client()
    reporter = _reporter(client, clock)
    reporter.set("runs_failing", "runtime_check_failed")
    await clock.advance(999)
    assert client.calls == []
    await clock.advance(1)
    assert client.calls == [
        {"instance_id": ID, "state": "runs_failing", "reason_kind": "runtime_check_failed"}
    ]
    status = reporter.status()
    assert (status.state, status.pending, status.failures, status.last_error) == (
        "runs_failing",
        False,
        0,
        None,
    )
    assert status.reported is not None
    assert (status.reported.state, status.reported.reason_kind, status.reported.at) == (
        "runs_failing",
        "runtime_check_failed",
        "2026-11-28T12:00:01.000Z",
    )
    reporter.stop()


async def test_same_state_is_no_report_ok_and_stopped_send_no_reason() -> None:
    clock, client = _Clock(), _Client()
    reporter = _reporter(client, clock)
    reporter.set("ok", "runtime_missing")
    await clock.advance(2_000)
    reporter.set("ok")
    reporter.set("ok", "other")
    await clock.advance(10 * MIN)
    assert client.sent() == [{"state": "ok"}]

    reporter.set("not_ready")
    await clock.advance(MIN)
    reporter.set("not_ready", "other")  # the same report
    await clock.advance(5 * MIN)
    reporter.set("stopped", "not_signed_in")
    await clock.advance(MIN)
    assert client.sent() == [
        {"state": "ok"},
        {"state": "not_ready", "reason_kind": "other"},
        {"state": "stopped"},
    ]
    reporter.stop()


async def test_a_new_reason_for_the_same_failing_state_is_a_change() -> None:
    clock, client = _Clock(), _Client()
    reporter = _reporter(client, clock)
    reporter.set("not_ready", "not_signed_in")
    await clock.advance(5 * MIN)
    reporter.set("not_ready", "runtime_outdated")
    await clock.advance(5 * MIN)
    assert client.sent() == [
        {"state": "not_ready", "reason_kind": "not_signed_in"},
        {"state": "not_ready", "reason_kind": "runtime_outdated"},
    ]
    reporter.stop()


async def test_not_ok_is_resent_every_30_minutes_and_ok_never() -> None:
    clock, client = _Clock(), _Client()
    reporter = _reporter(client, clock)
    reporter.set("runs_failing", "runtime_check_failed")
    await clock.advance(1_000)
    assert len(client.calls) == 1
    await clock.advance(30 * MIN - 1)
    assert len(client.calls) == 1
    await clock.advance(1)
    assert len(client.calls) == 2
    await clock.advance(30 * MIN)
    assert len(client.calls) == 3
    assert all(c["state"] == "runs_failing" for c in client.calls)

    reporter.set("ok")
    await clock.advance(MIN)
    assert client.sent()[-1] == {"state": "ok"}
    after_ok = len(client.calls)
    await clock.advance(5 * 60 * MIN)
    assert len(client.calls) == after_ok
    assert clock.pending == 0
    reporter.stop()


async def test_rapid_changes_are_one_report_of_the_latest_state() -> None:
    clock, client = _Clock(), _Client()
    reporter = _reporter(client, clock)
    reporter.set("not_ready", "runtime_missing")
    await clock.advance(300)
    reporter.set("runs_failing")
    await clock.advance(300)
    reporter.set("not_ready", "not_signed_in")
    await clock.advance(400)
    assert client.sent() == [{"state": "not_ready", "reason_kind": "not_signed_in"}]
    reporter.stop()


async def test_a_state_that_keeps_changing_is_still_reported() -> None:
    clock, client = _Clock(), _Client()
    reporter = _reporter(client, clock)
    flapping = ("not_ready", "runs_failing")
    for i in range(20):
        reporter.set(flapping[i % 2])  # type: ignore[arg-type]
        await clock.advance(200)
    # t = 4 s: one report, at t = 1 s, of the state then (set at 0.8 s); the
    # next waits for the minute and carries the latest (set at 3.8 s).
    assert client.sent() == [{"state": "not_ready", "reason_kind": "other"}]
    await clock.advance(MIN)
    assert client.sent() == [
        {"state": "not_ready", "reason_kind": "other"},
        {"state": "runs_failing", "reason_kind": "other"},
    ]
    reporter.stop()


async def test_a_change_back_to_what_the_server_holds_is_no_report() -> None:
    clock, client = _Clock(), _Client()
    reporter = _reporter(client, clock)
    reporter.set("ok")
    await clock.advance(5 * MIN)
    reporter.set("runs_failing")
    await clock.advance(500)
    reporter.set("ok")
    await clock.advance(60 * MIN)
    assert client.sent() == [{"state": "ok"}]
    assert reporter.status().pending is False
    reporter.stop()


async def test_at_most_one_attempt_a_minute() -> None:
    clock, client = _Clock(), _Client()
    reporter = _reporter(client, clock)
    reporter.set("runs_failing")
    await clock.advance(1_000)  # sent at t = 1 s
    reporter.set("ok")
    await clock.advance(LISTENER_REPORT_MIN_INTERVAL_MS - 1)
    assert len(client.calls) == 1
    await clock.advance(1)
    assert client.sent() == [{"state": "runs_failing", "reason_kind": "other"}, {"state": "ok"}]
    reporter.stop()


async def test_a_change_while_a_report_is_under_way_is_sent_after_it() -> None:
    clock, client = _Clock(), _Client()
    client.hold = asyncio.Event()
    reporter = _reporter(client, clock)
    reporter.set("not_ready")
    await clock.advance(1_000)
    assert len(client.calls) == 1
    reporter.set("ok")  # while the first is under way
    client.hold.set()
    await clock.advance(MIN)
    assert client.sent() == [{"state": "not_ready", "reason_kind": "other"}, {"state": "ok"}]
    assert reporter.status().pending is False
    reporter.stop()


async def test_a_sync_client_runs_in_a_worker_thread() -> None:
    clock = _Clock()
    threads: list[str] = []

    class _SyncClient:
        def report_listener(self, **kwargs: Any) -> dict[str, Any]:
            threads.append(threading.current_thread().name)
            return {"instance_id": kwargs["instance_id"], "state": kwargs["state"]}

    reporter = _reporter(_SyncClient(), clock)
    reporter.set("ok")
    assert await reporter.flush() is True
    assert len(threads) == 1
    assert threads[0] != threading.current_thread().name
    reporter.stop()


# ─────────────── failures ───────────────


async def test_an_error_is_retried_with_capped_backoff_told_once_never_raised() -> None:
    clock, client = _Clock(), _Client([RuntimeError("connection refused")])
    errors: list[str] = []
    reporter = _reporter(client, clock, on_error=lambda e: errors.append(str(e)))
    reporter.set("runs_failing")
    await clock.advance(1_000)
    assert len(client.calls) == 1
    for gap in [m * MIN for m in (1, 2, 4, 8, 15, 15)]:
        before = len(client.calls)
        await clock.advance(gap - 1)
        assert len(client.calls) == before
        await clock.advance(1)
        assert len(client.calls) == before + 1
    assert errors == ["listener report (runs_failing): connection refused; trying again later"]
    status = reporter.status()
    assert (status.pending, status.failures, status.last_error, status.reported) == (
        True,
        7,
        "connection refused",
        None,
    )
    reporter.stop()


async def test_a_success_resets_the_backoff_and_a_new_failure_is_told_again() -> None:
    clock = _Clock()
    client = _Client([RuntimeError("down"), "ok", RuntimeError("down")])
    errors: list[str] = []
    reporter = _reporter(client, clock, on_error=lambda e: errors.append(str(e)))
    reporter.set("not_ready")
    await clock.advance(1_000 + MIN)  # fails, then succeeds a minute later
    assert len(client.calls) == 2
    status = reporter.status()
    assert (status.pending, status.failures, status.last_error) == (False, 0, None)
    await clock.advance(30 * MIN)  # the re-send fails
    assert len(client.calls) == 3
    await clock.advance(MIN - 1)
    assert len(client.calls) == 3
    await clock.advance(1)  # retried after 1 min again, not 2
    assert len(client.calls) == 4
    assert len(errors) == 2
    reporter.stop()


async def test_a_429_waits_as_the_server_says_even_past_the_backoff_cap() -> None:
    limited = _api_error(
        429, {"code": "RATE_LIMITED", "message": "Too many requests"}, {"Retry-After": "1800"}
    )
    clock, client = _Clock(), _Client([limited, "ok"])
    reporter = _reporter(client, clock)
    reporter.set("runs_failing")
    await clock.advance(1_000)
    await clock.advance(30 * MIN - 1)
    assert len(client.calls) == 1
    await clock.advance(1)
    assert len(client.calls) == 2
    assert reporter.status().pending is False
    reporter.stop()


async def test_a_503_backs_off_and_a_503_naming_retry_after_s_waits_that_long() -> None:
    unavailable = _api_error(
        503,
        {"code": "LISTENER_REPORTS_UNAVAILABLE", "message": "Listener reports are not available right now"},
    )
    busy = _api_error(
        503, {"code": "SERVICE_UNAVAILABLE", "message": "busy", "details": {"retry_after_s": 300}}
    )
    clock, client = _Clock(), _Client([unavailable, busy, "ok"])
    errors: list[str] = []
    reporter = _reporter(client, clock, on_error=lambda e: errors.append(str(e)))
    reporter.set("not_ready", "runtime_missing")
    await clock.advance(1_000)
    assert "503 LISTENER_REPORTS_UNAVAILABLE" in errors[0]
    await clock.advance(MIN)  # the backoff's first minute
    assert len(client.calls) == 2
    await clock.advance(5 * MIN - 1)
    assert len(client.calls) == 2
    await clock.advance(1)
    assert len(client.calls) == 3
    assert reporter.status().pending is False
    reporter.stop()


async def test_an_on_error_that_raises_does_not_break_it() -> None:
    clock, client = _Clock(), _Client([RuntimeError("down"), "ok"])

    def bad(_err: Exception) -> None:
        raise RuntimeError("observer bug")

    reporter = _reporter(client, clock, on_error=bad)
    reporter.set("not_ready")
    await clock.advance(1_000 + MIN)
    assert len(client.calls) == 2
    assert reporter.status().pending is False
    reporter.stop()


async def test_without_on_error_a_failure_is_a_warning_on_the_kit_logger(
    caplog: pytest.LogCaptureFixture,
) -> None:
    clock, client = _Clock(), _Client([RuntimeError("down")])
    reporter = _reporter(client, clock)
    with caplog.at_level("WARNING", logger="agentchat.integration"):
        reporter.set("not_ready")
        await clock.advance(1_000)
    assert any("listener report (not_ready): down" in r.getMessage() for r in caplog.records)
    reporter.stop()


def test_set_without_a_running_loop_never_raises() -> None:
    client = _Client()
    errors: list[str] = []
    reporter = ListenerReporter(client, instance_id=ID, on_error=lambda e: errors.append(str(e)))
    reporter.set("not_ready")  # no loop in this thread: told, not raised
    assert len(errors) == 1
    assert "no running asyncio loop" in errors[0]
    assert reporter.status().pending is True
    assert client.calls == []


# ─────────────── flush and stop ───────────────


async def test_flush_sends_now_and_returns_whether_the_server_holds_it() -> None:
    clock, client = _Clock(), _Client(["ok", RuntimeError("down")])
    reporter = _reporter(client, clock)
    assert await reporter.flush() is True  # nothing set: nothing pending
    reporter.set("stopped")
    assert await reporter.flush() is True
    assert client.sent() == [{"state": "stopped"}]
    assert await reporter.flush() is True  # already held: no second report
    assert len(client.calls) == 1
    reporter.set("ok")
    assert await reporter.flush() is False  # inside the minute, but flush does not wait; it fails
    assert len(client.calls) == 2
    reporter.stop()
    assert await reporter.flush() is False


async def test_flush_keeps_a_wait_the_server_named() -> None:
    limited = _api_error(
        429, {"code": "RATE_LIMITED", "message": "Too many requests"}, {"Retry-After": "600"}
    )
    clock, client = _Clock(), _Client([limited, "ok"])
    reporter = _reporter(client, clock)
    reporter.set("stopped")
    assert await reporter.flush() is False
    assert await reporter.flush() is False
    assert len(client.calls) == 1
    await clock.advance(10 * MIN)
    assert len(client.calls) == 2
    assert await reporter.flush() is True
    reporter.stop()


async def test_flush_while_another_state_is_under_way_waits_then_sends_what_set_says() -> None:
    clock = _Clock()
    gate = asyncio.Event()

    class _Gated(_Client):
        async def report_listener(self, **kwargs: Any) -> dict[str, Any]:
            if len(self.calls) == 1:  # the second report waits for the gate
                self.calls.append(kwargs)
                await gate.wait()
                return {"instance_id": kwargs["instance_id"], "state": kwargs["state"]}
            return await super().report_listener(**kwargs)

    client = _Gated()
    reporter = _reporter(client, clock)
    reporter.set("ok")
    await clock.advance(MIN)  # the server holds ok
    reporter.set("runs_failing")
    await clock.advance(MIN)  # under way, held
    assert len(client.calls) == 2
    reporter.set("ok")  # the check recovered, while runs_failing is in flight
    assert reporter.status().pending is True  # runs_failing will land and replace ok
    flushed = asyncio.ensure_future(reporter.flush())
    await _settle()
    gate.set()
    assert await flushed is True
    assert client.sent() == [
        {"state": "ok"},
        {"state": "runs_failing", "reason_kind": "other"},
        {"state": "ok"},
    ]
    status = reporter.status()
    assert status.pending is False
    assert status.reported is not None and status.reported.state == "ok"
    reporter.stop()


async def test_stop_before_the_send_starts_makes_flush_false_not_cancelled() -> None:
    clock, client = _Clock(), _Client()
    reporter = _reporter(client, clock)
    reporter.set("stopped")
    asyncio.get_running_loop().call_soon(reporter.stop)  # e.g. a signal handler
    assert await reporter.flush() is False
    await _settle()
    assert client.calls == []


async def test_a_server_wait_is_kept_to_at_most_an_hour() -> None:
    limited = _api_error(
        429, {"code": "RATE_LIMITED", "message": "Too many requests"}, {"Retry-After": "86400"}
    )
    clock, client = _Clock(), _Client([limited, "ok"])
    reporter = _reporter(client, clock)
    reporter.set("runs_failing")
    await clock.advance(1_000)
    await clock.advance(60 * MIN - 1)
    assert len(client.calls) == 1
    await clock.advance(1)
    assert len(client.calls) == 2
    reporter.stop()


async def test_a_sync_method_that_returns_an_awaitable_is_awaited() -> None:
    clock = _Clock()
    inner = _Client()

    class _Wrapped:
        def report_listener(self, **kwargs: Any) -> Any:
            return inner.report_listener(**kwargs)  # a coroutine, from a plain function

    reporter = _reporter(_Wrapped(), clock)
    reporter.set("ok")
    assert await reporter.flush() is True
    assert inner.sent() == [{"state": "ok"}]
    reporter.stop()


async def test_a_set_made_before_the_loop_ran_is_sent_by_the_next_same_set_on_the_loop() -> None:
    client = _Client()
    errors: list[str] = []
    holder: dict[str, ListenerReporter] = {}

    def before_the_loop() -> None:
        reporter = ListenerReporter(
            client, instance_id=ID, coalesce_ms=10, on_error=lambda e: errors.append(str(e))
        )
        reporter.set("not_ready", "not_signed_in")  # no loop in this thread yet
        holder["r"] = reporter

    await asyncio.to_thread(before_the_loop)
    reporter = holder["r"]
    assert errors and "no running asyncio loop" in errors[0]
    reporter.set("not_ready", "not_signed_in")  # the same state, now on the loop
    await asyncio.sleep(0.1)
    assert client.sent() == [{"state": "not_ready", "reason_kind": "not_signed_in"}]
    reporter.stop()


async def test_stop_stops_no_report_after_no_timer_a_report_under_way_abandoned() -> None:
    clock, client = _Clock(), _Client()
    client.hold = asyncio.Event()
    errors: list[str] = []
    reporter = _reporter(client, clock, on_error=lambda e: errors.append(str(e)))
    reporter.set("runs_failing")
    await clock.advance(1_000)
    assert len(client.calls) == 1
    waiting = asyncio.ensure_future(reporter.flush())
    await _settle()
    reporter.stop()
    assert await waiting is False
    assert errors == []  # an abandoned report is not a failure to tell
    reporter.set("ok")
    await clock.advance(2 * 60 * MIN)
    assert len(client.calls) == 1
    assert clock.pending == 0
    assert reporter.status().stopped is True


async def test_stop_cancels_a_pending_report_and_its_timer() -> None:
    clock, client = _Clock(), _Client()
    reporter = _reporter(client, clock)
    reporter.set("not_ready")
    assert clock.pending == 1
    reporter.stop()
    assert clock.pending == 0
    await clock.advance(60 * MIN)
    assert client.calls == []


async def test_on_the_real_loop_the_timer_is_a_loop_timer_and_stop_cancels_it() -> None:
    client = _Client()
    reporter = ListenerReporter(client, instance_id=ID, coalesce_ms=10)
    reporter.set("not_ready")
    await asyncio.sleep(0.1)
    assert client.sent() == [{"state": "not_ready", "reason_kind": "other"}]
    assert reporter.status().next_attempt_at is not None  # the 30-minute re-send
    reporter.stop()
    assert reporter.status().next_attempt_at is None


# ─────────────── misuse and surface ───────────────


def test_a_bad_instance_id_state_or_reason_is_refused() -> None:
    client = _Client()
    with pytest.raises(ValueError):
        ListenerReporter(client, instance_id="short")
    with pytest.raises(ValueError):
        ListenerReporter(client, instance_id="has space in it")
    reporter = ListenerReporter(client, instance_id=ID)
    with pytest.raises(ValueError):
        reporter.set("broken")  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        reporter.set("not_ready", "tired")  # type: ignore[arg-type]
    assert reporter.status().state is None


def test_it_is_on_the_kit() -> None:
    assert integration.ListenerReporter is ListenerReporter
    assert integration.listener_instance_id is listener_instance_id
    assert integration.is_listener_instance_id("a" * 8) is True
    assert integration.is_listener_instance_id("a" * 65) is False
    assert integration.is_listener_instance_id(None) is False


# ─────────────── the install's id ───────────────


def test_makes_a_22_character_url_safe_id_once_and_reuses_it(tmp_path: Path) -> None:
    path = tmp_path / "state" / "listener-instance"
    first = listener_instance_id(path)
    assert len(first) == 22
    assert is_listener_instance_id(first)
    assert path.read_bytes() == f"{first}\n".encode()
    if os.name != "nt":
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert [p.name for p in (tmp_path / "state").iterdir()] == ["listener-instance"]
    assert listener_instance_id(path) == first
    assert listener_instance_id(tmp_path / "other") != first


def test_keeps_an_id_already_in_the_file(tmp_path: Path) -> None:
    path = tmp_path / "listener-instance"
    path.write_text("  kept_id-from-an-older-install \n", encoding="utf-8")
    assert listener_instance_id(path) == "kept_id-from-an-older-install"
    assert path.read_text(encoding="utf-8") == "  kept_id-from-an-older-install \n"


def test_replaces_a_file_that_holds_no_valid_id(tmp_path: Path) -> None:
    path = tmp_path / "listener-instance"
    path.write_text("not an id!\n", encoding="utf-8")
    made = listener_instance_id(path)
    assert is_listener_instance_id(made)
    assert path.read_text(encoding="utf-8") == f"{made}\n"


def test_callers_that_start_together_get_the_one_id_written(tmp_path: Path) -> None:
    path = tmp_path / "listener-instance"
    ids: list[str] = []
    barrier = threading.Barrier(8)

    def make() -> None:
        barrier.wait()
        ids.append(listener_instance_id(path))

    threads = [threading.Thread(target=make) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(set(ids)) == 1
    assert path.read_text(encoding="utf-8") == f"{ids[0]}\n"
    assert [p.name for p in tmp_path.iterdir()] == ["listener-instance"]
