"""Turns that are running, per conversation (reply turns) and per invite
(invite turns).

While a reply turn runs, its conversation's card stays on the tray (the
verdict is recorded when the turn has ended), so every wake in between would
gate it again and start a second turn: the tracker is what keeps such cards
out of the gate.

A turn ends only when the runtime confirms it ended. A wait that timed out
is not an end: the runtime reports ``still_running`` and the turn keeps its
conversation, following ``next`` when the runtime can say when it really
ends. Only :meth:`TurnTracker.release` lets go of a turn whose end never
comes.

Where the copies this replaces differed: Hermes ran one turn per cycle and
held its worker for it (one in-flight set per cycle); OpenClaw ended a turn
when its wait timed out (so a second turn could start for a conversation
whose first was still running). Here the turn is held until its confirmed
end, and cycles go on meanwhile.

Usable from a loop (:meth:`TurnTracker.confirmed_end` is a coroutine) and,
for :meth:`begin` / :meth:`end` / the queries, from any thread.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import threading
import time
from collections.abc import Awaitable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Callable, Literal, TypeVar

from .outcome import TurnEnd

TurnKind = Literal["reply", "invite"]


@dataclass(frozen=True)
class TrackedTurn:
    """What a turn is for: a reply turn's conversation (and the anchor the
    gate saw), or an invite turn's invite."""

    kind: TurnKind
    conversation_id: str | None = None
    anchor: str | None = None
    invite_id: str | None = None

    @staticmethod
    def reply(conversation_id: str, anchor: str | None = None) -> TrackedTurn:
        return TrackedTurn(kind="reply", conversation_id=conversation_id, anchor=anchor)

    @staticmethod
    def invite(invite_id: str) -> TrackedTurn:
        return TrackedTurn(kind="invite", invite_id=invite_id)

    @property
    def key(self) -> str:
        return (
            f"conversation:{self.conversation_id}"
            if self.kind == "reply"
            else f"invite:{self.invite_id}"
        )


@dataclass(frozen=True)
class TurnHandle:
    key: str
    turn: TrackedTurn
    started_at: float
    """Epoch milliseconds when the host started the turn."""


@dataclass(frozen=True)
class RunningTurn:
    """A status snapshot of one running turn."""

    turn: TrackedTurn
    started_at: float
    wait_timed_out_at: float | None
    """Epoch ms the last wait timed out without an end, or ``None``."""
    overdue: bool = False
    """Set by :meth:`~agentchatme.integration.ReceiveLoop.status` for a turn
    running past 30 minutes (the runtime should have ended it)."""


@dataclass
class _Entry:
    handle: TurnHandle
    wait_timed_out_at: float | None = None
    released: bool = False
    waiter: asyncio.Future[None] | None = None


CardT = TypeVar("CardT")


def _now_ms() -> float:
    return time.time() * 1000.0


class TurnTracker:
    """Per-conversation and per-invite running turns."""

    def __init__(self, *, now: Callable[[], float] | None = None) -> None:
        self._now = now or _now_ms
        self._lock = threading.Lock()
        self._turns: dict[str, _Entry] = {}
        self._release_listeners: list[Callable[[TurnHandle], object]] = []

    def begin(self, turn: TrackedTurn) -> TurnHandle | None:
        """Start tracking a turn. ``None`` when one is already running for
        that conversation or invite."""
        key = turn.key
        with self._lock:
            if key in self._turns:
                return None
            handle = TurnHandle(key=key, turn=turn, started_at=self._now())
            self._turns[key] = _Entry(handle=handle)
            return handle

    def conversation_running(self, conversation_id: str) -> bool:
        with self._lock:
            return f"conversation:{conversation_id}" in self._turns

    def invite_running(self, invite_id: str) -> bool:
        with self._lock:
            return f"invite:{invite_id}" in self._turns

    def without_running(self, cards: Iterable[CardT]) -> list[CardT]:
        """The cards whose conversation has no running turn: the ones a gate
        may see. Cards are the served dicts or typed cards."""
        out: list[CardT] = []
        for card in cards:
            cid = (
                card.get("conversation_id")
                if isinstance(card, Mapping)
                else getattr(card, "conversation_id", None)
            )
            if not (isinstance(cid, str) and self.conversation_running(cid)):
                out.append(card)
        return out

    def running(self) -> list[RunningTurn]:
        """Snapshot for status."""
        with self._lock:
            return [
                RunningTurn(
                    turn=entry.handle.turn,
                    started_at=entry.handle.started_at,
                    wait_timed_out_at=entry.wait_timed_out_at,
                )
                for entry in self._turns.values()
            ]

    @property
    def size(self) -> int:
        with self._lock:
            return len(self._turns)

    async def confirmed_end(
        self, handle: TurnHandle, done: Awaitable[TurnEnd] | TurnEnd
    ) -> TurnEnd | None:
        """Follow what the runtime reports until it confirms the end, through
        any number of ``still_running`` reports. Returns the confirmed end
        (``completed`` or ``failed``), or ``None`` as soon as :meth:`release`
        lets go of the turn — also while a report is still being awaited, so
        a runtime that never answers cannot hold a turn slot forever. A
        report that raises counts as a failed turn. The turn stays tracked:
        call :meth:`end` once its outcome is recorded."""
        released = self._release_signal(handle)
        if released is None:
            return None
        report: Awaitable[TurnEnd] | TurnEnd | None = done
        while report is not None:
            if inspect.isawaitable(report):
                pending: asyncio.Future[Any] = asyncio.ensure_future(report)
                finished, _ = await asyncio.wait(
                    {pending, released}, return_when=asyncio.FIRST_COMPLETED
                )
                if pending not in finished:
                    # Released while the runtime had not answered: let go.
                    pending.add_done_callback(_consume)
                    return None
                try:
                    end = pending.result()
                except asyncio.CancelledError:
                    raise
                except Exception as err:
                    return TurnEnd(status="failed", detail=f"the turn's report failed: {err}")
            else:
                end = report
            if not isinstance(end, TurnEnd):
                return TurnEnd(status="failed", detail=f"the turn reported {type(end).__name__}")
            if end.status != "still_running":
                return end if self.holds(handle) else None
            with self._lock:
                entry = self._turns.get(handle.key)
                if entry is None or entry.handle is not handle or entry.released:
                    return None
                entry.wait_timed_out_at = self._now()
            report = end.next
        # Still running and the runtime cannot say when it ends: hold the turn
        # until release() lets go of it.
        await released
        return None

    def _release_signal(self, handle: TurnHandle) -> asyncio.Future[None] | None:
        """The future release() resolves for this turn (made once, on the
        running loop); ``None`` when the turn is no longer tracked."""
        loop = asyncio.get_running_loop()
        with self._lock:
            entry = self._turns.get(handle.key)
            if entry is None or entry.handle is not handle or entry.released:
                return None
            if entry.waiter is None:
                entry.waiter = loop.create_future()
            return entry.waiter

    def holds(self, handle: TurnHandle) -> bool:
        """Whether ``handle`` is still tracked (not ended, not released). A
        host checks it after every await before recording anything: a
        released turn never records."""
        with self._lock:
            entry = self._turns.get(handle.key)
            return entry is not None and entry.handle is handle and not entry.released

    def end(self, handle: TurnHandle) -> None:
        """Stop tracking a turn (its outcome is recorded, or it never started)."""
        with self._lock:
            entry = self._turns.get(handle.key)
            if entry is not None and entry.handle is handle:
                del self._turns[handle.key]

    def on_release(self, listener: Callable[[TurnHandle], object]) -> Callable[[], None]:
        """Call ``listener(handle)`` whenever :meth:`release` lets go of a
        turn (a host frees what it held for it). Returns an unsubscribe
        function."""
        with self._lock:
            self._release_listeners.append(listener)

        def off() -> None:
            with self._lock:
                if listener in self._release_listeners:
                    self._release_listeners.remove(listener)

        return off

    def release(self, key: str) -> bool:
        """Let go of a turn whose end will not be confirmed (an operator's
        call). Every wait on it ends at once (:meth:`confirmed_end` returns
        ``None``) and the :meth:`on_release` listeners are told. Its
        conversation may be gated again; nothing is recorded for it."""
        with self._lock:
            entry = self._turns.pop(key, None)
            if entry is None:
                return False
            entry.released = True
            waiter = entry.waiter
            listeners = list(self._release_listeners)
        if waiter is not None and not waiter.done():
            waiter.get_loop().call_soon_threadsafe(_resolve, waiter)
        for listener in listeners:
            with contextlib.suppress(Exception):  # a listener must not break release
                listener(entry.handle)
        return True


def _resolve(waiter: asyncio.Future[None]) -> None:
    if not waiter.done():
        waiter.set_result(None)


def _consume(task: asyncio.Future[Any]) -> None:
    """Retrieve an abandoned report's outcome so it is never logged as lost."""
    if not task.cancelled():
        task.exception()


__all__ = ["RunningTurn", "TrackedTurn", "TurnHandle", "TurnKind", "TurnTracker"]
