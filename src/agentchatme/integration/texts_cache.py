"""The served texts, kept current: fetched at start and on a daily timer,
the last good copy kept in a file so an integration that starts offline is
not mute, and a change event for whatever is placed from them (the identity
block, the manual).

Every text an agent sees is written once on the server and served as data
(``GET /v1/texts?runtime=``). This cache carries no copy of any of them.

Policy (the stricter of the copies this replaces — Hermes ``texts.py``,
OpenClaw and agent-core ``integration/texts.ts``; the TypeScript kit's
``TextsCache`` is the twin):

- the file is read at start, before the first fetch, and only a file written
  for the same runtime counts;
- a served copy is never replaced by the file: a failed refresh keeps the
  last served copy;
- after a failure the next attempt comes after 5 min, doubling to 1 h; after
  a success after ``refresh_ms`` (24 h);
- one fetch attempt per call, bounded by a timeout, never retried inside the
  call (the timer is the retry);
- the file is replaced atomically (temp + fsync + rename, mode 0600);
- listeners hear only real changes (content, not a re-fetch of the same).

Runs on an asyncio loop (``await cache.start()``), with an async or a sync
client (a sync client's call runs in a worker thread). From sync code,
:meth:`TextsCache.refresh_sync` makes one attempt with a sync client.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import threading
import time
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Literal

from .._call import CallOptions
from ..types.texts import Texts
from ._files import write_atomic

_log = logging.getLogger("agentchat.integration")

TEXTS_REFRESH_MS = 24 * 60 * 60_000
TEXTS_RETRY_MS = 5 * 60_000
TEXTS_RETRY_MAX_MS = 60 * 60_000
TEXTS_FETCH_TIMEOUT_MS = 10_000
_CACHE_FILE_VERSION = 1

TextsCacheSource = Literal["server", "cache", "none"]
TextsListener = Callable[[Texts, Literal["server", "cache"]], Any]


def is_texts(value: Any) -> bool:
    """The shape every consumer relies on (extra fields a newer server adds
    pass through)."""
    data = value.model_dump(mode="json") if isinstance(value, Texts) else value
    if not isinstance(data, Mapping):
        return False
    manual = data.get("manual")

    def non_empty(key: str) -> bool:
        item = data.get(key)
        return isinstance(item, str) and bool(item.strip())

    return (
        isinstance(data.get("version"), str)
        and isinstance(manual, Mapping)
        and isinstance(manual.get("url"), str)
        and isinstance(manual.get("version"), str)
        and non_empty("wake_instruction")
        and non_empty("invite_prompt")
        and non_empty("identity_line")
        and isinstance(data.get("identity_line_template"), str)
    )


def _iso(epoch_ms: float) -> str:
    moment = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def _now_ms() -> float:
    return time.time() * 1000.0


class TextsCache:
    """Holds the served texts for one agent and keeps them fresh.

    ``client`` is an :class:`~agentchatme.AsyncAgentChatClient` or an
    :class:`~agentchatme.AgentChatClient`; ``runtime`` the name the server
    spells tool names for (``hermes``, ``openclaw``, …); ``file_path`` where
    the last good copy is kept (``None``: nothing is written or read).
    ``on_error`` is called once per new failure condition, not once per
    retry. ``now`` is the clock in epoch milliseconds.
    """

    def __init__(
        self,
        client: Any,
        *,
        runtime: str,
        file_path: str | Path | None = None,
        refresh_ms: int = TEXTS_REFRESH_MS,
        retry_ms: int = TEXTS_RETRY_MS,
        retry_max_ms: int = TEXTS_RETRY_MAX_MS,
        fetch_timeout_ms: int = TEXTS_FETCH_TIMEOUT_MS,
        on_error: Callable[[Exception], Any] | None = None,
        now: Callable[[], float] | None = None,
    ) -> None:
        if not isinstance(runtime, str) or not runtime:
            raise ValueError("runtime must be a non-empty runtime name")
        self.client = client
        self.runtime = runtime
        self.file_path = Path(file_path) if file_path is not None else None
        self._refresh_ms = refresh_ms
        self._retry_ms = retry_ms
        self._retry_max_ms = max(retry_ms, retry_max_ms)
        self._fetch_timeout_ms = fetch_timeout_ms
        self._on_error = on_error
        self._now = now or _now_ms

        self._lock = threading.Lock()
        self._listeners: list[TextsListener] = []
        self._texts: Texts | None = None
        self._source: TextsCacheSource = "none"
        self._fetched_at: str | None = None
        self._last_error: str | None = None
        self._failures = 0
        self._server_seen = False
        self._in_flight: asyncio.Future[bool] | None = None
        self._timer: asyncio.TimerHandle | None = None
        self._started = False
        self._stopped = False
        self._background: set[asyncio.Future[Any]] = set()

    # ─── state ───────────────────────────────────────────────────────────

    def current(self) -> Texts | None:
        """The texts in hand, or ``None`` until a copy exists. No network."""
        with self._lock:
            return self._texts

    @property
    def source(self) -> TextsCacheSource:
        """Where :meth:`current` came from: this process's fetch, the file,
        or nowhere yet."""
        return self._source

    @property
    def fetched_at(self) -> str | None:
        """When the current copy was served (also for a copy from the file)."""
        return self._fetched_at

    @property
    def last_error(self) -> str | None:
        """The last fetch failure, cleared by the next success."""
        return self._last_error

    @property
    def api_base(self) -> str | None:
        """The API base the texts are fetched from — what a manual download
        is pinned to."""
        base = getattr(self.client, "base_url", None)
        return base if isinstance(base, str) else None

    def on_change(self, listener: TextsListener) -> Callable[[], None]:
        """Call ``listener(texts, source)`` with every copy that differs from
        the one before (from the file or the server). Returns an unsubscribe
        function."""
        with self._lock:
            self._listeners.append(listener)

        def off() -> None:
            with self._lock:
                if listener in self._listeners:
                    self._listeners.remove(listener)

        return off

    # ─── lifecycle (on a loop) ───────────────────────────────────────────

    async def start(self) -> Texts | None:
        """Read the file, make one fetch, arm the timer. Returns what is in
        hand once the first attempt has finished; never raises."""
        if self._started:
            return self.current()
        self._started = True
        self._stopped = False
        loaded = await asyncio.to_thread(self._read_file)
        if loaded is not None:
            self._adopt(*loaded)
        await self.refresh()
        return self.current()

    def stop(self) -> None:
        self._stopped = True
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    async def ensure(self) -> Texts | None:
        """The texts for a cycle: what is in hand, after one more fetch
        attempt if the server has not answered yet in this process. ``None``
        only when the server is unreachable and no copy was ever kept."""
        if not self._server_seen and not self._stopped:
            await self.refresh()
        return self.current()

    async def refresh(self) -> bool:
        """Fetch now (coalesced with one in flight), keep it, arm the next
        attempt. Returns whether the server answered."""
        task = self._in_flight
        if task is None or task.done():
            task = asyncio.ensure_future(self._fetch_once())
            self._in_flight = task
        return await asyncio.shield(task)

    def refresh_sync(self) -> bool:
        """One attempt from sync code with a sync client (no timer): the file
        is read first when nothing is in hand. Returns whether the server
        answered."""
        if self._texts is None:
            loaded = self._read_file()
            if loaded is not None:
                self._adopt(*loaded)
        try:
            texts = self._checked(self.client.texts.get(runtime=self.runtime, opts=self._opts()))
        except Exception as err:
            self._failed(err)
            return False
        fetched_at = self._served(texts)
        self._save_file(texts, fetched_at)
        return True

    # ─── one attempt ─────────────────────────────────────────────────────

    def _opts(self) -> CallOptions:
        return CallOptions(timeout_ms=self._fetch_timeout_ms, retry="never")

    async def _fetch_once(self) -> bool:
        try:
            getter = self.client.texts.get
            if inspect.iscoroutinefunction(getter):
                served = await getter(runtime=self.runtime, opts=self._opts())
            else:
                served = await asyncio.to_thread(getter, runtime=self.runtime, opts=self._opts())
            texts = self._checked(served)
        except Exception as err:
            if self._stopped:
                return False
            self._failed(err)
            self._arm(min(self._retry_max_ms, self._retry_ms * 2 ** min(self._failures - 1, 20)))
            return False
        if self._stopped:
            return True
        fetched_at = self._served(texts)
        await asyncio.to_thread(self._save_file, texts, fetched_at)
        self._arm(self._refresh_ms)
        return True

    def _checked(self, served: Any) -> Texts:
        if inspect.isawaitable(served):
            close = getattr(served, "close", None)
            if callable(close):
                close()
            raise TypeError("refresh_sync() needs a sync client; use await refresh()")
        if not is_texts(served):
            raise ValueError("GET /v1/texts: the answer is not a text set")
        texts = served if isinstance(served, Texts) else Texts.model_validate(served)
        if texts.runtime is not None and texts.runtime != self.runtime:
            raise ValueError(f"GET /v1/texts: texts for {texts.runtime}, asked for {self.runtime}")
        return texts

    def _served(self, texts: Texts) -> str:
        fetched_at = _iso(self._now())
        self._server_seen = True
        self._failures = 0
        self._last_error = None
        self._adopt(texts, "server", fetched_at)
        return fetched_at

    def _failed(self, err: Exception) -> None:
        message = str(err) or type(err).__name__
        self._failures += 1
        if message != self._last_error:
            self._last_error = message
            held = (
                "no texts yet"
                if self._texts is None
                else f"keeping the {self._source} copy from {self._fetched_at or '?'}"
            )
            self._report(RuntimeError(f"texts: {message} — {held}"))

    def _adopt(self, texts: Texts, source: Literal["server", "cache"], fetched_at: str) -> None:
        with self._lock:
            before = self._texts
            changed = before is None or before.model_dump(mode="json") != texts.model_dump(
                mode="json"
            )
            self._texts = texts
            self._source = source
            self._fetched_at = fetched_at
            listeners = list(self._listeners)
        if not changed:
            return
        for listener in listeners:
            try:
                result = listener(texts, source)
            except Exception as err:
                self._report(err)
                continue
            if inspect.isawaitable(result):
                try:
                    asyncio.get_running_loop()
                except RuntimeError:
                    close = getattr(result, "close", None)
                    if callable(close):
                        close()
                    self._report(RuntimeError("texts: an async listener needs a running loop"))
                    continue
                task = asyncio.ensure_future(result)
                self._background.add(task)
                task.add_done_callback(self._background.discard)

    def _read_file(self) -> tuple[Texts, Literal["cache"], str] | None:
        if self.file_path is None:
            return None
        try:
            entry: Any = json.loads(self.file_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return None
        if not isinstance(entry, dict) or entry.get("version") != _CACHE_FILE_VERSION:
            return None
        if entry.get("runtime") != self.runtime:
            return None
        fetched_at = entry.get("fetched_at")
        if isinstance(fetched_at, (int, float)) and not isinstance(fetched_at, bool):
            # A file an earlier integration wrote with epoch seconds.
            fetched_at = _iso(float(fetched_at) * 1000.0)
        if not isinstance(fetched_at, str) or not is_texts(entry.get("texts")):
            return None
        try:
            texts = Texts.model_validate(entry["texts"])
        except Exception:
            return None
        return texts, "cache", fetched_at

    def _save_file(self, texts: Texts, fetched_at: str) -> None:
        if self.file_path is None:
            return
        entry = {
            "version": _CACHE_FILE_VERSION,
            "runtime": self.runtime,
            "fetched_at": fetched_at,
            "texts": texts.model_dump(mode="json"),
        }
        try:
            write_atomic(self.file_path, json.dumps(entry, indent=2) + "\n", mode=0o600)
        except OSError as err:
            # The file is a convenience for an offline start, never a dependency.
            self._report(RuntimeError(f"texts: the cache file could not be written: {err}"))

    def _arm(self, delay_ms: float) -> None:
        if self._stopped or not self._started:
            return
        if self._timer is not None:
            self._timer.cancel()
        loop = asyncio.get_running_loop()
        self._timer = loop.call_later(max(0.0, delay_ms) / 1000.0, self._fire)

    def _fire(self) -> None:
        self._timer = None
        if not self._stopped:
            task = asyncio.ensure_future(self.refresh())
            self._background.add(task)
            task.add_done_callback(self._background.discard)

    def _report(self, err: Exception) -> None:
        if self._on_error is None:
            _log.warning("%s", err)
            return
        try:
            self._on_error(err)
        except Exception:
            _log.debug("texts: on_error raised", exc_info=True)


__all__ = [
    "TEXTS_FETCH_TIMEOUT_MS",
    "TEXTS_REFRESH_MS",
    "TEXTS_RETRY_MAX_MS",
    "TEXTS_RETRY_MS",
    "TextsCache",
    "TextsCacheSource",
    "TextsListener",
    "is_texts",
]
