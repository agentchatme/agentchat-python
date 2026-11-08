"""The served manual, placed and kept current.

An integration's skill file follows the manual the server announces in the
served texts (``GET /v1/texts`` → ``manual: {url, version, sha256?}``). When
the file on disk is not the announced one, the announced address is
downloaded, checked and written over the old file atomically. The file is
its own record: its front matter (``version:``) and, when the server
announces one, the sha256 of its bytes say what is placed.

Every path is "never worse than before": offline, an error answer, a body
over the size cap, a body that is not the agentchat skill at the announced
version (or hash) — the existing file stays exactly as it is, and the next
texts refresh tries again. This package bundles no manual (every text is
served): an integration that wants a first-install fallback passes its own
``fallback`` bytes, written only when no file exists and nothing verified
could be had, or when they are the announced manual.

Where a download may go is decided by where the announcement came from
(the API base the texts were fetched from):

- an HTTPS base: every hop HTTPS, on ``agentchat.me``, ``*.agentchat.me`` or
  the base's own host — the announced address and every redirect (followed
  here, at most three, each one checked: never a plaintext hop, never off
  those hosts, never an address carrying credentials);
- a plain-HTTP base on loopback (a development server): the same hosts, and
  plain HTTP allowed only to loopback;
- any other plain-HTTP base: no download at all — an announcement that
  travelled in the clear does not get to replace the file.

The body is text for a model to read. It is never executed and never
interpreted beyond its front matter (a leading byte-order mark and CRLF line
endings read the same as LF), and it is fetched without credentials: the
manual is public, and the agent's key is not sent to an address the server
merely names. A port of agent-core ``skill/served-manual.ts``.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Literal
from urllib.parse import urljoin, urlsplit

import httpx

from ._files import write_atomic

MANUAL_SKILL_NAME = "agentchat"
"""The skill name every runtime sees: ``<skills dir>/agentchat/SKILL.md``."""
MANUAL_MAX_BYTES = 512 * 1024
"""The manual is ~60 KB; anything past this is not the manual."""
MANUAL_FETCH_TIMEOUT_MS = 10_000
"""One attempt, every hop included."""
MANUAL_MAX_REDIRECTS = 3

_MANUAL_HOME = "agentchat.me"
_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", "[::1]"})
_REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})
_SHA256_HEX = re.compile(r"^[0-9a-f]{64}$")
_FIELD_RE = re.compile(r"^([A-Za-z0-9_-]+):[ \t]*(.*?)[ \t]*$")

ManualSyncState = Literal["current", "updated", "kept", "fallback", "failed"]
"""
- ``current``  — the file already is the announced manual; nothing fetched.
- ``updated``  — the announced manual was written (downloaded, or the
  fallback copy when it is that manual).
- ``kept``     — the existing file was left as it was: nothing announced to
  compare with, or no verified copy could be had.
- ``fallback`` — no file existed and nothing verified could be had: the
  fallback copy was written.
- ``failed``   — no file existed and none could be written.
"""


@dataclass(frozen=True)
class ServedManual:
    """What the served texts announce (``texts.manual``)."""

    url: str
    version: str
    sha256: str | None = None


@dataclass(frozen=True)
class ManualSyncResult:
    state: ManualSyncState
    path: Path
    version: str | None
    """The version the file carries after the sync."""
    previous: str | None
    """The version the file carried before."""
    tampered: bool
    """The file before carried the announced version but not the announced
    bytes (its sha256 differed)."""
    served: str | None
    """The announced version; ``None`` when nothing was announced."""
    url: str | None
    source: Literal["download", "fallback"] | None
    written: bool
    up_to_date: bool | None
    """Whether the file now is the announced manual; ``None`` when nothing
    was announced."""
    error: str | None
    """Why the announced manual is not on disk (``kept`` / ``fallback`` /
    ``failed``)."""


def manual_front_matter(raw: str) -> dict[str, str] | None:
    """The YAML front matter's top-level ``key: value`` fields, or ``None``
    when the text does not open with a front matter block. Deliberately not
    a YAML parser: nothing here interprets more of a downloaded text than its
    name and version. A leading BOM and CRLF line endings read as LF."""
    text = (raw[1:] if raw.startswith("﻿") else raw).replace("\r\n", "\n")
    if not text.startswith("---\n"):
        return None
    close = text.find("\n---", 3)
    while close >= 0 and close + 4 != len(text) and text[close + 4] != "\n":
        close = text.find("\n---", close + 1)
    if close < 0:
        return None
    fields: dict[str, str] = {}
    for line in text[4:close].split("\n"):
        match = _FIELD_RE.match(line)
        if match is None:
            continue
        value = match.group(2)
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        fields[match.group(1)] = value
    return fields


def manual_version(text: str) -> str | None:
    fields = manual_front_matter(text)
    version = fields.get("version") if fields is not None else None
    return version if version else None


def _served_of(texts: Any) -> ServedManual | None:
    """The announcement from a :class:`TextsCache`, :class:`Texts`,
    :class:`TextsManual`, a mapping, or a :class:`ServedManual`."""
    if texts is None:
        return None
    if isinstance(texts, ServedManual):
        return texts
    current = getattr(texts, "current", None)
    if callable(current):
        texts = current()
        if texts is None:
            return None
    manual: Any = texts.get("manual", texts) if isinstance(texts, Mapping) else getattr(texts, "manual", texts)
    data: Any = manual if isinstance(manual, Mapping) else getattr(manual, "__dict__", {})
    if hasattr(manual, "model_dump"):
        data = manual.model_dump(mode="json")
    url, version = data.get("url"), data.get("version")
    if not isinstance(url, str) or not isinstance(version, str):
        return None
    sha = data.get("sha256")
    return ServedManual(url=url, version=version, sha256=sha if isinstance(sha, str) else None)


@dataclass(frozen=True)
class _Origin:
    host: str
    plain_loopback: bool


def _origin(api_base: str | None) -> _Origin | str:
    """Whether, and how, an announcement from ``api_base`` may be acted on;
    a reason string when it may not."""
    if not api_base:
        return "no API base is known for the served texts"
    parts = urlsplit(api_base)
    host = (parts.hostname or "").lower()
    if parts.scheme in ("https", "wss") and host:
        return _Origin(host=host, plain_loopback=False)
    if parts.scheme in ("http", "ws") and host in _LOOPBACK_HOSTS:
        return _Origin(host=host, plain_loopback=True)
    return (
        f"the served texts came from {parts.scheme}://{parts.netloc}, which is not HTTPS, "
        "so the manual is not downloaded"
    )


def _refusal(url: str, origin: _Origin) -> str | None:
    """Why ``url`` may not be fetched for a manual announced from ``origin``."""
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    if host != _MANUAL_HOME and not host.endswith(f".{_MANUAL_HOME}") and host != origin.host:
        return f"{parts.netloc} is neither {_MANUAL_HOME} nor the API's own host"
    if parts.username or parts.password:
        return "the address carries credentials"
    if parts.scheme == "https":
        return None
    if parts.scheme == "http" and origin.plain_loopback and host in _LOOPBACK_HOSTS:
        return None
    return (
        f"{parts.scheme}:// to {parts.netloc} — the manual is fetched over HTTPS only "
        "(plain HTTP only to loopback, from a loopback API base)"
    )


def _announced_hash(served: ServedManual) -> str | None:
    """Lower-cased announced hash; ``None`` when none; raises
    :class:`ValueError` when one is announced but malformed."""
    if served.sha256 is None:
        return None
    value = served.sha256.strip().lower()
    if not _SHA256_HEX.match(value):
        raise ValueError("the announced sha256 is not a hex sha256")
    return value


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _is_announced(data: bytes, served: ServedManual, digest: str | None) -> bool:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        return False
    return manual_version(text) == served.version and (digest is None or _sha256(data) == digest)


@dataclass(frozen=True)
class _Local:
    exists: bool
    data: bytes
    version: str | None


def _read_local(path: Path) -> _Local:
    if not path.exists():
        return _Local(False, b"", None)
    try:
        data = path.read_bytes()
    except OSError:
        return _Local(True, b"", None)
    try:
        return _Local(True, data, manual_version(data.decode("utf-8")))
    except UnicodeDecodeError:
        return _Local(True, data, None)


def _check_manual_text(text: str, announced: str) -> None:
    if "\x00" in text:
        raise ValueError("the body is not text")
    fields = manual_front_matter(text)
    if fields is None:
        raise ValueError("the body has no front matter — not the manual")
    if fields.get("name") != MANUAL_SKILL_NAME:
        raise ValueError(
            f'the front matter names "{fields.get("name", "")}", not the {MANUAL_SKILL_NAME} skill'
        )
    version = manual_version(text)
    if version != announced:
        raise ValueError(f"the body is version {version or '(none)'}, the server announced {announced}")


def sync_manual(
    *,
    path: str | Path,
    served: Any,
    api_base: str | None,
    fallback: bytes | str | None = None,
    timeout_ms: int = MANUAL_FETCH_TIMEOUT_MS,
    max_bytes: int = MANUAL_MAX_BYTES,
    http_client: httpx.Client | None = None,
) -> ManualSyncResult:
    """Bring the skill file at ``path`` to the announced manual (``served``:
    ``texts.manual``, or anything :class:`ManualSync` accepts as texts).
    ``api_base`` is the API base the texts came from: it decides where a
    download may go. Blocking; never raises — the result says what happened
    and, when the announced manual is not on disk, why."""
    return _Syncer(
        target=Path(path),
        api_base=api_base,
        fallback=fallback.encode("utf-8") if isinstance(fallback, str) else fallback,
        timeout_ms=timeout_ms,
        max_bytes=max_bytes,
        http_client=http_client,
    ).sync(_served_of(served))


class ManualSync:
    """Keeps the skill file at the manual the texts announce.

    ``texts`` is a :class:`~agentchatme.integration.TextsCache` (anything
    with ``current()``, ``on_change()`` and ``api_base``); ``target_path``
    the skill file (``<skills dir>/agentchat/SKILL.md``; the integration
    knows where). :meth:`start` syncs once and then on every texts change
    (the texts' own cadence: daily, sooner while the server does not
    answer). Calls are serialised and run in a worker thread; :meth:`stop`
    stops following. ``on_result`` hears every result that is up to date and
    every failure once per condition.
    """

    def __init__(
        self,
        *,
        texts: Any,
        target_path: str | Path,
        fallback: bytes | str | None = None,
        timeout_ms: int = MANUAL_FETCH_TIMEOUT_MS,
        max_bytes: int = MANUAL_MAX_BYTES,
        http_client: httpx.Client | None = None,
        on_result: Callable[[ManualSyncResult], Any] | None = None,
    ) -> None:
        self.texts = texts
        self.target_path = Path(target_path)
        self._fallback = fallback.encode("utf-8") if isinstance(fallback, str) else fallback
        self._timeout_ms = timeout_ms
        self._max_bytes = max_bytes
        self._http_client = http_client
        self._on_result = on_result
        self._lock = asyncio.Lock()
        self._unsubscribe: Callable[[], None] | None = None
        self._stopped = False
        self._last_error: str | None = None
        self._last: ManualSyncResult | None = None

    @property
    def result(self) -> ManualSyncResult | None:
        """The latest result, for an integration's status."""
        return self._last

    async def start(self) -> ManualSyncResult | None:
        """Sync now, then follow every texts change. Returns the first result."""
        self._stopped = False
        if self._unsubscribe is None:
            on_change = getattr(self.texts, "on_change", None)
            if callable(on_change):
                self._unsubscribe = on_change(
                    lambda texts, _source=None: asyncio.ensure_future(self.sync(texts))
                )
        return await self.sync()

    def stop(self) -> None:
        self._stopped = True
        if self._unsubscribe is not None:
            self._unsubscribe()
            self._unsubscribe = None

    async def sync(self, served: Any = None) -> ManualSyncResult | None:
        """One serialised sync against ``served`` (default: the texts in
        hand). ``None`` when nothing is in hand or stopped."""
        announcement = _served_of(served if served is not None else self.texts)
        async with self._lock:
            if announcement is None or self._stopped:
                return None
            api_base = getattr(self.texts, "api_base", None)
            syncer = _Syncer(
                target=self.target_path,
                api_base=api_base if isinstance(api_base, str) else None,
                fallback=self._fallback,
                timeout_ms=self._timeout_ms,
                max_bytes=self._max_bytes,
                http_client=self._http_client,
            )
            result = await asyncio.to_thread(syncer.sync, announcement)
            self._last = result
            if (result.up_to_date is True or result.error != self._last_error) and self._on_result:
                with contextlib.suppress(Exception):
                    self._on_result(result)
            self._last_error = None if result.up_to_date is True else result.error
            return result


class _Syncer:
    """One sync's settings; :meth:`sync` does the work."""

    def __init__(
        self,
        *,
        target: Path,
        api_base: str | None,
        fallback: bytes | None,
        timeout_ms: int,
        max_bytes: int,
        http_client: httpx.Client | None,
    ) -> None:
        self.target_path = target
        self.api_base = api_base
        self.bundled = fallback
        self.timeout_ms = timeout_ms
        self.max_bytes = max_bytes
        self._http_client = http_client

    def sync(self, served: ServedManual | None) -> ManualSyncResult:
        path = self.target_path
        local = _read_local(path)

        def result(**kw: Any) -> ManualSyncResult:
            base: dict[str, Any] = {
                "path": path,
                "previous": local.version,
                "served": served.version if served else None,
                "url": served.url if served else None,
                "tampered": False,
            }
            base.update(kw)
            return ManualSyncResult(**base)

        tampered = False
        error: str | None = None
        if served is not None:
            try:
                digest = _announced_hash(served)
            except ValueError as err:
                error = str(err)
            else:
                if local.exists and local.version == served.version:
                    if digest is None or _sha256(local.data) == digest:
                        return result(
                            state="current", version=local.version, source=None,
                            written=False, up_to_date=True, error=None,
                        )
                    tampered = True
                origin = _origin(self.api_base)
                if isinstance(origin, str):
                    error = origin
                else:
                    data: bytes | None = None
                    source: Literal["download", "fallback"] = "fallback"
                    if self.bundled is not None and _is_announced(self.bundled, served, digest):
                        data = self.bundled
                    else:
                        try:
                            data = self._download(served, digest, origin)
                            source = "download"
                        except Exception as err:
                            error = str(err) or type(err).__name__
                    if data is not None:
                        try:
                            write_atomic(path, data, mode=0o644)
                            return result(
                                state="updated", version=served.version, source=source,
                                written=True, up_to_date=True, error=None, tampered=tampered,
                            )
                        except OSError as err:
                            error = f"could not write {path}: {err}"

        if local.exists:
            return result(
                state="kept", version=local.version, source=None, written=False,
                up_to_date=None if served is None else False, error=error, tampered=tampered,
            )
        if self.bundled is None:
            return result(
                state="failed", version=None, source=None, written=False,
                up_to_date=None if served is None else False,
                error=error or "no manual could be had and no fallback was given",
                tampered=tampered,
            )
        try:
            write_atomic(path, self.bundled, mode=0o644)
        except OSError as err:
            return result(
                state="failed", version=None, source=None, written=False,
                up_to_date=None if served is None else False, error=str(err), tampered=tampered,
            )
        bundled_version = _version_of(self.bundled)
        bundled_is_served = False
        if served is not None:
            try:
                bundled_is_served = _is_announced(self.bundled, served, _announced_hash(served))
            except ValueError:
                bundled_is_served = False
        return result(
            state="fallback", version=bundled_version, source="fallback", written=True,
            up_to_date=None if served is None else bundled_is_served, error=error,
            tampered=tampered,
        )

    # ─── the download ────────────────────────────────────────────────────

    def _download(self, served: ServedManual, digest: str | None, origin: _Origin) -> bytes:
        url = served.url
        refused = _refusal(url, origin)
        if refused is not None:
            raise ValueError(f"refusing {url}: {refused}")
        deadline = time.monotonic() + self.timeout_ms / 1000.0
        owns = self._http_client is None
        client = self._http_client or httpx.Client(follow_redirects=False)
        try:
            data = self._fetch_hops(client, served.url, origin, deadline)
        finally:
            if owns:
                client.close()
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as err:
            raise ValueError("the body is not UTF-8 text") from err
        _check_manual_text(text, served.version)
        if digest is not None:
            got = _sha256(data)
            if got != digest:
                raise ValueError(f"the body's sha256 is {got}, the server announced {digest}")
        return data

    def _fetch_hops(
        self, client: httpx.Client, start: str, origin: _Origin, deadline: float
    ) -> bytes:
        url = start
        for hop in range(MANUAL_MAX_REDIRECTS + 1):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"no answer from {start} within {self.timeout_ms} ms")
            with client.stream(
                "GET",
                url,
                headers={"accept": "text/markdown, text/plain;q=0.9, */*;q=0.1"},
                follow_redirects=False,
                timeout=remaining,
            ) as response:
                if response.status_code in _REDIRECT_STATUSES:
                    if hop >= MANUAL_MAX_REDIRECTS:
                        raise ValueError(f"more than {MANUAL_MAX_REDIRECTS} redirects from {start}")
                    location = response.headers.get("location")
                    if not location:
                        raise ValueError(f"HTTP {response.status_code} from {url} with no Location")
                    target = urljoin(url, location)
                    why = _refusal(target, origin)
                    if why is not None:
                        raise ValueError(f"redirected to {target}: {why}")
                    url = target
                    continue
                if not 200 <= response.status_code < 300:
                    raise ValueError(f"HTTP {response.status_code} from {url}")
                declared = response.headers.get("content-length")
                if declared is not None and declared.isdigit() and int(declared) > self.max_bytes:
                    raise ValueError(f"{declared} bytes declared, over the {self.max_bytes}-byte cap")
                chunks: list[bytes] = []
                total = 0
                for chunk in response.iter_bytes():
                    total += len(chunk)
                    if total > self.max_bytes:
                        raise ValueError(f"the body is over the {self.max_bytes}-byte cap")
                    if time.monotonic() > deadline:
                        raise TimeoutError(f"no answer from {start} within {self.timeout_ms} ms")
                    chunks.append(chunk)
                return b"".join(chunks)
        raise ValueError(f"more than {MANUAL_MAX_REDIRECTS} redirects from {start}")


def _version_of(data: bytes) -> str | None:
    try:
        return manual_version(data.decode("utf-8"))
    except UnicodeDecodeError:
        return None


__all__ = [
    "MANUAL_FETCH_TIMEOUT_MS",
    "MANUAL_MAX_BYTES",
    "MANUAL_MAX_REDIRECTS",
    "MANUAL_SKILL_NAME",
    "ManualSync",
    "ManualSyncResult",
    "ManualSyncState",
    "ServedManual",
    "manual_front_matter",
    "manual_version",
    "sync_manual",
]
