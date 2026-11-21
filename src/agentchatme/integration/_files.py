"""Atomic file replacement for the kit's files (the texts cache, the manual),
and the checked replacement of a file someone else owns (the identity
block's follow).

Tmp file + rename in the same directory: a reader sees the old file or the
new one, never part of one, and a crash leaves the old file or a stray tmp.
A failed write removes its tmp file and leaves the old file exactly as it
was. The tmp name carries a random suffix and is created exclusively
(``O_CREAT | O_EXCL``: an existing name, a planted symlink included, is
refused rather than followed), its contents are flushed before the rename,
and on Windows a rename onto a file another process holds open (a scanner,
an editor) is retried for about 100 ms.
"""

from __future__ import annotations

import contextlib
import os
import secrets
import stat
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Literal

_RENAME_ATTEMPTS = 6
_RENAME_DELAY_S = 0.02


def _tmp_path_for(path: Path) -> Path:
    return path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(6)}.tmp")


def rename_with_retry(
    source: str | Path,
    target: str | Path,
    *,
    rename: Callable[[Path, Path], None] | None = None,
    retry: bool | None = None,
    attempts: int = _RENAME_ATTEMPTS,
    delay_s: float = _RENAME_DELAY_S,
) -> None:
    """``os.replace``, retried a few times on a transient ``PermissionError``
    (Windows sharing violations). ``retry`` defaults to Windows only."""
    do_rename: Callable[[Path, Path], None] = rename or (lambda a, b: os.replace(a, b))
    source, target = Path(source), Path(target)
    tries = attempts if (sys.platform == "win32" if retry is None else retry) else 1
    for attempt in range(1, tries + 1):
        try:
            do_rename(source, target)
            return
        except PermissionError:
            if attempt >= tries:
                raise
            time.sleep(delay_s)


def write_atomic(path: str | Path, data: bytes | str, *, mode: int = 0o644) -> None:
    """Replace ``path`` with ``data`` atomically (see the module docstring).
    ``str`` data is written as UTF-8."""
    path = Path(path)
    if isinstance(data, str):
        data = data.encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = _tmp_path_for(path)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
    try:
        fd = os.open(tmp, flags, mode)
        with os.fdopen(fd, "wb", closefd=True) as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        rename_with_retry(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            tmp.unlink()
        raise


# ─── Replacing a file someone else owns ─────────────────────────────────────
#
# For a file the kit did not create and must not damage: the user's
# instruction file (``anchor.follow``). Never creates one, keeps what an
# in-place write would have kept, and replaces it only if it still holds the
# bytes the caller decided on, looked at again at the last moment.


@dataclass(frozen=True)
class ReadForReplace:
    """A file as it was read for a replacement."""

    target: Path
    """The file at the end of any symlinks: the one replaced (a symlink stays a symlink)."""
    data: bytes
    mode: int
    """Permission bits."""
    uid: int
    gid: int


def read_for_replace(path: str | Path) -> ReadForReplace | None:
    """The file at ``path`` (through any symlinks), or ``None`` when there is
    none (a dangling symlink included). Raises :class:`OSError` when it is not
    a regular file or cannot be read."""
    try:
        target = Path(path).resolve(strict=True)
        info = os.stat(target)
    except FileNotFoundError:
        return None
    if not stat.S_ISREG(info.st_mode):
        raise IsADirectoryError(f"{path} is not a regular file")
    try:
        data = target.read_bytes()
    except FileNotFoundError:
        return None
    return ReadForReplace(
        target=target,
        data=data,
        mode=stat.S_IMODE(info.st_mode),
        uid=getattr(info, "st_uid", 0),
        gid=getattr(info, "st_gid", 0),
    )


def _is_root() -> bool:
    geteuid = getattr(os, "geteuid", None)
    return geteuid is not None and geteuid() == 0


def replace_if_unchanged(
    original: ReadForReplace,
    data: str,
    *,
    before_rename: Callable[[], None] | None = None,
) -> Literal["replaced", "changed", "gone"]:
    """Replace ``original.target`` with ``data`` (UTF-8), but only if it still
    holds ``original.data``: a temp file beside it (unguessable name, created
    exclusively, the original's permission bits, under root its owner), the
    data written and flushed, then the file read again and renamed over only
    if it is byte for byte what was decided on. ``changed``: it is not (an
    edit saved meanwhile); ``gone``: it was removed meanwhile, and is not
    made again. Raises (the file as it was, the temp file removed) when the
    file is not writable (a file the user made read-only is refused, not
    replaced) or the temp file or the rename fails. Never an in-place write:
    one that fails halfway would leave the user's file cut short."""
    target = original.target
    if not os.access(target, os.W_OK):
        if not os.path.lexists(target):
            raise FileNotFoundError(f"{target} was removed")
        raise PermissionError(f"{target} is not writable")
    tmp = _tmp_path_for(target)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
    # Only a temp file this call created is ever removed: O_EXCL refuses a name that exists.
    created = False
    renamed = False
    try:
        fd = os.open(tmp, flags, original.mode)
        created = True
        with os.fdopen(fd, "wb", closefd=True) as handle:
            fchmod = getattr(os, "fchmod", None)
            if fchmod is not None:
                fchmod(handle.fileno(), original.mode)
            fchown = getattr(os, "fchown", None)
            if fchown is not None and _is_root() and (original.uid != 0 or original.gid != 0):
                with contextlib.suppress(OSError):
                    fchown(handle.fileno(), original.uid, original.gid)
            handle.write(data.encode("utf-8"))
            handle.flush()
            os.fsync(handle.fileno())
        if before_rename is not None:
            before_rename()
        try:
            now = target.read_bytes()
        except FileNotFoundError:
            return "gone"
        if now != original.data:
            return "changed"
        rename_with_retry(tmp, target)
        renamed = True
        return "replaced"
    finally:
        if created and not renamed:
            with contextlib.suppress(OSError):
                tmp.unlink()


__all__ = ["rename_with_retry", "write_atomic"]
