"""Atomic file replacement for the kit's files (the texts cache, the manual).

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
import sys
import time
from pathlib import Path
from typing import Callable

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


__all__ = ["rename_with_retry", "write_atomic"]
