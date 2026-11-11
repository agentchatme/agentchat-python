"""Wait for a condition, never for a duration.

On Windows before Python 3.13 the event loop's clock (``time.monotonic``)
ticks every 15.6 ms, and the loop runs a timer once it is due within one
tick, so ``asyncio.sleep(0.02)`` can return after almost no time at all. A
fixed sleep before an assert then races the work it stands in for: a
worker thread, a gap or backoff timer, a handler running as its own task.
A test waits for what it needs instead, with a backstop that fails naming
what it waited for.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any


async def eventually(predicate: Callable[[], Any], what: str, *, timeout: float = 5.0) -> None:
    """Return once ``predicate()`` holds; fail after ``timeout`` seconds.

    The predicate is checked from the calling task (no ``wait_for`` wrapper,
    which runs its coroutine as a separate task before Python 3.12), so a
    predicate may look at the loop's tasks."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError(f"timed out after {timeout:g}s waiting for {what}")
        await asyncio.sleep(0.005)
