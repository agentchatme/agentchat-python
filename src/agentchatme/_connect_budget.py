"""Connect budget (contract v1, D8).

Over its per-agent connect budget the server answers the WebSocket upgrade
with HTTP 429 and ``Retry-After``, or closes the socket with 1013 ("try
again later") and a retry hint in the close reason after the HELLO. Either
way the client backs off for the server's delay; it never goes
``blocked``. Conformance: ``conformance/v1/connect-budget.cases.json``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Literal, Union

CONNECT_BUDGET_CLOSE_CODE = 1013
"""Close code the server uses when the connect budget is spent."""
CONNECT_BUDGET_DEFAULT_DELAY_MS = 60_000
"""Delay when the server gave none (the budget's shortest window is a minute)."""
CONNECT_BUDGET_MIN_DELAY_MS = 1_000
CONNECT_BUDGET_MAX_DELAY_MS = 3_600_000

_HINT_RE = re.compile(r"retry[_-]after(?:\s*[=:]\s*|\s+)(\d+)", re.IGNORECASE)
_DELTA_SECONDS = re.compile(r"^\d+$")
_HAS_ALPHA = re.compile(r"[A-Za-z]")


@dataclass(frozen=True)
class BudgetClose:
    """A server close: ``code`` and ``reason`` as received."""

    code: int
    reason: str
    kind: Literal["close"] = "close"


@dataclass(frozen=True)
class BudgetUpgrade:
    """An upgrade answered with an HTTP status instead of 101."""

    status: int
    retry_after: str | None
    """The ``Retry-After`` header as received, or ``None``."""
    now_ms: int | None = None
    """Epoch ms an HTTP-date ``Retry-After`` is measured from. Default: now."""
    kind: Literal["upgrade"] = "upgrade"


ConnectBudgetEvent = Union[BudgetClose, BudgetUpgrade]


def _retry_after_ms(raw: str | None, now_ms: int | None) -> int | None:
    """``Retry-After`` per RFC 9110 (whole seconds, or an HTTP date measured
    from ``now_ms``), in ms; ``None`` when missing or unusable."""
    if raw is None:
        return None
    value = raw.strip()
    if not value:
        return None
    if _DELTA_SECONDS.match(value):
        return int(value) * 1000
    if not _HAS_ALPHA.search(value):
        return None
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    now = now_ms if now_ms is not None else int(datetime.now(timezone.utc).timestamp() * 1000)
    return max(0, int(when.timestamp() * 1000) - now)


def connect_budget_delay_ms(event: ConnectBudgetEvent) -> int | None:
    """How long to wait before the next attempt when the server said "over
    budget", in ms; ``None`` when the event is not a budget answer (the
    ordinary reconnect backoff applies). Pure.

    Close 1013: the first ``retry_after`` (or ``retry-after``), any case,
    followed by ``=``, ``:`` or spaces and whole seconds. Upgrade 429:
    ``Retry-After``. No usable hint: one minute. Clamped to [1 s, 1 h].
    """
    ms: int | None
    if isinstance(event, BudgetClose):
        if event.code != CONNECT_BUDGET_CLOSE_CODE:
            return None
        match = _HINT_RE.search(event.reason if isinstance(event.reason, str) else "")
        ms = int(match.group(1)) * 1000 if match else None
    else:
        if event.status != 429:
            return None
        ms = _retry_after_ms(event.retry_after, event.now_ms)
    if ms is None:
        ms = CONNECT_BUDGET_DEFAULT_DELAY_MS
    return min(CONNECT_BUDGET_MAX_DELAY_MS, max(CONNECT_BUDGET_MIN_DELAY_MS, ms))


__all__ = [
    "CONNECT_BUDGET_CLOSE_CODE",
    "CONNECT_BUDGET_DEFAULT_DELAY_MS",
    "CONNECT_BUDGET_MAX_DELAY_MS",
    "CONNECT_BUDGET_MIN_DELAY_MS",
    "BudgetClose",
    "BudgetUpgrade",
    "ConnectBudgetEvent",
    "connect_budget_delay_ms",
]
