"""Connect budget (contract v1, D8).

Over its per-agent connect budget the server accepts the WebSocket upgrade
and closes the socket with 1013 ("try again later") and a retry hint in the
close reason (``rate_limited; retry_after=<seconds>``), on both auth paths.
The client backs off for the server's delay; it never goes ``blocked``. A
1013 without the hint (the slow-consumer ``overload`` close) keeps the
ordinary backoff, as does an upgrade answered with an HTTP status (429
included: no server answers that way; a client only tolerates it).
Conformance: ``conformance/v1/connect-budget.cases.json``.
"""

from __future__ import annotations

import re

CONNECT_BUDGET_CLOSE_CODE = 1013
"""Close code the server uses when the connect budget is spent."""
CONNECT_BUDGET_MIN_DELAY_MS = 1_000
CONNECT_BUDGET_MAX_DELAY_MS = 3_600_000

_HINT_RE = re.compile(r"retry[_-]after(?:\s*[=:]\s*|\s+)(\d+)", re.IGNORECASE)


def connect_budget_delay_ms(*, code: int, reason: str) -> int | None:
    """How long to wait before the next attempt after a server close, in ms,
    when the close is a connect-budget answer; ``None`` otherwise (the
    ordinary reconnect backoff applies). Pure.

    A budget answer is a 1013 whose reason carries the first ``retry_after``
    (or ``retry-after``), any case, followed by ``=``, ``:`` or spaces and
    whole seconds. Clamped to [1 s, 1 h]; a client waits up to 10 % longer,
    never less.
    """
    if code != CONNECT_BUDGET_CLOSE_CODE:
        return None
    match = _HINT_RE.search(reason if isinstance(reason, str) else "")
    if match is None:
        return None
    ms = int(match.group(1)) * 1000
    return min(CONNECT_BUDGET_MAX_DELAY_MS, max(CONNECT_BUDGET_MIN_DELAY_MS, ms))


__all__ = [
    "CONNECT_BUDGET_CLOSE_CODE",
    "CONNECT_BUDGET_MAX_DELAY_MS",
    "CONNECT_BUDGET_MIN_DELAY_MS",
    "connect_budget_delay_ms",
]
