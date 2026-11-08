"""Per-call options and URL helpers shared by the client and its resources."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urlencode

if TYPE_CHECKING:
    from ._http import RetryOption


@dataclass
class CallOptions:
    """Per-call overrides accepted by every client method.

    ``idempotency_key`` supplies an explicit ``Idempotency-Key`` header —
    any UUID/ULID works. Reusing the same key makes the call safe to retry:
    the server returns the original outcome rather than double-executing.

    ``retry`` overrides the client's retry behaviour for this one call:
    ``"never"`` (one attempt, e.g. a probe that must answer now), ``"auto"``
    (retry even a non-idempotent method under the client's policy), or a
    :class:`~agentchatme.RetryPolicy` of its own. ``None`` (default) keeps
    the client's rule: idempotent methods, and calls with an idempotency
    key, retry under the client's policy.
    """

    timeout_ms: int | None = None
    idempotency_key: str | None = None
    retry: RetryOption | None = None


def encode_segment(segment: str) -> str:
    """URL-path segment encoder that matches ``encodeURIComponent``."""
    return quote(segment, safe="")


def query_string(params: dict[str, Any]) -> str:
    """Build a query-string, skipping ``None`` values."""
    filtered = {k: v for k, v in params.items() if v is not None}
    if not filtered:
        return ""
    return "?" + urlencode(filtered, doseq=True)
