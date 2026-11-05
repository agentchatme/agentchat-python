"""Per-call options and URL helpers shared by the client and its resources."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlencode


@dataclass
class CallOptions:
    """Per-call overrides accepted by every client method.

    ``idempotency_key`` supplies an explicit ``Idempotency-Key`` header —
    any UUID/ULID works. Reusing the same key makes the call safe to retry:
    the server returns the original outcome rather than double-executing.
    """

    timeout_ms: int | None = None
    idempotency_key: str | None = None


def encode_segment(segment: str) -> str:
    """URL-path segment encoder that matches ``encodeURIComponent``."""
    return quote(segment, safe="")


def query_string(params: dict[str, Any]) -> str:
    """Build a query-string, skipping ``None`` values."""
    filtered = {k: v for k, v in params.items() if v is not None}
    if not filtered:
        return ""
    return "?" + urlencode(filtered, doseq=True)
