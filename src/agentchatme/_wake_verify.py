"""Verify a wake the platform POSTed to your wake webhook.

A wake webhook (``client.wake_webhook.set(...)``) is framed in the dialect
the subscription chose. The dialects whose receiver is code the agent runs,
and that carry the JSON wake payload, are verified here:

``agentchat``
    ``Authorization: Bearer <secret>``.
``hermes``
    ``X-Webhook-Timestamp`` + ``X-Webhook-Signature-V2`` = hex
    HMAC-SHA256(secret, ``"<timestamp>.<body>"``), inside the replay window.
``standard-webhooks``
    ``webhook-id`` / ``webhook-timestamp`` / ``webhook-signature``
    (``v1,<base64 HMAC-SHA256(key, "<id>.<timestamp>.<body>")>``; a
    ``whsec_`` secret is base64-decoded into the key, any other is its UTF-8
    bytes; the header may list several space-separated signatures).

The body is checked as the exact bytes received, every comparison is
constant-time, and header names are case-insensitive. The payload is
returned only when it is a JSON object of type ``agentchat.wake``. A wake
carries ids, counts and handles, never message bodies: after verifying,
check that ``payload.handle`` is yours and read the tray. Never log the raw
headers or body with a failure. Conformance:
``conformance/v1/wake-webhook.cases.json``.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import re
import time
from collections.abc import Mapping
from typing import Any, Callable, Literal

from pydantic import ValidationError

from .types.wake_webhook import WakeWebhookPayload

WakeWebhookVerifyDialect = Literal["agentchat", "hermes", "standard-webhooks"]

WakeWebhookVerifyReason = Literal[
    "missing_signature",
    "malformed_signature",
    "timestamp_skew",
    "bad_signature",
    "malformed_payload",
]

DEFAULT_WAKE_TOLERANCE_SECONDS = 300
"""Replay window for the signed dialects (the Hermes adapter's and the
Standard Webhooks default)."""

_DIGITS = re.compile(r"^\d+$")
_HEX = re.compile(r"^[0-9a-fA-F]+$")
_BEARER = re.compile(r"^Bearer\s+(\S+)\s*$", re.IGNORECASE)


class WakeWebhookVerificationError(Exception):
    """A wake webhook request failed verification. ``reason`` is a stable
    tag to log; never log the request itself."""

    def __init__(self, reason: WakeWebhookVerifyReason, message: str | None = None) -> None:
        super().__init__(message or f"wake webhook verification failed: {reason}")
        self.reason: WakeWebhookVerifyReason = reason


def verify_wake_webhook(
    *,
    dialect: WakeWebhookVerifyDialect | str,
    body: str | bytes,
    headers: Mapping[str, Any],
    secret: str,
    tolerance_seconds: int = DEFAULT_WAKE_TOLERANCE_SECONDS,
    now: Callable[[], float] | None = None,
) -> WakeWebhookPayload:
    """Verify one wake webhook request and return its payload.

    ``body`` is the raw request body exactly as received (do not parse it
    first). ``headers`` is any mapping of the request headers (a plain dict,
    ``httpx.Headers``, a framework's headers object). ``secret`` is the one
    you passed to ``client.wake_webhook.set``. ``tolerance_seconds`` is the
    replay window for the signed dialects (``0`` disables the check, not
    recommended); ``now`` is the receiver's clock in unix seconds (default
    :func:`time.time`).

    Raises :class:`WakeWebhookVerificationError` on any failure.
    """
    raw = body if isinstance(body, bytes) else body.encode("utf-8")
    now_seconds = (now or time.time)()

    if dialect == "agentchat":
        auth = _header(headers, "authorization")
        if auth is None or not auth.strip():
            raise WakeWebhookVerificationError("missing_signature")
        match = _BEARER.match(auth.strip())
        if match is None:
            raise WakeWebhookVerificationError("malformed_signature")
        given = hashlib.sha256(match.group(1).encode("utf-8")).digest()
        wanted = hashlib.sha256(secret.encode("utf-8")).digest()
        if not hmac.compare_digest(given, wanted):
            raise WakeWebhookVerificationError("bad_signature")
    elif dialect == "hermes":
        ts = _header(headers, "x-webhook-timestamp")
        sig = _header(headers, "x-webhook-signature-v2")
        if ts is None or sig is None:
            raise WakeWebhookVerificationError("missing_signature")
        timestamp, signature = ts.strip(), sig.strip()
        if not _DIGITS.match(timestamp) or not _HEX.match(signature):
            raise WakeWebhookVerificationError("malformed_signature")
        _check_skew(int(timestamp), now_seconds, tolerance_seconds)
        expected = hmac.new(
            secret.encode("utf-8"), f"{timestamp}.".encode() + raw, hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(expected, signature.lower()):
            raise WakeWebhookVerificationError("bad_signature")
    elif dialect == "standard-webhooks":
        message_id = _header(headers, "webhook-id")
        ts = _header(headers, "webhook-timestamp")
        sig = _header(headers, "webhook-signature")
        if message_id is None or ts is None or sig is None:
            raise WakeWebhookVerificationError("missing_signature")
        timestamp = ts.strip()
        if not _DIGITS.match(timestamp):
            raise WakeWebhookVerificationError("malformed_signature")
        candidates = [
            entry.partition(",")[2]
            for entry in sig.strip().split()
            if entry.partition(",")[0] == "v1" and entry.find(",") > 0
        ]
        if not candidates:
            raise WakeWebhookVerificationError("malformed_signature")
        _check_skew(int(timestamp), now_seconds, tolerance_seconds)
        key = _standard_webhooks_key(secret)
        expected = base64.b64encode(
            hmac.new(
                key, f"{message_id.strip()}.{timestamp}.".encode() + raw, hashlib.sha256
            ).digest()
        ).decode("ascii")
        matched = False
        for candidate in candidates:
            # Every candidate is compared: the time does not depend on which matched.
            if hmac.compare_digest(expected.encode("ascii"), candidate.encode("utf-8")):
                matched = True
        if not matched:
            raise WakeWebhookVerificationError("bad_signature")
    else:
        raise WakeWebhookVerificationError(
            "malformed_signature", f"wake webhook verification: unknown dialect {dialect!r}"
        )

    try:
        parsed = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as err:
        raise WakeWebhookVerificationError("malformed_payload") from err
    if not isinstance(parsed, dict) or parsed.get("type") != "agentchat.wake":
        raise WakeWebhookVerificationError("malformed_payload")
    try:
        return WakeWebhookPayload.model_validate(parsed)
    except ValidationError as err:
        raise WakeWebhookVerificationError("malformed_payload") from err


def _header(headers: Mapping[str, Any], name: str) -> str | None:
    """Case-insensitive lookup over any headers mapping. A list value (a
    framework's multi-value headers) is joined like an HTTP header."""
    getter = getattr(headers, "get", None)
    if callable(getter) and not isinstance(headers, dict):
        value = getter(name)
        if isinstance(value, str):
            return value
    for key, value in headers.items():
        if not isinstance(key, str) or key.lower() != name:
            continue
        if isinstance(value, str):
            return value
        if isinstance(value, (list, tuple)) and value and all(isinstance(v, str) for v in value):
            return ", ".join(value)
    return None


def _check_skew(timestamp: int, now_seconds: float, tolerance: int) -> None:
    if tolerance > 0 and abs(now_seconds - timestamp) > tolerance:
        raise WakeWebhookVerificationError("timestamp_skew")


def _standard_webhooks_key(secret: str) -> bytes:
    if not secret.startswith("whsec_"):
        return secret.encode("utf-8")
    encoded = secret[len("whsec_") :].strip().replace("-", "+").replace("_", "/")
    try:
        return base64.b64decode(encoded + "=" * (-len(encoded) % 4))
    except (binascii.Error, ValueError) as err:
        raise WakeWebhookVerificationError("malformed_signature") from err


__all__ = [
    "DEFAULT_WAKE_TOLERANCE_SECONDS",
    "WakeWebhookVerificationError",
    "WakeWebhookVerifyDialect",
    "WakeWebhookVerifyReason",
    "verify_wake_webhook",
]
