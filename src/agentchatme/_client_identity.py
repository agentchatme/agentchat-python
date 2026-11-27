"""Stable client identity attached to AgentChat REST and WebSocket traffic."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Callable, Literal

from ._runtime import PlatformFacts, is_platform_value, platform_facts
from ._version import VERSION

AgentChatClientKind = Literal["python_sdk", "hermes"]


@dataclass(frozen=True)
class AgentChatClientIdentity:
    """Low-cardinality integration name and release version.

    Direct SDK consumers should use the default. First-party wrappers such as
    Hermes override it so platform analytics reflects the product users chose,
    not merely the underlying transport library.

    ``host``, ``install_id`` and ``install_method`` go in
    ``X-AgentChat-Platform`` (as ``host``, ``install``, ``method``); a value
    of another shape is left out.
    """

    name: AgentChatClientKind
    version: str | None = None
    host: str | None = None
    """The runtime this client runs inside and its version, as
    ``<runtime>/<version>`` (``hermes/0.9.2``)."""
    install_id: str | None = None
    """The install id: ``ins_`` and 22 base64url characters, generated once
    per install (random, never derived from the machine)."""
    install_method: str | None = None
    """How this client was installed (``pip``, ``marketplace``, ...), only
    when known for sure."""


DEFAULT_CLIENT_IDENTITY = AgentChatClientIdentity(
    name="python_sdk",
    version=VERSION,
)

PLATFORM_HEADER = "X-AgentChat-Platform"
"""The header that names the platform a request comes from (and the socket
HELLO's ``platform`` field)."""

_INSTALL_ID = re.compile(r"ins_[A-Za-z0-9_-]{22}")


def _is_install_id(value: object) -> bool:
    return isinstance(value, str) and _INSTALL_ID.fullmatch(value) is not None


def platform_header_value(
    identity: AgentChatClientIdentity | None = None,
    facts: PlatformFacts | None = None,
) -> str | None:
    """The ``X-AgentChat-Platform`` value: an RFC 8941 dictionary of strings,
    members in this order, each only when it has its shape:

    - ``host`` (the identity's ``host``), ``os``, ``arch``, ``runtime``
      (computed by the SDK, ``facts``), ``install`` (``install_id``),
      ``method`` (``install_method``).
    - Every value ``^[A-Za-z0-9][A-Za-z0-9._+/-]{0,63}$``; ``install``
      ``^ins_[A-Za-z0-9_-]{22}$``. A value of another shape is left out,
      never repaired. So nothing needs escaping, and the whole value stays
      under 512 bytes (at most 454).

    ``None`` when no member is left: then nothing is sent.
    """
    resolved = identity or DEFAULT_CLIENT_IDENTITY
    known = facts if facts is not None else platform_facts()
    members: tuple[tuple[str, object, Callable[[object], bool]], ...] = (
        ("host", resolved.host, is_platform_value),
        ("os", known.os, is_platform_value),
        ("arch", known.arch, is_platform_value),
        ("runtime", known.runtime, is_platform_value),
        ("install", resolved.install_id, _is_install_id),
        ("method", resolved.install_method, is_platform_value),
    )
    kept = [f'{key}="{value}"' for key, value, valid in members if valid(value)]
    return ", ".join(kept) if kept else None


def client_identity_headers(
    identity: AgentChatClientIdentity | None = None,
) -> dict[str, str]:
    resolved = identity or DEFAULT_CLIENT_IDENTITY
    headers: dict[str, str] = {"X-AgentChat-Client": resolved.name}
    if resolved.version:
        headers["X-AgentChat-Client-Version"] = resolved.version
    platform = platform_header_value(resolved)
    if platform is not None:
        headers[PLATFORM_HEADER] = platform
    return headers
