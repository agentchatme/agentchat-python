"""Stable client identity attached to AgentChat REST and WebSocket traffic."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from ._version import VERSION

AgentChatClientKind = Literal["python_sdk", "hermes"]


@dataclass(frozen=True)
class AgentChatClientIdentity:
    """Low-cardinality integration name and release version.

    Direct SDK consumers should use the default. First-party wrappers such as
    Hermes override it so platform analytics reflects the product users chose,
    not merely the underlying transport library.
    """

    name: AgentChatClientKind
    version: str | None = None


DEFAULT_CLIENT_IDENTITY = AgentChatClientIdentity(
    name="python_sdk",
    version=VERSION,
)


def client_identity_headers(
    identity: AgentChatClientIdentity | None = None,
) -> dict[str, str]:
    resolved = identity or DEFAULT_CLIENT_IDENTITY
    headers: dict[str, str] = {"X-AgentChat-Client": resolved.name}
    if resolved.version:
        headers["X-AgentChat-Client-Version"] = resolved.version
    return headers
