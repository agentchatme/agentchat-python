"""Can this key list the hosted tools?

One MCP ``initialize`` and one ``tools/list`` over Streamable HTTP to
``<api_base>/mcp`` with the bearer key — the requests a runtime makes on
connect — and the answer read. A ``401`` means the key does not
authenticate; a transport error means the endpoint is not reachable from
here; a list without the tools a turn needs means a turn could not look.
Nothing is called on the agent's behalf.

For an integration's ``status`` / ``doctor``, and for a readiness check
before starting turns (a turn that cannot look must not count as one that
looked). The TypeScript kit's ``probeHostedTools`` is the twin.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import httpx

from .._client_identity import AgentChatClientIdentity, client_identity_headers

HOSTED_MCP_PATH = "/mcp"
_MCP_PROTOCOL_VERSION = "2025-06-18"
REQUIRED_HOSTED_TOOLS: tuple[str, ...] = ("agentchat_inbox", "agentchat_decide")
"""What a reply turn needs at the least: read the tray and record a verdict."""
_DEFAULT_TIMEOUT_MS = 10_000


@dataclass(frozen=True)
class ProbeHostedToolsResult:
    ok: bool
    """The key authenticated and every required tool is listed."""
    status: int | None = None
    server_name: str | None = None
    server_version: str | None = None
    protocol_version: str | None = None
    tools: list[str] = field(default_factory=list)
    """Every tool name listed."""
    missing: list[str] = field(default_factory=list)
    """Required tools that were not listed."""
    error: str | None = None


def hosted_mcp_url(api_base: str) -> str:
    return f"{api_base.rstrip('/')}{HOSTED_MCP_PATH}"


def _read_result(text: str) -> dict[str, Any] | None:
    """The JSON-RPC result from a JSON body or an SSE body (``data:`` lines)."""
    candidates: list[str] = []
    trimmed = text.strip()
    if trimmed.startswith("{"):
        candidates.append(trimmed)
    for line in text.splitlines():
        if line.startswith("data:"):
            candidates.append(line[5:].strip())
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except ValueError:
            continue
        result = parsed.get("result") if isinstance(parsed, dict) else None
        if isinstance(result, dict):
            return result
    return None


def _headers(api_key: str, identity: AgentChatClientIdentity | None) -> dict[str, str]:
    return {
        "authorization": f"Bearer {api_key}",
        "content-type": "application/json",
        "accept": "application/json, text/event-stream",
        **client_identity_headers(identity),
    }


def _body(request_id: int, method: str, params: dict[str, Any]) -> str:
    return json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})


def _init_params(identity: AgentChatClientIdentity | None) -> dict[str, Any]:
    return {
        "protocolVersion": _MCP_PROTOCOL_VERSION,
        "capabilities": {},
        "clientInfo": {
            "name": identity.name if identity is not None else "python_sdk",
            "version": (identity.version if identity is not None else None) or "0",
        },
    }


def _after_initialize(
    response: httpx.Response,
) -> tuple[ProbeHostedToolsResult | None, dict[str, Any], dict[str, str]]:
    """``(a failed result or None, the server facts, headers for tools/list)``."""
    if not response.is_success:
        error = (
            "the key does not authenticate against the hosted MCP server"
            if response.status_code == 401
            else f"HTTP {response.status_code}"
        )
        return ProbeHostedToolsResult(ok=False, status=response.status_code, error=error), {}, {}
    result = _read_result(response.text)
    if result is None:
        failed = ProbeHostedToolsResult(
            ok=False, status=response.status_code, error="no initialize result in the answer"
        )
        return failed, {}, {}
    raw_info = result.get("serverInfo")
    info: dict[str, Any] = raw_info if isinstance(raw_info, dict) else {}
    protocol = result.get("protocolVersion")
    facts: dict[str, Any] = {
        "server_name": info.get("name") if isinstance(info.get("name"), str) else None,
        "server_version": info.get("version") if isinstance(info.get("version"), str) else None,
        "protocol_version": protocol if isinstance(protocol, str) else _MCP_PROTOCOL_VERSION,
    }
    extra = {"mcp-protocol-version": facts["protocol_version"]}
    session = response.headers.get("mcp-session-id")
    if session:
        extra["mcp-session-id"] = session
    return None, facts, extra


def _after_list(
    response: httpx.Response, facts: dict[str, Any], require: tuple[str, ...]
) -> ProbeHostedToolsResult:
    if not response.is_success:
        return ProbeHostedToolsResult(
            ok=False,
            status=response.status_code,
            error=f"tools/list: HTTP {response.status_code}",
            **facts,
        )
    result = _read_result(response.text)
    raw = result.get("tools") if result is not None else None
    if not isinstance(raw, list):
        return ProbeHostedToolsResult(
            ok=False,
            status=response.status_code,
            error="no tools/list result in the answer",
            **facts,
        )
    tools = [
        tool["name"] for tool in raw if isinstance(tool, dict) and isinstance(tool.get("name"), str)
    ]
    missing = [name for name in require if name not in tools]
    return ProbeHostedToolsResult(
        ok=not missing,
        status=response.status_code,
        tools=tools,
        missing=missing,
        error=f"the hosted tools do not list {', '.join(missing)}" if missing else None,
        **facts,
    )


def probe_hosted_tools(
    *,
    api_base: str,
    api_key: str,
    require: tuple[str, ...] | list[str] = REQUIRED_HOSTED_TOOLS,
    client_identity: AgentChatClientIdentity | None = None,
    timeout_ms: int = _DEFAULT_TIMEOUT_MS,
    http_client: httpx.Client | None = None,
) -> ProbeHostedToolsResult:
    """Probe the hosted tools with a blocking HTTP client. Never raises."""
    url = hosted_mcp_url(api_base)
    headers = _headers(api_key, client_identity)
    owns = http_client is None
    client = http_client or httpx.Client(timeout=timeout_ms / 1000.0)
    try:
        init = client.post(url, headers=headers, content=_body(1, "initialize", _init_params(client_identity)))
        failed, facts, extra = _after_initialize(init)
        if failed is not None:
            return failed
        listed = client.post(url, headers={**headers, **extra}, content=_body(2, "tools/list", {}))
        return _after_list(listed, facts, tuple(require))
    except httpx.TimeoutException:
        return ProbeHostedToolsResult(ok=False, error="no answer from the hosted MCP server in time")
    except Exception as err:
        return ProbeHostedToolsResult(ok=False, error=str(err) or type(err).__name__)
    finally:
        if owns:
            client.close()


async def probe_hosted_tools_async(
    *,
    api_base: str,
    api_key: str,
    require: tuple[str, ...] | list[str] = REQUIRED_HOSTED_TOOLS,
    client_identity: AgentChatClientIdentity | None = None,
    timeout_ms: int = _DEFAULT_TIMEOUT_MS,
    http_client: httpx.AsyncClient | None = None,
) -> ProbeHostedToolsResult:
    """:func:`probe_hosted_tools` on a loop. Never raises."""
    url = hosted_mcp_url(api_base)
    headers = _headers(api_key, client_identity)
    owns = http_client is None
    client = http_client or httpx.AsyncClient(timeout=timeout_ms / 1000.0)
    try:
        init = await client.post(
            url, headers=headers, content=_body(1, "initialize", _init_params(client_identity))
        )
        failed, facts, extra = _after_initialize(init)
        if failed is not None:
            return failed
        listed = await client.post(url, headers={**headers, **extra}, content=_body(2, "tools/list", {}))
        return _after_list(listed, facts, tuple(require))
    except httpx.TimeoutException:
        return ProbeHostedToolsResult(ok=False, error="no answer from the hosted MCP server in time")
    except Exception as err:
        return ProbeHostedToolsResult(ok=False, error=str(err) or type(err).__name__)
    finally:
        if owns:
            await client.aclose()


__all__ = [
    "HOSTED_MCP_PATH",
    "REQUIRED_HOSTED_TOOLS",
    "ProbeHostedToolsResult",
    "hosted_mcp_url",
    "probe_hosted_tools",
    "probe_hosted_tools_async",
]
