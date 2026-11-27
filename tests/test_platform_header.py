"""X-AgentChat-Platform: an RFC 8941 dictionary of strings, host / os / arch /
runtime / install / method, each only when it has its shape. The socket
carries the same value as the HELLO's ``platform`` field. Also the declared
sign-up fields (``runtime``, ``heard_from``, ``referred_by``).

The TypeScript SDK's tests/platform-header.test.ts is the twin."""

from __future__ import annotations

import json
import os
import platform
import re
import sys
from types import SimpleNamespace
from typing import Any, Callable

import httpx
import pytest

from agentchatme import (
    AgentChatClient,
    AgentChatClientIdentity,
    AsyncAgentChatClient,
    RealtimeClient,
)
from agentchatme import _runtime as runtime_module
from agentchatme._client_identity import (
    DEFAULT_CLIENT_IDENTITY,
    PLATFORM_HEADER,
    client_identity_headers,
    platform_header_value,
)
from agentchatme._runtime import PlatformFacts, platform_facts, platform_facts_of
from agentchatme.integration import account, probe

FACTS = PlatformFacts(os="darwin/25.6.0", arch="arm64", runtime="cpython/3.12.4")
INSTALL = "ins_AbCdEfGhIjKlMnOpQrSt_-"
FULL = AgentChatClientIdentity(
    name="hermes",
    version="0.9.0",
    host="hermes/0.9.2",
    install_id=INSTALL,
    install_method="pip",
)
API = "https://api.test"

_MEMBER = re.compile(r'([a-z*][a-z0-9_.*-]*)="((?:[\x20\x21\x23-\x5b\x5d-\x7e]|\\["\\])*)"')


def parse_string_dictionary(value: str) -> dict[str, str]:
    """An RFC 8941 dictionary whose every member is a string, parsed strictly."""
    out: dict[str, str] = {}
    rest = value
    while rest:
        match = _MEMBER.match(rest)
        if match is None:
            raise AssertionError(f"not a string dictionary: {value!r}")
        out[match.group(1)] = re.sub(r'\\(["\\])', r"\1", match.group(2))
        rest = rest[match.end() :]
        if not rest:
            break
        if not rest.startswith(", ") or rest == ", ":
            raise AssertionError(f"bad separator in {value!r}")
        rest = rest[2:]
    return out


def identity(**fields: Any) -> AgentChatClientIdentity:
    return AgentChatClientIdentity(name="python_sdk", **fields)


# ─────────────── the value ───────────────


def test_every_member_in_a_fixed_order_as_an_rfc_8941_dictionary() -> None:
    value = platform_header_value(FULL, FACTS)
    assert value == (
        f'host="hermes/0.9.2", os="darwin/25.6.0", arch="arm64", runtime="cpython/3.12.4", '
        f'install="{INSTALL}", method="pip"'
    )
    assert value is not None
    assert parse_string_dictionary(value) == {
        "host": "hermes/0.9.2",
        "os": "darwin/25.6.0",
        "arch": "arm64",
        "runtime": "cpython/3.12.4",
        "install": INSTALL,
        "method": "pip",
    }
    assert list(parse_string_dictionary(value)) == ["host", "os", "arch", "runtime", "install", "method"]


def test_the_default_identity_carries_only_the_sdk_facts() -> None:
    assert platform_header_value(DEFAULT_CLIENT_IDENTITY, FACTS) == (
        'os="darwin/25.6.0", arch="arm64", runtime="cpython/3.12.4"'
    )
    assert platform_header_value(None, FACTS) == platform_header_value(DEFAULT_CLIENT_IDENTITY, FACTS)


BAD_VALUES = [
    pytest.param("", id="empty"),
    pytest.param(".hermes/1", id="a leading dot"),
    pytest.param("-x", id="a leading dash"),
    pytest.param("/x", id="a leading slash"),
    pytest.param("hermes agent/1", id="a space"),
    pytest.param('hermes"/1', id="a double quote"),
    pytest.param("hermes\\1", id="a backslash"),
    pytest.param("a,b", id="a comma"),
    pytest.param("a=b", id="an equals sign"),
    pytest.param("a;b", id="a semicolon"),
    pytest.param("hërmes/1", id="a non-ASCII letter"),
    pytest.param("hermes/1\n", id="a trailing newline"),
    pytest.param("a" + "b" * 64, id="65 characters"),
]


@pytest.mark.parametrize("bad", BAD_VALUES)
def test_a_value_without_the_shape_is_left_out(bad: str) -> None:
    assert platform_header_value(identity(host=bad, install_method=bad), FACTS) == (
        'os="darwin/25.6.0", arch="arm64", runtime="cpython/3.12.4"'
    )
    assert platform_header_value(identity(), PlatformFacts(os=bad, arch=bad, runtime=bad)) is None


def test_a_value_of_exactly_64_characters_is_kept() -> None:
    host = "a" + "b" * 63
    assert platform_header_value(identity(host=host), PlatformFacts()) == f'host="{host}"'


def test_a_value_that_is_not_a_string_is_left_out() -> None:
    odd = AgentChatClientIdentity(name="python_sdk", host=42, install_id=None, install_method={})  # type: ignore[arg-type]
    assert platform_header_value(odd, PlatformFacts(os=1, arch=True)) is None  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "install_id",
    [
        pytest.param("inst_" + "a" * 22, id="another prefix"),
        pytest.param("ins_" + "a" * 21, id="21 characters after the prefix"),
        pytest.param("ins_" + "a" * 23, id="23 characters after the prefix"),
        pytest.param("ins_" + "a" * 21 + ".", id="a dot"),
        pytest.param("ins_" + "a" * 21 + "+", id="a plus"),
        pytest.param(INSTALL + "\n", id="a trailing newline"),
    ],
)
def test_an_install_id_without_its_shape_is_left_out(install_id: str) -> None:
    assert platform_header_value(identity(install_id=install_id), PlatformFacts()) is None


def test_every_member_at_its_longest_stays_under_512_bytes() -> None:
    longest = "a" + "b" * 63
    value = platform_header_value(
        identity(host=longest, install_id=INSTALL, install_method=longest),
        PlatformFacts(os=longest, arch=longest, runtime=longest),
    )
    assert value is not None
    assert len(parse_string_dictionary(value)) == 6
    assert len(value.encode("utf-8")) <= 512


def test_no_member_left_means_no_value_and_no_header(monkeypatch: pytest.MonkeyPatch) -> None:
    assert platform_header_value(identity(), PlatformFacts()) is None
    assert platform_header_value(identity(host="not valid"), PlatformFacts()) is None
    # A process the SDK knows nothing about: no header at all.
    monkeypatch.setattr("agentchatme._client_identity.platform_facts", PlatformFacts)
    assert PLATFORM_HEADER not in client_identity_headers(identity(version="1.0.0", host="not valid"))
    assert client_identity_headers(identity(host="hermes/0.9.2"))[PLATFORM_HEADER] == 'host="hermes/0.9.2"'


def test_client_identity_headers_carries_the_value() -> None:
    assert client_identity_headers(FULL) == {
        "X-AgentChat-Client": "hermes",
        "X-AgentChat-Client-Version": "0.9.0",
        PLATFORM_HEADER: platform_header_value(FULL),
    }


def test_old_positional_identities_still_construct() -> None:
    old = AgentChatClientIdentity("hermes", "0.9.0")
    assert (old.host, old.install_id, old.install_method) == (None, None, None)


# ─────────────── the facts ───────────────


def facts_of(**over: Any) -> PlatformFacts:
    args: dict[str, Any] = {
        "sys_platform": "darwin",
        "release": "25.6.0",
        "machine": "arm64",
        "implementation": "CPython",
        "implementation_version": "3.12.4",
    }
    args.update(over)
    return platform_facts_of(**args)


def test_facts_in_node_s_vocabulary() -> None:
    assert facts_of() == PlatformFacts(os="darwin/25.6.0", arch="arm64", runtime="cpython/3.12.4")
    assert facts_of(implementation="PyPy", implementation_version="7.3.17").runtime == "pypy/7.3.17"


@pytest.mark.parametrize(
    ("machine", "arch"),
    [
        ("x86_64", "x64"),
        ("AMD64", "x64"),
        ("aarch64", "arm64"),
        ("ARM64", "arm64"),
        ("arm64", "arm64"),
        ("i686", "ia32"),
        ("x86", "ia32"),
        ("armv7l", "arm"),
        ("armv6l", "arm"),
        ("ppc64le", "ppc64"),
        ("loongarch64", "loong64"),
        ("s390x", "s390x"),
        ("riscv64", "riscv64"),
    ],
)
def test_arch_is_named_as_node_names_it(machine: str, arch: str) -> None:
    assert facts_of(machine=machine).arch == arch


@pytest.mark.parametrize(
    ("sys_platform", "os_value"),
    [
        ("linux", "linux/25.6.0"),
        ("darwin", "darwin/25.6.0"),
        ("win32", "win32/25.6.0"),
        ("cygwin", "cygwin/25.6.0"),
        ("freebsd14", "freebsd/25.6.0"),
        ("sunos5", "sunos/25.6.0"),
    ],
)
def test_the_platform_loses_its_version_suffix(sys_platform: str, os_value: str) -> None:
    assert facts_of(sys_platform=sys_platform).os == os_value


def test_the_platform_alone_when_the_release_is_unknown_or_out_of_shape() -> None:
    assert facts_of(release=None).os == "darwin"
    assert facts_of(release="25.6.0 custom").os == "darwin"
    assert facts_of(release="").os == "darwin"
    assert facts_of(sys_platform=None, release="25.6.0").os is None
    assert facts_of(machine=None, implementation=None).arch is None


def test_the_windows_release_is_the_real_build_like_node_s(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setattr(
        sys,
        "getwindowsversion",
        lambda: SimpleNamespace(major=10, minor=0, build=22000, platform_version=(10, 0, 22631)),
        raising=False,
    )
    assert runtime_module._os_release() == "10.0.22631"


def test_a_release_that_cannot_be_read_is_left_out(monkeypatch: pytest.MonkeyPatch) -> None:
    def broken() -> str:
        raise OSError("no uname here")

    read: Callable[[Callable[[], object]], str | None] = runtime_module._read
    assert read(broken) is None
    assert read(lambda: "") is None


def test_this_process_facts_are_read_once() -> None:
    facts = platform_facts()
    assert platform_facts() is facts
    impl = platform.python_implementation().lower()
    assert facts.runtime is not None and facts.runtime.startswith(f"{impl}/")
    if impl == "cpython":
        assert facts.runtime == f"cpython/{platform.python_version()}"
    name = runtime_module._node_platform(sys.platform)
    assert facts.os is not None and facts.os.startswith(name)
    if sys.platform != "win32":
        assert facts.os == f"{name}/{os.uname().release}"
    assert facts.arch == runtime_module._node_arch(platform.machine())


# ─────────────── where the value goes ───────────────


class Recorder:
    def __init__(self, status: int = 200, body: Any = None) -> None:
        self.status = status
        self.body: Any = {} if body is None else body
        self.seen: list[httpx.Request] = []

    def _answer(self, request: httpx.Request) -> httpx.Response:
        self.seen.append(request)
        return httpx.Response(self.status, json=self.body)

    def client(self) -> httpx.Client:
        return httpx.Client(transport=httpx.MockTransport(self._answer))

    def async_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self._answer))

    def platform(self, index: int = 0) -> str | None:
        return self.seen[index].headers.get("x-agentchat-platform")

    def body_of(self, index: int = 0) -> Any:
        return json.loads(self.seen[index].content)


def test_every_client_request_carries_the_sdk_facts_by_default() -> None:
    srv = Recorder(body=[])
    with AgentChatClient(api_key="k", base_url=API, http_client=srv.client()) as client:
        client.list_conversations()
    assert srv.platform() == platform_header_value()
    sent = parse_string_dictionary(srv.platform() or "")
    assert sent["runtime"] == platform_facts().runtime


def test_the_integration_s_host_install_and_method_go_with_it() -> None:
    srv = Recorder(body=[])
    with AgentChatClient(api_key="k", base_url=API, http_client=srv.client(), client_identity=FULL) as client:
        client.list_conversations()
    assert parse_string_dictionary(srv.platform() or "") == {
        **parse_string_dictionary(platform_header_value() or ""),
        "host": "hermes/0.9.2",
        "install": INSTALL,
        "method": "pip",
    }
    assert srv.seen[0].headers.get("x-agentchat-client") == "hermes"


async def test_the_async_client_carries_it_too() -> None:
    srv = Recorder(body=[])
    async with AsyncAgentChatClient(api_key="k", base_url=API, http_client=srv.async_client(), client_identity=FULL) as client:
        await client.list_conversations()
    assert srv.platform() == platform_header_value(FULL)


def test_the_unauthenticated_calls_carry_it() -> None:
    srv = Recorder(body={"pending_id": "pnd_1", "message": "sent"})
    AgentChatClient.register(email="a@example.com", handle="alice", base_url=API, client_identity=FULL, http_client=srv.client())
    assert srv.platform() == platform_header_value(FULL)


def test_integration_account_calls_carry_it() -> None:
    srv = Recorder(body={"pending_id": "pnd_1", "message": "sent"})
    account.start("register", {"email": "a@example.com", "handle": "alice"}, client_identity=FULL, api_base=API, http_client=srv.client())
    assert srv.platform() == platform_header_value(FULL)


def test_the_hosted_tools_probe_carries_it() -> None:
    srv = Recorder(status=401)
    probe.probe_hosted_tools(api_base=API, api_key="k", client_identity=FULL, http_client=srv.client())
    assert srv.seen[0].url.path == "/mcp"
    assert srv.platform() == platform_header_value(FULL)


class _Socket:
    def __init__(self) -> None:
        self.sent: list[str] = []

    async def send(self, data: str) -> None:
        self.sent.append(data)

    async def close(self, code: int = 1000, reason: str = "") -> None:
        return None

    def __aiter__(self) -> _Socket:
        return self

    async def __anext__(self) -> str:
        raise StopAsyncIteration


async def _hello(client_identity: AgentChatClientIdentity | None) -> dict[str, Any]:
    sock = _Socket()

    async def connect(_url: str, **_kw: Any) -> _Socket:
        return sock

    rt = RealtimeClient(api_key="k", reconnect=False, websocket_connect=connect, client_identity=client_identity)
    try:
        await rt.connect()
    finally:
        await rt.disconnect()
    hello: dict[str, Any] = json.loads(sock.sent[0])
    return hello


async def test_the_socket_hello_carries_the_same_value_as_platform() -> None:
    hello = await _hello(FULL)
    assert hello["type"] == "hello"
    assert hello["client"] == "hermes"
    assert hello["platform"] == platform_header_value(FULL)
    assert parse_string_dictionary(hello["platform"])["host"] == "hermes/0.9.2"


async def test_the_socket_hello_has_no_platform_when_nothing_is_known(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("agentchatme._client_identity.platform_facts", PlatformFacts)
    hello = await _hello(None)
    assert "platform" not in hello
    assert hello["client"] == "python_sdk"


# ─────────────── B5: what an agent declares at sign-up ───────────────

DECLARED: dict[str, Any] = {"runtime": "hermes", "heard_from": "agent", "referred_by": "some-agent"}


def test_register_sends_the_declared_fields() -> None:
    srv = Recorder(body={"pending_id": "pnd_1", "message": "sent"})
    AgentChatClient.register(email="a@example.com", handle="alice", **DECLARED, base_url=API, http_client=srv.client())
    assert srv.body_of() == {"email": "a@example.com", "handle": "alice", **DECLARED}


def test_register_without_them_sends_the_body_as_before() -> None:
    srv = Recorder(body={"pending_id": "pnd_1", "message": "sent"})
    AgentChatClient.register(email="a@example.com", handle="alice", display_name="Alice", base_url=API, http_client=srv.client())
    assert srv.body_of() == {"email": "a@example.com", "handle": "alice", "display_name": "Alice"}


def test_a_value_the_sdk_does_not_know_is_sent_as_given() -> None:
    srv = Recorder(body={"pending_id": "pnd_1", "message": "sent"})
    AgentChatClient.register(
        email="a@example.com",
        handle="alice",
        heard_from="a friend",  # type: ignore[arg-type]
        base_url=API,
        http_client=srv.client(),
    )
    assert srv.body_of() == {"email": "a@example.com", "handle": "alice", "heard_from": "a friend"}


async def test_async_register_sends_the_declared_fields() -> None:
    srv = Recorder(body={"pending_id": "pnd_1", "message": "sent"})
    await AsyncAgentChatClient.register(
        email="a@example.com", handle="alice", **DECLARED, base_url=API, http_client=srv.async_client()
    )
    assert srv.body_of() == {"email": "a@example.com", "handle": "alice", **DECLARED}


def test_account_start_sends_them_with_register_and_recover_sends_none() -> None:
    reg = Recorder(body={"pending_id": "pnd_1", "message": "sent"})
    account.start(
        "register",
        {"email": "a@example.com", "handle": "alice", **DECLARED},  # type: ignore[typeddict-item]
        client_identity=FULL,
        api_base=API,
        http_client=reg.client(),
    )
    assert reg.body_of() == {"email": "a@example.com", "handle": "alice", **DECLARED}

    rec = Recorder(body={"pending_id": "pnd_2", "message": "sent"})
    account.start(
        "recover",
        {"email": "a@example.com", "handle": "alice", **DECLARED},  # type: ignore[typeddict-item]
        client_identity=FULL,
        api_base=API,
        http_client=rec.client(),
    )
    assert rec.body_of() == {"email": "a@example.com", "handle": "alice"}


async def test_account_start_async_sends_them() -> None:
    srv = Recorder(body={"pending_id": "pnd_1", "message": "sent"})
    await account.start_async(
        "register",
        {"email": "a@example.com", "handle": "alice", **DECLARED},  # type: ignore[typeddict-item]
        client_identity=FULL,
        api_base=API,
        http_client=srv.async_client(),
    )
    assert srv.body_of() == {"email": "a@example.com", "handle": "alice", **DECLARED}


def test_account_run_sends_them_with_the_register_call() -> None:
    srv = Recorder(body={"pending_id": "pnd_1", "message": "sent"})
    result = account.run(
        kind="register",
        input={"email": "a@example.com", "handle": "alice", **DECLARED},
        client_identity=FULL,
        api_base=API,
        http_client=srv.client(),
        show=lambda _message: None,
        ask=lambda _question: account.AccountAnswer("cancel"),
    )
    assert result.outcome == "cancelled"
    assert srv.seen[0].url.path == "/v1/register"
    assert srv.body_of() == {"email": "a@example.com", "handle": "alice", **DECLARED}


async def test_account_run_async_sends_them_with_the_register_call() -> None:
    srv = Recorder(body={"pending_id": "pnd_1", "message": "sent"})
    result = await account.run_async(
        kind="register",
        input={"email": "a@example.com", "handle": "alice", **DECLARED},
        client_identity=FULL,
        api_base=API,
        http_client=srv.async_client(),
        show=lambda _message: None,
        ask=lambda _question: account.AccountAnswer("cancel"),
    )
    assert result.outcome == "cancelled"
    assert srv.body_of() == {"email": "a@example.com", "handle": "alice", **DECLARED}
