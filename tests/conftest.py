"""Shared pytest fixtures and the network guard.

The network guard: no test may reach a network beyond this machine. Every
socket connect and every name lookup for a non-loopback destination is
refused, and the run fails for it, even when the code under test swallowed
the error. Loopback stays open for local fake servers. Only the live smoke
tests (``@pytest.mark.live``, skipped unless ``AGENTCHAT_LIVE_API_KEY`` is
set) are exempt, for their whole run including the fixtures set up for them.

The guard is on from the start of the session to its end: while test
modules are collected (a module that connects at import), while session-
and module-scoped fixtures run, and during every test. It watches three
doors:

- ``socket.socket.connect`` and ``connect_ex``: every blocking connect, and
  every asyncio connect on a selector loop (Linux, macOS, and Windows'
  selector loop).
- ``socket.getaddrinfo``: every name lookup, blocking or through asyncio.
- ``asyncio``'s ``BaseProactorEventLoop.sock_connect``: on Windows' default
  proactor loop, asyncio connects through ``ConnectEx`` and never calls
  ``socket.connect``, and a literal IP address skips ``getaddrinfo`` too.
  Without this door, ``asyncio.open_connection("203.0.113.7", 443)`` would
  go out on Windows.

A refused attempt while a test runs (its setup, call or teardown, including
a wider-scoped fixture set up for it) fails that test. An attempt outside
any test (collection, or a fixture's final teardown after the last test
that used it) fails the session: the exit status becomes "tests failed" and
the summary names each attempt.

Not covered: subprocesses, UDP ``sendto`` without a connect, and code that
calls the C-level ``_socket`` module directly. The SDK does none of these.
"""

from __future__ import annotations

import asyncio.proactor_events
import ipaddress
import socket
from collections.abc import Generator, Iterator
from typing import Any

import pytest

_LOOPBACK_NAMES = frozenset({"localhost", "localhost.localdomain", "ip6-localhost", ""})


class NetworkAccessBlocked(OSError):
    """Raised by the network guard for a non-loopback connect or lookup."""


def _is_loopback_host(host: Any) -> bool:
    if host is None:
        return True  # getaddrinfo(None, port): the local host
    if isinstance(host, bytes):
        host = host.decode("ascii", "replace")
    if not isinstance(host, str):
        return False
    name = host.strip().strip("[]").lower()
    if name in _LOOPBACK_NAMES:
        return True
    name = name.split("%", 1)[0]  # an IPv6 zone id
    try:
        return ipaddress.ip_address(name).is_loopback
    except ValueError:
        return False


def _is_loopback_address(address: Any) -> bool:
    if isinstance(address, (str, bytes)):
        return True  # AF_UNIX: a local path
    if isinstance(address, tuple) and address:
        return _is_loopback_host(address[0])
    return False


class _NetworkGuard:
    """Session-wide state: where the run is, and what it tried."""

    def __init__(self) -> None:
        self._installed: list[tuple[Any, str, Any]] = []
        self.reset()

    def reset(self) -> None:
        self.where = "collection, before the first test"
        self.exempt = False
        self.in_test = False
        self.test_attempts: list[str] = []  # while the current test runs
        self.session_attempts: list[str] = []  # outside any test: fail the session

    def refuse(self, what: str) -> NetworkAccessBlocked:
        if self.in_test:
            self.test_attempts.append(what)
        else:
            self.session_attempts.append(f"{what} ({self.where})")
        return NetworkAccessBlocked(f"network access blocked in tests: {what}")

    def install(self) -> None:
        self.reset()  # a fresh session starts clean
        real_connect = socket.socket.connect
        real_connect_ex = socket.socket.connect_ex
        real_getaddrinfo = socket.getaddrinfo
        proactor = asyncio.proactor_events.BaseProactorEventLoop
        real_sock_connect = proactor.sock_connect

        def connect(sock: socket.socket, address: Any) -> None:
            if not self.exempt and not _is_loopback_address(address):
                raise self.refuse(f"connect {address!r}")
            real_connect(sock, address)

        def connect_ex(sock: socket.socket, address: Any) -> int:
            if not self.exempt and not _is_loopback_address(address):
                raise self.refuse(f"connect_ex {address!r}")
            return real_connect_ex(sock, address)

        def getaddrinfo(host: Any, *args: Any, **kwargs: Any) -> Any:
            if not self.exempt and not _is_loopback_host(host):
                raise self.refuse(f"getaddrinfo {host!r}")
            return real_getaddrinfo(host, *args, **kwargs)

        async def sock_connect(loop: Any, sock: socket.socket, address: Any) -> None:
            if not self.exempt and not _is_loopback_address(address):
                raise self.refuse(f"proactor sock_connect {address!r}")
            return await real_sock_connect(loop, sock, address)

        for target, name, wrapper in (
            (socket.socket, "connect", connect),
            (socket.socket, "connect_ex", connect_ex),
            (socket, "getaddrinfo", getaddrinfo),
            (proactor, "sock_connect", sock_connect),
        ):
            self._installed.append((target, name, getattr(target, name)))
            setattr(target, name, wrapper)

    def uninstall(self) -> None:
        while self._installed:
            target, name, original = self._installed.pop()
            setattr(target, name, original)

    def take_test_attempts(self) -> list[str]:
        attempts, self.test_attempts = self.test_attempts, []
        return attempts


_GUARD = _NetworkGuard()


def pytest_configure(config: pytest.Config) -> None:
    _GUARD.install()


def pytest_unconfigure(config: pytest.Config) -> None:
    _GUARD.uninstall()


@pytest.hookimpl(wrapper=True)
def pytest_runtest_protocol(
    item: pytest.Item, nextitem: pytest.Item | None
) -> Generator[None, object, object]:
    # Around the whole protocol, so the fixtures set up for this test count
    # as the test (and a live test's fixtures are exempt with it).
    _GUARD.where = f"test {item.nodeid}"
    _GUARD.in_test = True
    _GUARD.exempt = item.get_closest_marker("live") is not None
    try:
        return (yield)
    finally:
        # What the test's own check (the fixture below) did not see: a
        # wider-scoped fixture's teardown after it, or a setup that failed
        # before the check was set up.
        _GUARD.session_attempts.extend(
            f"{what} (test {item.nodeid}, outside its own check)"
            for what in _GUARD.take_test_attempts()
        )
        _GUARD.where = f"between tests, after {item.nodeid}"
        _GUARD.in_test = False
        _GUARD.exempt = False


@pytest.fixture(autouse=True)
def _network_guard() -> Iterator[None]:
    yield
    attempts = _GUARD.take_test_attempts()
    if attempts:
        pytest.fail(
            "the test tried to reach the network: " + "; ".join(attempts[:5]),
            pytrace=False,
        )


def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    if _GUARD.session_attempts and session.exitstatus in (
        pytest.ExitCode.OK,
        pytest.ExitCode.NO_TESTS_COLLECTED,
    ):
        session.exitstatus = pytest.ExitCode.TESTS_FAILED


def pytest_terminal_summary(terminalreporter: Any, exitstatus: int, config: pytest.Config) -> None:
    if not _GUARD.session_attempts:
        return
    terminalreporter.write_sep("=", "network guard: the run tried to reach the network", red=True)
    for what in _GUARD.session_attempts:
        terminalreporter.write_line(what)


@pytest.fixture(autouse=True)
def _quiet_httpx_caplog(caplog: pytest.LogCaptureFixture) -> None:
    # Silence httpx/httpcore DEBUG spam in test output without affecting
    # the ability for individual tests to opt in to caplog assertions.
    import logging

    for name in ("httpx", "httpcore", "websockets"):
        logging.getLogger(name).setLevel(logging.WARNING)
