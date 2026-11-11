"""The test network guard (``conftest.py``) itself: a non-loopback connect or
lookup fails the test that tried, even when the code swallowed the error; one
outside any test (collection, a session fixture) fails the session; asyncio's
proactor connect (Windows) is watched too; loopback stays open."""

from __future__ import annotations

import socket
import sys
from pathlib import Path

import pytest

pytest_plugins = ["pytester"]

_CONFTEST = (Path(__file__).resolve().parent / "conftest.py").read_text(encoding="utf-8")


_INI = "[pytest]\nasyncio_default_fixture_loop_scope = function\nmarkers =\n    live: live smoke\n"


def test_a_swallowed_non_loopback_connect_still_fails_the_test(pytester: pytest.Pytester) -> None:
    pytester.makeconftest(_CONFTEST)
    pytester.makeini(_INI)
    pytester.makepyfile(
        """
        import socket

        def test_swallows_a_connect():
            try:
                socket.create_connection(("203.0.113.7", 443), timeout=1)
            except OSError:
                pass

        def test_swallows_a_lookup():
            try:
                socket.getaddrinfo("api.example.com", 443)
            except OSError:
                pass

        def test_loopback_is_open():
            server = socket.socket()
            server.bind(("127.0.0.1", 0))
            server.listen(1)
            client = socket.create_connection(server.getsockname(), timeout=1)
            client.close()
            server.close()
        """
    )
    before = (socket.socket.connect, socket.getaddrinfo)
    result = pytester.runpytest_inprocess("-p", "no:cacheprovider")
    result.assert_outcomes(passed=3, errors=2)
    result.stdout.fnmatch_lines(["*the test tried to reach the network*"])
    # The inner session put back what it found: this session's own guard.
    assert (socket.socket.connect, socket.getaddrinfo) == before


def test_the_proactor_connect_is_watched_on_every_platform(pytester: pytest.Pytester) -> None:
    # asyncio on Windows' proactor loop connects through ConnectEx, never
    # socket.connect, and a literal IP skips getaddrinfo. The guard wraps the
    # loop's sock_connect and refuses before touching the loop, so the first
    # step of the unbound coroutine proves the door is watched on every
    # platform. No event loop runs: this inner session shares the process,
    # and asyncio.run would leave the main thread without a current loop.
    pytester.makeconftest(_CONFTEST)
    pytester.makeini(_INI)
    pytester.makepyfile(
        """
        import socket
        from asyncio.proactor_events import BaseProactorEventLoop

        def test_swallows_a_proactor_connect():
            sock = socket.socket()
            step = BaseProactorEventLoop.sock_connect(None, sock, ("203.0.113.7", 443))
            try:
                step.send(None)
            except OSError:
                pass
            finally:
                step.close()
                sock.close()
        """
    )
    result = pytester.runpytest_inprocess("-p", "no:cacheprovider")
    result.assert_outcomes(passed=1, errors=1)
    result.stdout.fnmatch_lines(["*proactor sock_connect ('203.0.113.7', 443)*"])


@pytest.mark.skipif(sys.platform != "win32", reason="the proactor loop is Windows' default loop")
def test_a_literal_ip_asyncio_connect_on_the_windows_default_loop_fails_the_test(
    pytester: pytest.Pytester,
) -> None:
    pytester.makeconftest(_CONFTEST)
    pytester.makeini(_INI)
    pytester.makepyfile(
        """
        import asyncio

        def test_swallows_an_open_connection():
            async def go():
                assert type(asyncio.get_running_loop()).__name__ == "ProactorEventLoop"
                try:
                    await asyncio.wait_for(asyncio.open_connection("203.0.113.7", 443), 5)
                except (OSError, asyncio.TimeoutError):
                    pass
            asyncio.run(go())

        def test_loopback_is_open_through_the_proactor():
            async def go():
                server = await asyncio.start_server(lambda r, w: w.close(), "127.0.0.1", 0)
                port = server.sockets[0].getsockname()[1]
                _, writer = await asyncio.open_connection("127.0.0.1", port)
                writer.close()
                server.close()
                await server.wait_closed()
            asyncio.run(go())
        """
    )
    # In a subprocess: asyncio.run would leave this process's main thread
    # without a current event loop.
    result = pytester.runpytest_subprocess("-p", "no:cacheprovider")
    result.assert_outcomes(passed=2, errors=1)
    result.stdout.fnmatch_lines(["*proactor sock_connect*203.0.113.7*"])


def test_a_swallowed_lookup_at_collection_time_fails_the_session(pytester: pytest.Pytester) -> None:
    pytester.makeconftest(_CONFTEST)
    pytester.makeini(_INI)
    pytester.makepyfile(
        """
        import socket

        try:  # at import, while the module is collected
            socket.getaddrinfo("api.example.com", 443)
        except OSError:
            pass

        def test_nothing_wrong_here():
            pass
        """
    )
    result = pytester.runpytest_inprocess("-p", "no:cacheprovider")
    result.assert_outcomes(passed=1)
    assert result.ret == pytest.ExitCode.TESTS_FAILED
    result.stdout.fnmatch_lines(
        [
            "*network guard: the run tried to reach the network*",
            "getaddrinfo 'api.example.com' (collection, before the first test)",
        ]
    )


def test_a_session_fixture_is_watched_in_setup_and_in_its_final_teardown(
    pytester: pytest.Pytester,
) -> None:
    pytester.makeconftest(
        _CONFTEST
        + """

@pytest.fixture(scope="session")
def remote():
    try:
        socket.create_connection(("203.0.113.7", 443), timeout=1)
    except OSError:
        pass
    yield
    try:
        socket.getaddrinfo("teardown.example.com", 443)
    except OSError:
        pass
"""
    )
    pytester.makeini(_INI)
    pytester.makepyfile(
        """
        def test_uses_the_session_fixture(remote):
            pass
        """
    )
    result = pytester.runpytest_inprocess("-p", "no:cacheprovider")
    # The setup ran for this test: the test fails for it. The final teardown
    # ran after the test's own check: the session fails for it.
    result.assert_outcomes(passed=1, errors=1)
    assert result.ret == pytest.ExitCode.TESTS_FAILED
    result.stdout.fnmatch_lines(
        [
            "*the test tried to reach the network: getaddrinfo '203.0.113.7'*",
            "*network guard: the run tried to reach the network*",
            "getaddrinfo 'teardown.example.com' (test *test_uses_the_session_fixture, outside its own check)",
        ]
    )


def test_live_tests_are_exempt(pytester: pytest.Pytester) -> None:
    # In a subprocess, so this suite's own guard is not in the way. Nothing
    # touches the network: a numeric-host lookup (AI_NUMERICHOST) is answered
    # on this machine, yet the guard refuses it outside a live test.
    pytester.makeconftest(
        _CONFTEST
        + """

@pytest.fixture(scope="module")
def looked_up():
    return socket.getaddrinfo("203.0.113.7", 443, flags=socket.AI_NUMERICHOST)
"""
    )
    pytester.makeini(_INI)
    pytester.makepyfile(
        """
        import socket
        import pytest

        @pytest.mark.live
        def test_live(looked_up):
            assert looked_up  # the fixture set up for a live test is exempt with it
            assert socket.getaddrinfo("203.0.113.7", 443, flags=socket.AI_NUMERICHOST)

        def test_not_live():
            with pytest.raises(OSError, match="network access blocked"):
                socket.getaddrinfo("203.0.113.7", 443, flags=socket.AI_NUMERICHOST)
        """
    )
    result = pytester.runpytest_subprocess("-p", "no:cacheprovider")
    result.assert_outcomes(passed=2, errors=1)
    assert result.ret == pytest.ExitCode.TESTS_FAILED
