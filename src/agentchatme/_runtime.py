"""Runtime detection + default ``User-Agent`` builder.

Mirrors the TypeScript SDK's ``defaultUserAgent`` format so server-side
log analyzers can parse SDK traffic uniformly regardless of language.

Format: ``agentchatme-py/<sdk-version> <runtime>/<runtime-version>``

Also the platform facts of ``X-AgentChat-Platform`` (:func:`platform_facts`),
in the TypeScript SDK's vocabulary so both SDKs send the same values.
"""

from __future__ import annotations

import functools
import os
import platform
import re
import sys
from dataclasses import dataclass
from typing import Callable

from ._version import VERSION


def detect_runtime() -> str:
    """Return a ``<runtime>/<version>`` token identifying the Python runtime.

    Distinguishes CPython, PyPy, and other implementations. Used as the
    second segment of the default ``User-Agent`` header.
    """
    impl = platform.python_implementation().lower()  # cpython, pypy, etc.
    version = platform.python_version()  # e.g. "3.12.1"
    return f"{impl}/{version}"


def default_user_agent() -> str:
    """Default ``User-Agent`` emitted on every HTTP request.

    Format is deliberately close to Stripe / Twilio / OpenAI conventions
    so existing log pipelines parse it without adjustment.
    """
    return f"agentchatme-py/{VERSION} {detect_runtime()}"


def platform_tag() -> str:
    """Coarse OS tag — useful for support tickets. Not part of the UA header."""
    return f"{platform.system().lower()}-{platform.machine().lower()}-py{sys.version_info.major}.{sys.version_info.minor}"


# ─────────────── X-AgentChat-Platform facts ───────────────

PLATFORM_VALUE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+/-]{0,63}")
"""The shape every ``X-AgentChat-Platform`` value but ``install`` must have
(matched whole, with :func:`re.fullmatch`: ``$`` would let a trailing
newline through)."""


def is_platform_value(value: object) -> bool:
    return isinstance(value, str) and PLATFORM_VALUE.fullmatch(value) is not None


@dataclass(frozen=True)
class PlatformFacts:
    """What the SDK knows about the process it runs in: the members of
    ``X-AgentChat-Platform`` it computes itself. Nothing that names a
    machine or a person."""

    os: str | None = None
    """``<platform>/<release>`` (``darwin/25.6.0``), or the platform alone
    when the release is not known."""
    arch: str | None = None
    """The CPU architecture, as Node names it (``arm64``, ``x64``, ...)."""
    runtime: str | None = None
    """The implementation and its own version (``cpython/3.12.4``,
    ``pypy/7.3.17``)."""


# platform.machine() → Node's process.arch, so both SDKs send one vocabulary.
_NODE_ARCH = {
    "x86_64": "x64",
    "amd64": "x64",
    "x64": "x64",
    "aarch64": "arm64",
    "arm64": "arm64",
    "i386": "ia32",
    "i486": "ia32",
    "i586": "ia32",
    "i686": "ia32",
    "x86": "ia32",
    "ppc64le": "ppc64",
    "ppc64": "ppc64",
    "loongarch64": "loong64",
}


def _node_arch(machine: str) -> str:
    name = machine.strip().lower()
    if name in _NODE_ARCH:
        return _NODE_ARCH[name]
    # armv6l, armv7l, armv8l (32-bit ARM): Node's `arm`.
    return "arm" if name.startswith("arm") else name


def _node_platform(sys_platform: str) -> str:
    return sys_platform if sys_platform == "win32" else re.sub(r"\d+$", "", sys_platform)


def platform_facts_of(
    *,
    sys_platform: str | None,
    release: str | None,
    machine: str | None,
    implementation: str | None,
    implementation_version: str | None,
) -> PlatformFacts:
    """The platform facts from what the interpreter reports, in Node's names:
    ``sys.platform`` without a version suffix (``freebsd14`` → ``freebsd``,
    ``sunos5`` → ``sunos``; ``win32`` is a name, not a version, and stays),
    ``platform.machine()`` as ``process.arch``. A release that does not have
    the value shape leaves the platform alone."""
    name = _node_platform(sys_platform) if sys_platform else ""
    with_release = f"{name}/{release}" if name and release else None
    return PlatformFacts(
        os=with_release if is_platform_value(with_release) else (name or None),
        arch=_node_arch(machine) if machine else None,
        runtime=(
            f"{implementation.lower()}/{implementation_version}"
            if implementation and implementation_version
            else None
        ),
    )


def _os_release() -> str:
    """The kernel release, as Node's ``os.release()`` gives it: the uname
    release on POSIX, ``<major>.<minor>.<build>`` of the real OS on Windows
    (``platform.release()`` would say only ``10`` or ``11``)."""
    if sys.platform == "win32":
        major, minor, build = sys.getwindowsversion().platform_version
        return f"{major}.{minor}.{build}"
    return os.uname().release


def _implementation_version() -> str:
    """The implementation's own version (PyPy's, not the language's), as
    CPython spells a version: ``3.13.1``, ``3.14.0rc1``."""
    v = sys.implementation.version
    text = f"{v.major}.{v.minor}.{v.micro}"
    suffix = {"alpha": "a", "beta": "b", "candidate": "rc"}.get(v.releaselevel)
    return f"{text}{suffix}{v.serial}" if suffix else text


def _read(source: Callable[[], object]) -> str | None:
    try:
        value = source()
    except Exception:
        return None
    return value if isinstance(value, str) and value else None


@functools.lru_cache(maxsize=1)
def platform_facts() -> PlatformFacts:
    """This process's platform facts: read once, then kept (they cannot
    change while it runs)."""
    return platform_facts_of(
        sys_platform=sys.platform,
        release=_read(_os_release),
        machine=_read(platform.machine),
        implementation=_read(platform.python_implementation),
        implementation_version=_read(_implementation_version),
    )
