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
import sysconfig
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
    # `type(...) is str`, not isinstance: a str subclass could format itself
    # as other text than the one checked here.
    return type(value) is str and PLATFORM_VALUE.fullmatch(value) is not None


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


# Machine names → Node's process.arch, so both SDKs send one vocabulary. A
# closed list: a name it does not know is left out, never passed through
# (on AIX the machine name is the machine's id).
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
    "arm": "arm",
    "armv5tel": "arm",
    "armv6l": "arm",
    "armv7l": "arm",
    "armv8l": "arm",
    "ppc64le": "ppc64",
    "ppc64": "ppc64",
    "powerpc64le": "ppc64",
    "powerpc64": "ppc64",
    "ppc": "ppc",
    "powerpc": "ppc",
    "s390x": "s390x",
    "s390": "s390",
    "riscv64": "riscv64",
    "loongarch64": "loong64",
    "mips": "mips",
}


def _node_arch(machine: str) -> str | None:
    return _NODE_ARCH.get(machine.strip().lower())


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
    the machine name as ``process.arch`` (an unknown one is left out). A
    release that does not have the value shape leaves the platform alone."""
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
    release on POSIX; on Windows ``<major>.<minor>.<build>`` of the running
    OS from ``RtlGetVersion``, the call Node makes. (``platform.release()``
    says only ``10`` or ``11``; ``platform_version`` is kernel32.dll's file
    version, which stays at the base build across feature updates; and
    ``platform.version()`` starts a ``ver`` shell or a WMI query.)"""
    if sys.platform == "win32":
        import ctypes

        class _OsVersionInfo(ctypes.Structure):
            _fields_ = (  # OSVERSIONINFOW
                ("size", ctypes.c_ulong),
                ("major", ctypes.c_ulong),
                ("minor", ctypes.c_ulong),
                ("build", ctypes.c_ulong),
                ("platform_id", ctypes.c_ulong),
                ("csd", ctypes.c_wchar * 128),
            )

        info = _OsVersionInfo()
        info.size = ctypes.sizeof(info)
        if ctypes.WinDLL("ntdll").RtlGetVersion(ctypes.byref(info)) != 0:
            raise OSError("RtlGetVersion failed")
        return f"{info.major}.{info.minor}.{info.build}"
    return os.uname().release


def _machine() -> str:
    """The machine name, cheaply: ``os.uname()`` on POSIX; on Windows the
    interpreter's own build (``platform.machine()`` would start a ``ver``
    shell or a WMI query there)."""
    if sys.platform == "win32":
        return {"win-amd64": "amd64", "win-arm64": "arm64", "win-arm32": "arm", "win32": "x86"}.get(
            sysconfig.get_platform(), ""
        )
    return os.uname().machine


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
    change while it runs). The implementation is ``sys.implementation``'s
    name and version (``cpython``, ``pypy``, ``graalpy``)."""
    return platform_facts_of(
        sys_platform=sys.platform,
        release=_read(_os_release),
        machine=_read(_machine),
        implementation=_read(lambda: sys.implementation.name),
        implementation_version=_read(_implementation_version),
    )
