"""Version-consistency gate.

``pyproject.toml`` is the authoritative version for the build backend
(hatchling reads the static ``[project] version``); ``_version.py`` only
feeds the default ``User-Agent`` header. They must never drift — this is
the assertion ``_version.py``'s docstring promises exists.
"""

from __future__ import annotations

import re
from pathlib import Path

from agentchatme import VERSION


def test_version_matches_pyproject() -> None:
    # Regex instead of a TOML parser: tomllib is 3.11+ and the suite runs
    # on 3.9; the line is fully controlled by this repo.
    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    match = re.search(r'^version = "(?P<v>[^"]+)"$', pyproject.read_text(), re.MULTILINE)
    assert match is not None, "no static [project] version found in pyproject.toml"
    assert match.group("v") == VERSION
