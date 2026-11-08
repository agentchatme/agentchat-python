"""The identity block: the served identity line, fenced, in the file the
runtime loads on every turn (``SOUL.md``, ``AGENTS.md``, ``CLAUDE.md``, …).
Pure string functions — which file, and when to write it, is the
integration's business (a shared function that picked the file once stamped
one agent's handle into another agent's file).

The words are the server's (``texts.identity_line``). This module adds the
fences and one handle line, so a block is recognised whatever the served
wording becomes::

    <!-- agentchat:start -->
    <!-- agentchat:handle relay-bot -->
    I am @relay-bot on AgentChat …
    <!-- agentchat:end -->

Blocks written before the handle line are recognised by the served
template's words and by the two wordings earlier releases wrote, and the
legacy ``agentchat-skill`` markers are still found and replaced
(conformance ``anchor.cases.json``).

Where the copies this replaces differed (Hermes ``soul_anchor.py``, OpenClaw
``binding/agents-anchor.ts``, agent-core ``anchor/block.ts``), the stricter
behaviour is kept: markers count only as whole lines (a marker quoted in
prose is not a fence); every duplicate and legacy block is removed while the
block keeps its place in the file; the handle line and the template-based
recognition (Hermes); a wrong handle is read back rather than ignored. A
start marker with no end before the next start is ignored instead of
fencing everything up to a later block.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

START_MARKER = "<!-- agentchat:start -->"
END_MARKER = "<!-- agentchat:end -->"
LEGACY_START_MARKER = "<!-- agentchat-skill:start -->"
LEGACY_END_MARKER = "<!-- agentchat-skill:end -->"

_HANDLE_PLACEHOLDER = "<handle>"
_HANDLE_LINE_RE = re.compile(r"^<!-- agentchat:handle @?([A-Za-z0-9][A-Za-z0-9_-]*) -->$")
_HANDLE_CAPTURE = r"@?([A-Za-z0-9][A-Za-z0-9_-]*)"
# Wordings earlier releases wrote without a handle line.
_OLD_WORDINGS = (
    re.compile(rf"I am {_HANDLE_CAPTURE} on AgentChat"),
    re.compile(rf"You are \*\*{_HANDLE_CAPTURE}\*\* on AgentChat"),
)


def normalize_handle(handle: str) -> str:
    """Handles are canonical lowercase on the wire; a leading ``@`` is
    display only."""
    trimmed = handle.strip()
    if trimmed.startswith("@"):
        trimmed = trimmed[1:]
    return trimmed.strip().lower()


def handle_line(handle: str) -> str:
    return f"<!-- agentchat:handle {normalize_handle(handle)} -->"


def _texts_field(texts: Any, key: str) -> str:
    value = texts.get(key) if isinstance(texts, Mapping) else getattr(texts, key, None)
    return value if isinstance(value, str) else ""


def render(texts: Any, handle: str) -> str:
    """The block for ``handle``: the fences, the handle line and the served
    line.

    Uses ``texts.identity_line`` when it names ``@handle``, else fills
    ``texts.identity_line_template``. ``texts`` is the typed
    :class:`~agentchatme.types.Texts` or the served JSON. Raises
    :class:`ValueError` when the handle is empty or no served text names it —
    a silently wrong identity is worse than a loud failure: the agent would
    hand peers an address that reaches someone else.
    """
    bare = normalize_handle(handle)
    if not bare:
        raise ValueError("anchor.render: the handle is empty")
    line = _texts_field(texts, "identity_line").strip()
    if not _names_handle(line, bare):
        template = _texts_field(texts, "identity_line_template")
        line = template.replace(_HANDLE_PLACEHOLDER, bare).strip() if _HANDLE_PLACEHOLDER in template else ""
    if not _names_handle(line, bare):
        raise ValueError(f"anchor.render: no served identity text names @{bare}")
    return "\n".join([START_MARKER, handle_line(bare), line, END_MARKER])


@dataclass(frozen=True)
class FoundBlock:
    start: int
    """Offset of the start marker's line."""
    end: int
    """Offset just past the end marker (before its line break)."""
    handle: str | None
    """The handle the block claims, lower-cased; ``None`` for a block in a
    shape not recognised (hand-edited)."""
    marked: bool
    """The block carries a handle line."""
    legacy: bool
    """The block uses the legacy ``agentchat-skill`` markers."""
    body: str
    """Between the markers, without the handle line; CRLF read as LF; trimmed."""


def find(text: str, *, template: str | None = None) -> FoundBlock | None:
    """The first current block, else the first legacy block, else ``None``.

    ``template`` is the served ``identity_line_template``: it recognises an
    unmarked block by its words.
    """
    spans = _blocks_of(text)
    span = next((s for s in spans if not s.legacy), spans[0] if spans else None)
    if span is None:
        return None
    inner = text[span.inner_start : span.inner_end].replace("\r\n", "\n")
    handle: str | None = None
    marked = False
    kept: list[str] = []
    for line in inner.split("\n"):
        match = _HANDLE_LINE_RE.match(line.strip()) if not marked else None
        if match:
            marked = True
            handle = match.group(1).lower()
            continue
        kept.append(line)
    body = "\n".join(kept).strip()
    if handle is None:
        handle = _handle_from_wording(body, template)
    return FoundBlock(
        start=span.start, end=span.end, handle=handle, marked=marked, legacy=span.legacy, body=body
    )


def replace(text: str, block: str) -> str:
    """``text`` with ``block`` in place of the first block (current or
    legacy) and every other block removed; appended after one blank line
    when there is none. Idempotent."""
    spans = _blocks_of(text)
    if not spans:
        trimmed = text.rstrip("\n")
        return f"{block}\n" if not trimmed else f"{trimmed}\n\n{block}\n"
    return _rebuild(text, spans, block)


def remove(text: str) -> str:
    """``text`` without any block (current or legacy); unchanged when there
    is none."""
    spans = _blocks_of(text)
    if not spans:
        return text
    return _rebuild(text, spans, None)


# ─── internals ──────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class _Span:
    start: int
    end: int
    inner_start: int
    inner_end: int
    legacy: bool


@dataclass(frozen=True)
class _Line:
    start: int
    end: int
    """Offset of the line break (or the text end)."""
    next: int
    trimmed: str


def _lines_of(text: str) -> list[_Line]:
    lines: list[_Line] = []
    at = 0
    while at <= len(text):
        nl = text.find("\n", at)
        end = len(text) if nl == -1 else nl
        lines.append(
            _Line(
                start=at,
                end=end,
                next=len(text) + 1 if nl == -1 else nl + 1,
                trimmed=text[at:end].strip(),
            )
        )
        if nl == -1:
            break
        at = nl + 1
    return lines


def _blocks_of(text: str) -> list[_Span]:
    """Every complete block, in order. One open start at a time: a second
    start drops the first."""
    spans: list[_Span] = []
    open_line: _Line | None = None
    open_legacy = False
    for line in _lines_of(text):
        if line.trimmed in (START_MARKER, LEGACY_START_MARKER):
            open_line = line
            open_legacy = line.trimmed == LEGACY_START_MARKER
            continue
        if open_line is not None and line.trimmed == (
            LEGACY_END_MARKER if open_legacy else END_MARKER
        ):
            spans.append(
                _Span(
                    start=open_line.start,
                    end=line.end,
                    inner_start=min(open_line.next, line.start),
                    inner_end=line.start,
                    legacy=open_legacy,
                )
            )
            open_line = None
    return spans


def _rebuild(text: str, spans: list[_Span], replacement: str | None) -> str:
    first, last = spans[0], spans[-1]
    parts: list[str] = [text[: first.start].rstrip("\n")]
    if replacement is not None:
        parts.append(replacement)
    for previous, current in zip(spans, spans[1:]):
        parts.append(text[previous.end : current.start].lstrip("\n").rstrip("\n"))
    parts.append(text[last.end :].lstrip("\n"))
    kept = [part for part in parts if part]
    if not kept:
        return ""
    out = "\n\n".join(kept)
    return out if out.endswith("\n") else f"{out}\n"


def _names_handle(line: str, bare: str) -> bool:
    """``@bare`` as a whole handle (not the prefix of a longer one)."""
    if not line:
        return False
    return re.search(rf"@{re.escape(bare)}(?![A-Za-z0-9_-])", line) is not None


def _template_pattern(template: str | None) -> re.Pattern[str] | None:
    """The served template's words before ``<handle>``
    (whitespace-insensitive), else the first words after it."""
    if not template or _HANDLE_PLACEHOLDER not in template:
        return None
    index = template.index(_HANDLE_PLACEHOLDER)
    head = template[:index].split()
    tail = template[index + len(_HANDLE_PLACEHOLDER) :].split()
    if head:
        words = r"\s+".join(re.escape(word) for word in head)
        # ``@`` closes the head when the template writes ``@<handle>``; the
        # capture takes an optional one, so drop a trailing ``@``.
        if words.endswith("@"):
            words = words[:-1]
        return re.compile(rf"{words}\s*{_HANDLE_CAPTURE}")
    if tail:
        words = r"\s+".join(re.escape(word) for word in tail[:3])
        return re.compile(rf"{_HANDLE_CAPTURE}\s*{words}")
    return None


def _handle_from_wording(body: str, template: str | None) -> str | None:
    patterns: list[re.Pattern[str]] = []
    from_template = _template_pattern(template)
    if from_template is not None:
        patterns.append(from_template)
    patterns.extend(_OLD_WORDINGS)
    flat = " ".join(body.split())
    for pattern in patterns:
        match = pattern.search(flat) or pattern.search(body)
        if match and match.group(1):
            return match.group(1).lower()
    return None


__all__ = [
    "END_MARKER",
    "LEGACY_END_MARKER",
    "LEGACY_START_MARKER",
    "START_MARKER",
    "FoundBlock",
    "find",
    "handle_line",
    "normalize_handle",
    "remove",
    "render",
    "replace",
]
