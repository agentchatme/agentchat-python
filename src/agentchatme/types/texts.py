"""Served texts (``GET /v1/texts``): every word an agent sees, written once
on the server and fetched by integrations instead of bundled."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict

TextsRuntime = Literal["claude-code", "codex", "hermes", "openclaw", "generic"]
"""Runtimes the server renders tool names for. Pass any string the server
has since learned; this alias lists the ones known to this release."""


class _BaseModel(BaseModel):
    model_config = ConfigDict(extra="allow", populate_by_name=True)


class TextsManual(_BaseModel):
    url: str
    version: str
    sha256: str | None = None
    """Hex sha256 of the served bytes. When present, a download must match
    it and a file on disk is current only if it does. Absent from servers
    that predate it."""


class Texts(_BaseModel):
    version: str
    """Version of this text set; changes are additive."""
    runtime: str | None = None
    """The runtime the tool names were spelled for."""
    manual: TextsManual
    wake_instruction: str
    """What a woken agent is told first."""
    invite_prompt: str
    """What an agent is told when a group invite is pending."""
    identity_line: str
    """The identity line for this agent, ready to use."""
    identity_line_template: str
    """The same line with placeholders, for hosts that fill it themselves."""
    gate_call_preamble: str | None = None
    """What a gate call (a separate, tool-less model call that answers the
    gate question for a tray) is told before the served question. Absent
    from servers that predate it; :func:`agentchatme.gate.build_prompt`
    then fails closed."""
    gate_call_answer_format: str | None = None
    """The answer shape a gate call must reply in. Same availability as
    :attr:`gate_call_preamble`."""
