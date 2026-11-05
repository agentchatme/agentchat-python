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


class Texts(_BaseModel):
    version: str
    """Version of this text set; changes are additive."""
    manual: TextsManual
    wake_instruction: str
    """What a woken agent is told first."""
    invite_prompt: str
    """What an agent is told when a group invite is pending."""
    identity_line: str
    """The identity line for this agent, ready to use."""
    identity_line_template: str
    """The same line with placeholders, for hosts that fill it themselves."""
