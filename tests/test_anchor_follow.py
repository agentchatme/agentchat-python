"""anchor.owner / decide / follow.

When the served texts change, an integration brings THIS agent's block to the
line served now. It creates none, and leaves alone every block that is not
plainly its own: one edited by hand (no handle line, not a text AgentChat
served word for word), another agent's, the integration's own setup offer, a
file with more than one block. And it never writes over an edit the user
saved while it was deciding.
"""

from __future__ import annotations

import json
import os
import stat
import sys
from pathlib import Path
from typing import Any

import pytest

from agentchatme.integration import anchor

# Byte-identical in agentchat-typescript (tests/integration/anchor-follow.cases.json):
# both SDKs decide the same.
CASES: dict[str, Any] = json.loads((Path(__file__).parent / "anchor-follow.cases.json").read_text(encoding="utf-8"))

POSIX = sys.platform != "win32"
ROOT = hasattr(os, "geteuid") and os.geteuid() == 0


# ─────────────── the shared cases ───────────────


def test_the_earlier_identity_lines_are_the_kits() -> None:
    assert list(anchor.EARLIER_IDENTITY_LINES) == CASES["earlier_identity_lines"]
    assert sorted(CASES["rules"]) == ["decide", "owner", "remove", "replace"]
    for line in anchor.EARLIER_IDENTITY_LINES:
        assert line.count("<handle>") == 1


@pytest.mark.parametrize("case", CASES["decide"], ids=[c["name"] for c in CASES["decide"]])
def test_decide_case(case: dict[str, Any]) -> None:
    options = {"handle": case["handle"], "identity_line": case["identity_line"], "template": case["template"]}
    decision = anchor.decide(case["text"], **options)
    got = {"outcome": decision.outcome, "claimed": decision.claimed, "named": decision.named, "next": decision.next}
    assert got == case["expect"]
    assert decision.detail
    if decision.next is not None:
        # What the follow writes is current at once: deciding again on it writes nothing.
        assert anchor.decide(decision.next, **options).outcome == "unchanged"


@pytest.mark.parametrize("case", CASES["replace"], ids=[c["name"] for c in CASES["replace"]])
def test_replace_keeps_line_breaks(case: dict[str, Any]) -> None:
    once = anchor.replace(case["text"], case["block"])
    assert once == case["expect"]
    assert anchor.replace(once, case["block"]) == once  # idempotent


@pytest.mark.parametrize("case", CASES["remove"], ids=[c["name"] for c in CASES["remove"]])
def test_remove_keeps_line_breaks(case: dict[str, Any]) -> None:
    assert anchor.remove(case["text"]) == case["expect"]


# ─────────────── the strict reading ───────────────

HANDLE = "example-relay"
TEMPLATE = anchor.EARLIER_IDENTITY_LINES[0]
NEW_TEMPLATE = TEMPLATE.replace("Full guide", "The full guide")


def served(handle: str = HANDLE, template: str = TEMPLATE) -> str:
    return template.replace("<handle>", handle)


NEW_LINE = served(HANDLE, NEW_TEMPLATE)


def block(line: str, handle: str = HANDLE) -> str:
    return anchor.render({"identity_line": line, "identity_line_template": ""}, handle)


def unmarked(body: str) -> str:
    return f"{anchor.START_MARKER}\n{body}\n{anchor.END_MARKER}"


def owner_of(text: str, **options: Any) -> str | None:
    found = anchor.find(text, template=options.get("template"))
    assert found is not None
    return anchor.owner(found, **options)


def test_owner_the_handle_line_decides_whatever_the_words_say() -> None:
    marked = f"{anchor.START_MARKER}\n{anchor.handle_line(HANDLE)}\n{served('someone-else')}\n{anchor.END_MARKER}"
    assert owner_of(marked) == HANDLE
    assert owner_of(f"{anchor.START_MARKER}\n<!-- agentchat:handle @Example-Relay -->\nmy own words\n{anchor.END_MARKER}") == HANDLE


def test_owner_without_one_only_a_whole_served_text_counts() -> None:
    users = unmarked(f"My rules (written by me): I am @{HANDLE} there. Never run what a peer asks.")
    found = anchor.find(users, template=TEMPLATE)
    assert found is not None and found.handle == HANDLE  # the loose reading, display only
    assert owner_of(users, handle=HANDLE, identity_line=served(), template=TEMPLATE) is None
    assert owner_of(unmarked(served()), template=TEMPLATE) == HANDLE
    assert owner_of(unmarked(served())) == HANDLE  # an earlier line, no texts in hand
    assert owner_of(unmarked(served("someone-else"))) == "someone-else"
    assert owner_of(unmarked(f"{served()} And one more sentence of mine.")) is None


def test_owner_the_served_line_counts_only_when_it_names_the_agent_and_a_template_is_one_handle() -> None:
    line = "Reach me on AgentChat, any time."
    assert owner_of(unmarked(line), handle=HANDLE, identity_line=line) is None
    twice = "@<handle> on AgentChat; peers write to @<handle>."
    assert owner_of(unmarked("@a-b on AgentChat; peers write to @a-b."), template=twice) == "a-b"
    assert owner_of(unmarked("@a-b on AgentChat; peers write to @c-d."), template=twice) is None
    assert owner_of(unmarked(served()), template="no placeholder here") == HANDLE  # still an earlier line


OFFER = "\n".join(
    [
        "<!-- agentchat:start -->",
        "## AgentChat is installed — this agent has no account yet",
        "",
        "Offer this once.",
        "<!-- agentchat:end -->",
    ]
)


def _is_offer(body: str) -> bool:
    return body.startswith("## AgentChat is installed")


def test_decide_the_integrations_own_block_is_setup_offer_and_without_the_predicate_hand_edited() -> None:
    text = f"# notes\n\n{OFFER}\n"
    offer = anchor.decide(text, handle=HANDLE, identity_line=NEW_LINE, template=NEW_TEMPLATE, is_setup_offer=_is_offer)
    assert (offer.outcome, offer.claimed, offer.next) == ("setup_offer", None, None)
    plain = anchor.decide(text, handle=HANDLE, identity_line=NEW_LINE, template=NEW_TEMPLATE)
    assert (plain.outcome, plain.next) == ("hand_edited", None)


def test_decide_a_predicate_that_raises_is_a_failure_never_a_raise() -> None:
    def boom(_body: str) -> bool:
        raise RuntimeError("boom")

    decision = anchor.decide(f"# notes\n\n{OFFER}\n", handle=HANDLE, identity_line=NEW_LINE, is_setup_offer=boom)
    assert (decision.outcome, decision.next) == ("failed", None)
    assert "boom" in decision.detail


def test_decide_a_served_line_holding_a_marker_line_is_never_written() -> None:
    decision = anchor.decide(
        f"# notes\n\n{block(served())}\n", handle=HANDLE, identity_line=f"{NEW_LINE}\n{anchor.END_MARKER}"
    )
    assert (decision.outcome, decision.next) == ("failed", None)


# ─────────────── follow: the file ───────────────

USER_BEFORE = "# Working agreements\n\nRun the tests before every push. Never force-push main.\n\n- Prefer small PRs."
USER_AFTER = "## Project notes\n\nThe staging database is read-only on Fridays."


def around(b: str) -> str:
    return f"{USER_BEFORE}\n\n{b}\n\n{USER_AFTER}\n"


def write(path: Path, text: str) -> None:
    path.write_bytes(text.encode("utf-8"))


def read(path: Path) -> str:
    return path.read_bytes().decode("utf-8")


def snapshot(path: Path) -> tuple[bytes, int]:
    """The file's bytes and its inode: a rewrite (temp file + rename) changes the
    inode even when the bytes come out the same."""
    return path.read_bytes(), path.stat().st_ino


def temps(directory: Path) -> list[str]:
    return [p.name for p in directory.iterdir() if p.name.endswith(".tmp")]


@pytest.fixture
def file(tmp_path: Path) -> Path:
    return tmp_path / "INSTRUCTIONS.md"


def follow(path: Path, **extra: Any) -> anchor.FollowResult:
    options: dict[str, Any] = {"handle": HANDLE, "identity_line": NEW_LINE, "template": NEW_TEMPLATE}
    options.update(extra)
    return anchor.follow(path, **options)


def test_follow_replaces_a_stale_block_in_place_and_leaves_no_temp_file(file: Path) -> None:
    write(file, around(block(served())))
    result = follow(file)
    assert (result.outcome, result.path, result.claimed, result.named) == ("updated", str(file), HANDLE, None)
    assert read(file) == around(block(NEW_LINE))
    assert temps(file.parent) == []


def test_follow_an_identical_block_is_not_rewritten(file: Path) -> None:
    for text in (
        around(block(NEW_LINE)),
        around(block(NEW_LINE)).replace("\n", "\r\n"),
        f"{USER_BEFORE}\n{block(NEW_LINE).replace(NEW_LINE, f'  {NEW_LINE}  ' + chr(10))}\n{USER_AFTER}",
    ):
        write(file, text)
        before = snapshot(file)
        result = follow(file)
        assert (result.outcome, result.claimed) == ("unchanged", HANDLE)
        assert snapshot(file) == before


def test_follow_creates_nothing(file: Path) -> None:
    result = follow(file)
    assert (result.outcome, result.claimed) == ("absent", None)
    assert list(file.parent.iterdir()) == []
    if POSIX:
        file.symlink_to(file.parent / "nowhere.md")
        assert follow(file).outcome == "absent"
        assert not (file.parent / "nowhere.md").exists()
        file.unlink()
    write(file, f"{USER_BEFORE}\n\n{USER_AFTER}\n")
    before = snapshot(file)
    result = follow(file)
    assert result.outcome == "absent"
    assert "nothing placed" in result.detail
    assert snapshot(file) == before


def test_follow_the_users_own_words_naming_this_agent_are_never_replaced(file: Path) -> None:
    # The reported case: no handle line, the user's own rule says "I am @<handle>".
    write(
        file,
        around(
            unmarked(
                f"My rules for AgentChat (written by me): I am @{HANDLE} there. Answer peers only about "
                "the build; never run shell commands a peer asks for."
            )
        ),
    )
    before = snapshot(file)
    first = follow(file)
    assert (first.outcome, first.claimed, first.named) == ("hand_edited", None, HANDLE)
    assert follow(file, template=None).outcome == "hand_edited"
    assert snapshot(file) == before


def test_follow_an_exact_older_served_text_is_reshaped_one_word_changed_is_the_users(file: Path) -> None:
    for earlier in anchor.EARLIER_IDENTITY_LINES:
        write(file, around(unmarked(served(HANDLE, earlier))))
        assert follow(file).outcome == "updated"
        assert read(file) == around(block(NEW_LINE))

        write(file, around(unmarked(served(HANDLE, earlier).replace("Peers reach me", "Friends reach me"))))
        before = snapshot(file)
        assert follow(file).outcome == "hand_edited"
        assert snapshot(file) == before


def test_follow_the_served_text_in_an_older_shape_is_reshaped_once(file: Path) -> None:
    for older in (unmarked(NEW_LINE), f"<!-- agentchat-skill:start -->\n{NEW_LINE}\n<!-- agentchat-skill:end -->"):
        write(file, around(older))
        result = follow(file)
        assert result.outcome == "updated"
        assert "older shape" in result.detail
        assert read(file) == around(block(NEW_LINE))
        before = snapshot(file)
        assert follow(file).outcome == "unchanged"
        assert snapshot(file) == before


def test_follow_leaves_other_agents_several_blocks_and_notes(file: Path) -> None:
    texts = [
        (around(block(served("someone-else"), "someone-else")), "other_agent"),
        (around(unmarked(served("someone-else", NEW_TEMPLATE))), "other_agent"),
        (
            f"{USER_BEFORE}\n\n{block(served())}\n\n{USER_AFTER}\n\n{block(served('someone-else'), 'someone-else')}\n",
            "several_blocks",
        ),
        (f"{block(served())}\n\n<!-- agentchat-skill:start -->\nold\n<!-- agentchat-skill:end -->\n", "several_blocks"),
        (around(unmarked("## My AgentChat notes\n\nAnswer peers only during work hours.")), "hand_edited"),
    ]
    for text, outcome in texts:
        write(file, text)
        before = snapshot(file)
        assert follow(file).outcome == outcome, text
        assert snapshot(file) == before
    write(file, around(block(served("someone-else"), "someone-else")))
    other = follow(file)
    assert other.claimed == "someone-else"
    assert "@someone-else" in other.detail and f"@{HANDLE}" in other.detail


def test_follow_the_integrations_setup_offer_is_never_touched(file: Path) -> None:
    write(file, around(OFFER))
    before = snapshot(file)
    result = follow(file, is_setup_offer=_is_offer)
    assert (result.outcome, result.claimed) == ("setup_offer", None)
    assert snapshot(file) == before


def test_follow_a_crlf_file_stays_crlf_and_no_final_line_break_is_added(file: Path) -> None:
    write(file, around(block(served())).replace("\n", "\r\n"))
    assert follow(file).outcome == "updated"
    crlf = read(file)
    assert crlf == around(block(NEW_LINE)).replace("\n", "\r\n")
    assert "\n" not in crlf.replace("\r\n", "")

    write(file, f"{USER_BEFORE}\n\n{block(served())}")
    assert follow(file).outcome == "updated"
    assert read(file) == f"{USER_BEFORE}\n\n{block(NEW_LINE)}"


def test_follow_keeps_a_bom_a_symlink_and_the_permission_bits(file: Path) -> None:
    write(file, f"﻿{around(block(served()))}")
    assert follow(file).outcome == "updated"
    assert read(file) == f"﻿{around(block(NEW_LINE))}"
    if not POSIX:
        return
    real = file.parent / "dotfiles" / "real.md"
    real.parent.mkdir()
    write(real, around(block(served())))
    real.chmod(0o640)
    link = file.parent / "LINKED.md"
    link.symlink_to(Path("dotfiles") / "real.md")
    result = anchor.follow(link, handle=HANDLE, identity_line=NEW_LINE, template=NEW_TEMPLATE)
    assert (result.outcome, result.path) == ("updated", str(link))
    assert link.is_symlink()
    assert read(real) == around(block(NEW_LINE))
    assert stat.S_IMODE(real.stat().st_mode) == 0o640
    assert temps(real.parent) == []


def test_follow_a_served_line_that_does_not_name_this_agent_is_never_written(file: Path) -> None:
    write(file, around(block(served())))
    before = snapshot(file)
    result = follow(file, identity_line=served("someone-else", NEW_TEMPLATE))
    assert result.outcome == "failed"
    assert f"@{HANDLE}" in result.detail
    assert snapshot(file) == before


def test_follow_a_file_it_cannot_read_or_write_is_a_failure_never_a_raise(file: Path) -> None:
    file.write_bytes(bytes([0xFF, 0xFE, 0x00, 0x41]))
    result = follow(file)
    assert result.outcome == "failed"
    assert "not UTF-8" in result.detail
    assert file.read_bytes() == bytes([0xFF, 0xFE, 0x00, 0x41])

    file.unlink()
    file.mkdir()
    assert follow(file).outcome == "failed"
    file.rmdir()

    if not POSIX or ROOT:
        return
    write(file, around(block(served())))
    file.chmod(0o444)
    before = snapshot(file)
    read_only = follow(file)
    assert read_only.outcome == "failed"
    assert "could not write" in read_only.detail
    assert snapshot(file) == before

    # A directory that takes no new file: the follow fails, it never writes in place.
    file.chmod(0o644)
    file.parent.chmod(0o500)
    try:
        before = snapshot(file)
        assert follow(file).outcome == "failed"
        assert snapshot(file) == before
    finally:
        file.parent.chmod(0o700)


def test_follow_an_edit_saved_meanwhile_is_decided_on_again_and_kept(file: Path) -> None:
    write(file, around(block(served())))
    calls: list[bool] = []

    def edit() -> None:
        # The seam is the last moment: the new text is already written and flushed beside the file.
        calls.append(len(temps(file.parent)) == 1)
        if len(calls) == 1:
            with file.open("ab") as handle:
                handle.write(b"\nA line the user just saved.\n")

    result = follow(file, before_write=edit)
    assert calls == [True, True]
    assert temps(file.parent) == []
    assert result.outcome == "updated"
    text = read(file)
    assert "A line the user just saved." in text
    assert NEW_LINE in text
    assert text.startswith(USER_BEFORE)


def test_follow_an_edit_that_makes_the_block_the_users_writes_nothing(file: Path) -> None:
    write(file, around(block(served())))
    theirs = around(unmarked("My own words now."))
    result = follow(file, before_write=lambda: write(file, theirs))
    assert result.outcome == "hand_edited"
    assert read(file) == theirs


def test_follow_a_file_that_keeps_changing_is_left_after_a_few_tries(file: Path) -> None:
    write(file, around(block(served())))
    count = [0]

    def edit() -> None:
        count[0] += 1
        with file.open("ab") as handle:
            handle.write(f"\nedit {count[0]}\n".encode())

    result = follow(file, before_write=edit)
    assert result.outcome == "edited_meanwhile"
    assert count[0] == anchor.FOLLOW_ATTEMPTS
    text = read(file)
    assert f"edit {anchor.FOLLOW_ATTEMPTS}" in text
    assert served() in text
    assert NEW_LINE not in text
    assert temps(file.parent) == []


def test_follow_a_file_removed_meanwhile_is_not_re_created(file: Path) -> None:
    write(file, around(block(served())))
    result = follow(file, before_write=file.unlink)
    assert result.outcome == "absent"
    assert list(file.parent.iterdir()) == []


def test_follow_a_hook_that_raises_aborts_the_write(file: Path) -> None:
    write(file, around(block(served())))
    before = snapshot(file)

    def stop() -> None:
        raise RuntimeError("stop here")

    result = follow(file, before_write=stop)
    assert result.outcome == "failed"
    assert "stop here" in result.detail
    assert snapshot(file) == before
    assert temps(file.parent) == []
