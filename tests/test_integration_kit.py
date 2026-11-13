"""The integration kit beyond the shared conformance cases: TurnTracker,
TextsCache, ManualSync, probe_hosted_tools and ReceiveLoop."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import stat
import sys
from pathlib import Path
from typing import Any

import httpx
import pytest

from agentchatme.integration import (
    AgentChatCallCount,
    ManualSync,
    Readiness,
    ReceiveLoop,
    RunEnd,
    TextsCache,
    TrackedTurn,
    TurnAdmission,
    TurnEnd,
    TurnOutcome,
    TurnRequest,
    TurnTracker,
    apply_outcome,
    apply_outcome_async,
    probe_hosted_tools,
    probe_hosted_tools_async,
    sync_manual,
)
from agentchatme.types import Texts

from ._texts import texts as texts_body
from .test_realtime import MockWebSocket, _flush

# ─────────────── TurnTracker ───────────────


@pytest.mark.asyncio
async def test_tracker_holds_one_turn_per_conversation_and_per_invite() -> None:
    tracker = TurnTracker(now=lambda: 1000.0)
    first = tracker.begin(TrackedTurn.reply("c1", "m1"))
    assert first is not None and first.started_at == 1000.0
    assert tracker.begin(TrackedTurn.reply("c1", "m2")) is None
    invite = tracker.begin(TrackedTurn.invite("ginv_1"))
    assert invite is not None
    assert tracker.conversation_running("c1") and tracker.invite_running("ginv_1")
    cards = [{"conversation_id": "c1"}, {"conversation_id": "c2"}]
    assert tracker.without_running(cards) == [{"conversation_id": "c2"}]
    assert tracker.size == 2
    tracker.end(first)
    assert not tracker.conversation_running("c1")


@pytest.mark.asyncio
async def test_a_wait_timeout_is_not_an_end_and_next_is_followed() -> None:
    tracker = TurnTracker()
    handle = tracker.begin(TrackedTurn.reply("c1"))
    assert handle is not None
    later: asyncio.Future[TurnEnd] = asyncio.get_running_loop().create_future()
    waiting = asyncio.ensure_future(
        tracker.confirmed_end(handle, _value(TurnEnd(status="still_running", next=later)))
    )
    await _flush()
    assert not waiting.done()
    assert tracker.running()[0].wait_timed_out_at is not None
    later.set_result(TurnEnd(status="completed"))
    end = await asyncio.wait_for(waiting, 1)
    assert end is not None and end.status == "completed"
    assert tracker.conversation_running("c1")  # held until end()


@pytest.mark.asyncio
async def test_a_turn_with_no_known_end_is_held_until_released() -> None:
    tracker = TurnTracker()
    handle = tracker.begin(TrackedTurn.reply("c1"))
    assert handle is not None
    waiting = asyncio.ensure_future(tracker.confirmed_end(handle, TurnEnd(status="still_running")))
    await _flush()
    assert not waiting.done()
    assert tracker.release("conversation:c1") is True
    assert await asyncio.wait_for(waiting, 1) is None
    assert tracker.release("conversation:c1") is False


@pytest.mark.asyncio
async def test_release_ends_a_wait_on_a_report_that_never_comes_and_tells_listeners() -> None:
    tracker = TurnTracker()
    handle = tracker.begin(TrackedTurn.reply("c1"))
    assert handle is not None
    released: list[str] = []
    tracker.on_release(lambda h: released.append(h.key))
    never: asyncio.Future[TurnEnd] = asyncio.get_running_loop().create_future()
    waiting = asyncio.ensure_future(tracker.confirmed_end(handle, never))
    await _flush()
    assert tracker.holds(handle)
    assert tracker.release("conversation:c1") is True
    assert await asyncio.wait_for(waiting, 1) is None
    assert released == ["conversation:c1"] and not tracker.holds(handle)


@pytest.mark.asyncio
async def test_a_shared_tracker_release_frees_the_loops_slot() -> None:
    tracker = TurnTracker()
    tray = _tray([_card("c1", "m1"), _card("c2", "m2")])
    turns = _Turns()
    loop = _loop(
        _LoopClient([tray]),
        call_model=lambda _p: _answer(("c1", "reply"), ("c2", "reply")),
        start_turn=turns,
        turn_tracker=tracker,
    )
    await loop.start()
    await _cycle(loop)
    assert [r.conversation_id for r in turns.requests] == ["c1"]
    tracker.release("conversation:c1")  # the host lets go, not the loop
    await _eventually(lambda: len(turns.requests) == 2, "the waiting turn to start")
    assert [r.conversation_id for r in turns.requests] == ["c1", "c2"]
    await loop.stop()


@pytest.mark.asyncio
async def test_a_report_that_raises_is_a_failed_turn() -> None:
    tracker = TurnTracker()
    handle = tracker.begin(TrackedTurn.reply("c1"))
    assert handle is not None

    async def boom() -> TurnEnd:
        raise RuntimeError("runtime crashed")

    end = await tracker.confirmed_end(handle, boom())
    assert end is not None and end.status == "failed" and "runtime crashed" in (end.detail or "")


async def _value(value: Any) -> Any:
    return value


# ─────────────── TextsCache ───────────────


class _Texts:
    def __init__(self, answers: list[Any]) -> None:
        self.answers = list(answers)
        self.calls: list[dict[str, Any]] = []

    async def get(self, *, runtime: str, opts: Any = None) -> Any:
        self.calls.append({"runtime": runtime, "opts": opts})
        answer = self.answers.pop(0) if len(self.answers) > 1 else self.answers[0]
        if isinstance(answer, Exception):
            raise answer
        return Texts.model_validate(answer)


class _AsyncTextsClient:
    base_url = "https://api.example.com"

    def __init__(self, answers: list[Any]) -> None:
        self.texts = _Texts(answers)


@pytest.mark.asyncio
async def test_texts_cache_fetches_writes_the_file_and_hears_changes(tmp_path: Path) -> None:
    file = tmp_path / "texts.json"
    client = _AsyncTextsClient([texts_body(runtime="hermes")])
    cache = TextsCache(client, runtime="hermes", file_path=file)
    heard: list[str] = []
    cache.on_change(lambda texts, source: heard.append(source))
    try:
        got = await cache.start()
        assert got is not None and cache.source == "server" and cache.fetched_at
        assert heard == ["server"]
        assert client.texts.calls[0]["opts"].retry == "never"
        entry = json.loads(file.read_text())
        assert entry["version"] == 1 and entry["runtime"] == "hermes"
        if sys.platform != "win32":
            assert stat.S_IMODE(os.stat(file).st_mode) == 0o600
        assert await cache.refresh() is True
        assert heard == ["server"]  # same content: no change
        assert cache.api_base == "https://api.example.com"
    finally:
        cache.stop()


@pytest.mark.asyncio
async def test_texts_cache_starts_from_the_file_offline_and_keeps_a_served_copy(tmp_path: Path) -> None:
    file = tmp_path / "texts.json"
    online = TextsCache(_AsyncTextsClient([texts_body(runtime="hermes")]), runtime="hermes", file_path=file)
    await online.start()
    online.stop()

    errors: list[Exception] = []
    offline = TextsCache(
        _AsyncTextsClient([OSError("offline")]), runtime="hermes", file_path=file, on_error=errors.append
    )
    try:
        got = await offline.start()
        assert got is not None and offline.source == "cache"
        assert offline.last_error == "offline" and len(errors) == 1
        await offline.refresh()
        assert len(errors) == 1  # one report per condition
        assert await offline.ensure() is not None
    finally:
        offline.stop()

    other_runtime = TextsCache(_AsyncTextsClient([OSError("offline")]), runtime="openclaw", file_path=file)
    try:
        assert await other_runtime.start() is None  # a file for another runtime does not count
    finally:
        other_runtime.stop()


@pytest.mark.asyncio
async def test_texts_cache_rejects_texts_for_another_runtime() -> None:
    cache = TextsCache(_AsyncTextsClient([texts_body(runtime="openclaw")]), runtime="hermes")
    try:
        assert await cache.start() is None
        assert "asked for hermes" in (cache.last_error or "")
    finally:
        cache.stop()


def test_texts_cache_refresh_sync_with_a_sync_client(tmp_path: Path) -> None:
    class SyncTexts:
        def get(self, *, runtime: str, opts: Any = None) -> Texts:
            return Texts.model_validate(texts_body(runtime=runtime))

    class SyncClient:
        texts = SyncTexts()

    cache = TextsCache(SyncClient(), runtime="hermes", file_path=tmp_path / "t.json")
    assert cache.refresh_sync() is True
    assert cache.current() is not None and (tmp_path / "t.json").exists()


def test_texts_cache_reads_a_hermes_file_with_epoch_seconds(tmp_path: Path) -> None:
    file = tmp_path / "t.json"
    file.write_text(
        json.dumps({"version": 1, "runtime": "hermes", "fetched_at": 1762560000.0, "texts": texts_body()})
    )

    class Failing:
        def get(self, *, runtime: str, opts: Any = None) -> Texts:
            raise OSError("offline")

    class SyncClient:
        texts = Failing()

    cache = TextsCache(SyncClient(), runtime="hermes", file_path=file)
    assert cache.refresh_sync() is False
    assert cache.source == "cache" and (cache.fetched_at or "").startswith("2025-11-08T")


# ─────────────── ManualSync ───────────────


def _manual(version: str = "2.2.2", name: str = "agentchat", *, crlf: bool = False, bom: bool = False) -> bytes:
    text = f"---\nname: {name}\nversion: {version}\n---\n\n# AgentChat\n\nThe manual.\n"
    if crlf:
        text = text.replace("\n", "\r\n")
    data = text.encode()
    return b"\xef\xbb\xbf" + data if bom else data


def _served(data: bytes, version: str = "2.2.2", url: str = "https://agentchat.me/skill.md") -> dict[str, Any]:
    return {"url": url, "version": version, "sha256": hashlib.sha256(data).hexdigest()}


def _http(routes: dict[str, httpx.Response]) -> tuple[httpx.Client, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        response = routes.get(str(request.url))
        return response if response is not None else httpx.Response(404)

    return httpx.Client(transport=httpx.MockTransport(handler)), seen


def test_manual_is_downloaded_checked_and_written(tmp_path: Path) -> None:
    body = _manual()
    target = tmp_path / "agentchat" / "SKILL.md"
    client, seen = _http({"https://agentchat.me/skill.md": httpx.Response(200, content=body)})
    result = sync_manual(path=target, served=_served(body), api_base="https://api.agentchat.me", http_client=client)
    assert result.state == "updated" and result.source == "download" and result.up_to_date is True
    assert target.read_bytes() == body
    assert "authorization" not in seen[0].headers  # never the agent's key
    again = sync_manual(path=target, served=_served(body), api_base="https://api.agentchat.me", http_client=client)
    assert again.state == "current" and len(seen) == 1


@pytest.mark.parametrize(
    ("location", "why"),
    [
        ("http://agentchat.me/skill.md", "HTTPS only"),
        ("https://evil.example.com/skill.md", "neither agentchat.me"),
        ("https://user:pw@agentchat.me/skill.md", "credentials"),
    ],
)
def test_every_redirect_hop_is_checked(tmp_path: Path, location: str, why: str) -> None:
    body = _manual()
    target = tmp_path / "SKILL.md"
    target.write_bytes(_manual("2.0.0"))
    client, _ = _http({"https://agentchat.me/skill.md": httpx.Response(302, headers={"location": location})})
    result = sync_manual(path=target, served=_served(body), api_base="https://api.agentchat.me", http_client=client)
    assert result.state == "kept" and why in (result.error or "")
    assert target.read_bytes() == _manual("2.0.0")  # never worse than before


def test_more_than_three_redirects_is_refused(tmp_path: Path) -> None:
    routes = {
        f"https://agentchat.me/{i}": httpx.Response(302, headers={"location": f"/{i + 1}"}) for i in range(5)
    }
    client, _ = _http(routes)
    body = _manual()
    result = sync_manual(
        path=tmp_path / "S.md",
        served=_served(body, url="https://agentchat.me/0"),
        api_base="https://api.agentchat.me",
        http_client=client,
    )
    assert result.state == "failed" and "redirects" in (result.error or "")


@pytest.mark.parametrize(
    ("content", "why"),
    [
        (_manual("2.2.1"), "the server announced 2.2.2"),
        (_manual(name="other"), "not the agentchat skill"),
        (b"no front matter", "no front matter"),
        (b"\xff\xfe\x00bad", "not UTF-8"),
    ],
)
def test_a_body_that_is_not_the_announced_manual_is_never_written(
    tmp_path: Path, content: bytes, why: str
) -> None:
    target = tmp_path / "SKILL.md"
    client, _ = _http({"https://agentchat.me/skill.md": httpx.Response(200, content=content)})
    served = {"url": "https://agentchat.me/skill.md", "version": "2.2.2"}
    result = sync_manual(path=target, served=served, api_base="https://api.agentchat.me", http_client=client)
    assert result.state == "failed" and why in (result.error or "")
    assert not target.exists()


def test_a_hash_mismatch_or_an_oversized_body_keeps_the_old_file(tmp_path: Path) -> None:
    target = tmp_path / "SKILL.md"
    target.write_bytes(_manual("2.2.2") + b"tampered")
    body = _manual()
    client, _ = _http({"https://agentchat.me/skill.md": httpx.Response(200, content=body + b"x")})
    result = sync_manual(path=target, served=_served(body), api_base="https://api.agentchat.me", http_client=client)
    assert result.state == "kept" and result.tampered and "sha256" in (result.error or "")
    big = httpx.Response(200, content=b"x" * 2000)
    client2, _ = _http({"https://agentchat.me/skill.md": big})
    capped = sync_manual(
        path=target, served=_served(body), api_base="https://api.agentchat.me", http_client=client2, max_bytes=1000
    )
    assert capped.state == "kept" and "cap" in (capped.error or "")


def test_bom_and_crlf_read_the_same(tmp_path: Path) -> None:
    body = _manual(crlf=True, bom=True)
    client, _ = _http({"https://agentchat.me/skill.md": httpx.Response(200, content=body)})
    result = sync_manual(path=tmp_path / "S.md", served=_served(body), api_base="https://api.agentchat.me", http_client=client)
    assert result.state == "updated" and result.version == "2.2.2"


def test_a_plain_http_base_off_loopback_downloads_nothing_and_loopback_may(tmp_path: Path) -> None:
    body = _manual()
    client, seen = _http({"http://localhost:3000/skill.md": httpx.Response(200, content=body)})
    plain = sync_manual(
        path=tmp_path / "a.md", served=_served(body, url="http://localhost:3000/skill.md"),
        api_base="http://api.example.com", http_client=client,
    )
    assert plain.state == "failed" and "not HTTPS" in (plain.error or "") and not seen
    loop = sync_manual(
        path=tmp_path / "b.md", served=_served(body, url="http://localhost:3000/skill.md"),
        api_base="http://localhost:3000", http_client=client,
    )
    assert loop.state == "updated"


def test_the_fallback_is_written_only_without_a_file_or_when_it_is_the_announced_manual(tmp_path: Path) -> None:
    body = _manual()
    target = tmp_path / "SKILL.md"
    offline, _ = _http({})
    first = sync_manual(
        path=target, served=_served(body, version="2.3.0"), api_base="https://api.agentchat.me",
        fallback=body, http_client=offline,
    )
    assert first.state == "fallback" and first.up_to_date is False and target.read_bytes() == body
    same = sync_manual(path=target, served=_served(body), api_base="https://api.agentchat.me", fallback=body)
    assert same.state == "current"
    target.unlink()
    exact = sync_manual(path=target, served=_served(body), api_base="https://api.agentchat.me", fallback=body)
    assert exact.state == "updated" and exact.source == "fallback"


@pytest.mark.asyncio
async def test_manual_sync_follows_the_texts(tmp_path: Path) -> None:
    body = _manual()
    client = _AsyncTextsClient([texts_body(manual=_served(body), runtime="hermes")])
    client.base_url = "https://api.agentchat.me"
    cache = TextsCache(client, runtime="hermes")
    target = tmp_path / "SKILL.md"
    http, _ = _http({"https://agentchat.me/skill.md": httpx.Response(200, content=body)})
    results: list[str] = []
    manual = ManualSync(texts=cache, target_path=target, http_client=http, on_result=lambda r: results.append(r.state))
    try:
        await cache.start()
        first = await manual.start()
        assert first is not None and first.state == "updated"
        assert manual.result is first and results == ["updated"]
    finally:
        manual.stop()
        cache.stop()


# ─────────────── probe_hosted_tools ───────────────


def _mcp(tools: list[str], *, sse: bool = False, status: int = 200) -> tuple[httpx.MockTransport, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if status != 200:
            return httpx.Response(status)
        body = json.loads(request.content)
        if body["method"] == "initialize":
            result: dict[str, Any] = {
                "protocolVersion": "2025-06-18",
                "serverInfo": {"name": "agentchat", "version": "0.4.1"},
            }
            headers = {"mcp-session-id": "sess-1"}
        else:
            result = {"tools": [{"name": name} for name in tools]}
            headers = {}
        payload = json.dumps({"jsonrpc": "2.0", "id": body["id"], "result": result})
        if sse:
            return httpx.Response(200, text=f"event: message\ndata: {payload}\n\n", headers=headers)
        return httpx.Response(200, text=payload, headers=headers)

    return httpx.MockTransport(handler), seen


def test_probe_lists_the_tools_and_forwards_the_session() -> None:
    transport, seen = _mcp(["agentchat_inbox", "agentchat_decide", "agentchat_send_message"])
    result = probe_hosted_tools(
        api_base="https://api.example.com/", api_key="key", http_client=httpx.Client(transport=transport)
    )
    assert result.ok and result.server_name == "agentchat" and result.missing == []
    assert str(seen[0].url) == "https://api.example.com/mcp"
    assert seen[0].headers["authorization"] == "Bearer key"
    assert seen[1].headers["mcp-session-id"] == "sess-1"
    assert seen[1].headers["mcp-protocol-version"] == "2025-06-18"


def test_probe_reports_missing_tools_and_a_key_that_does_not_authenticate() -> None:
    transport, _ = _mcp(["agentchat_inbox"], sse=True)
    missing = probe_hosted_tools(api_base="https://a.example", api_key="k", http_client=httpx.Client(transport=transport))
    assert not missing.ok and missing.missing == ["agentchat_decide"]
    denied_transport, _ = _mcp([], status=401)
    denied = probe_hosted_tools(api_base="https://a.example", api_key="k", http_client=httpx.Client(transport=denied_transport))
    assert not denied.ok and denied.status == 401 and "does not authenticate" in (denied.error or "")


@pytest.mark.asyncio
async def test_probe_async() -> None:
    transport, _ = _mcp(["agentchat_inbox", "agentchat_decide"])
    result = await probe_hosted_tools_async(
        api_base="https://a.example", api_key="k", http_client=httpx.AsyncClient(transport=transport)
    )
    assert result.ok


# ─────────────── ReceiveLoop ───────────────

QUESTION = "For each conversation decide reply, dismiss or defer."


def _card(cid: str, mid: str, **extra: Any) -> dict[str, Any]:
    card: dict[str, Any] = {
        "conversation_id": cid,
        "type": "direct",
        "relationship": "established",
        "you_owe_reply": True,
        "mentioned_you": False,
        "unread": 1,
        "pace": {"last_60s": 0, "last_10m": 1},
        "preview": {"message_id": mid, "sender": "@peer", "text": "hi", "at": "2026-11-08T09:00:00.000Z"},
        "last_message_id": mid,
        "your_last_message_at": None,
        "your_last_decision": None,
    }
    card.update(extra)
    return card


def _tray(cards: list[dict[str, Any]], invites: list[dict[str, Any]] | None = None, **extra: Any) -> dict[str, Any]:
    tray: dict[str, Any] = {
        "version": 1,
        "you": "@nova",
        "question": QUESTION,
        "categories": ["open_request", "closing", "no_action_needed"],
        "conversations": cards,
        "deferred": 0,
        "truncated": False,
        "invites": invites or [],
        "invites_truncated": False,
    }
    tray.update(extra)
    return tray


class _Inbox:
    def __init__(self, trays: list[Any]) -> None:
        self.trays = list(trays)
        self.reads = 0

    async def tray(self, *, wait_ms: int = 0, limit: int | None = None, include_invites: bool = False) -> Any:
        self.reads += 1
        self.include_invites = include_invites
        tray = self.trays.pop(0) if len(self.trays) > 1 else self.trays[0]
        if isinstance(tray, Exception):
            raise tray
        return tray


class _Decisions:
    def __init__(self) -> None:
        self.recorded: list[dict[str, Any]] = []

    async def record(self, conversation_id: str, **kwargs: Any) -> Any:
        # None means "not sent" on the real client.
        given = {k: v for k, v in kwargs.items() if v is not None}
        self.recorded.append({"conversation_id": conversation_id, **given})
        return {"id": "dec"}


class _Groups:
    def __init__(self) -> None:
        self.dismissed: list[str] = []

    async def dismiss_invite(self, invite_id: str) -> Any:
        self.dismissed.append(invite_id)
        return {"dismissed": True}


class _LoopClient:
    base_url = "https://api.example.com"

    def __init__(self, trays: list[Any]) -> None:
        self.inbox = _Inbox(trays)
        self.decisions = _Decisions()
        self.groups = _Groups()


def _answer(*verdicts: tuple[str, str]) -> str:
    return json.dumps({"decisions": [{"conversation_id": c, "decision": d} for c, d in verdicts]})


class _Turns:
    """A runtime: each turn waits until the test ends it."""

    def __init__(self) -> None:
        self.requests: list[TurnRequest] = []
        self.ends: list[asyncio.Future[TurnEnd]] = []

    async def __call__(self, request: TurnRequest) -> TurnAdmission:
        self.requests.append(request)
        done: asyncio.Future[TurnEnd] = asyncio.get_running_loop().create_future()
        self.ends.append(done)
        return TurnAdmission(started=True, done=done)


async def _eventually(predicate: Any, what: str = "condition", *, timeout: float = 5.0) -> None:
    """Wait until ``predicate()`` holds (condition-driven: no fixed sleep
    that a coarse timer could cut short). On Windows before Python 3.13 the
    event loop's clock ticks every 15.6 ms, so ``asyncio.sleep(0.02)`` can
    return after a few milliseconds, before a worker thread or a turn's end
    has run: wait for what the test needs, never for a duration."""
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError(f"timed out after {timeout:g}s waiting for {what}")
        await asyncio.sleep(0.005)


async def _cycle(loop: ReceiveLoop) -> None:
    loop.wake()
    for _ in range(3):
        await asyncio.sleep(0.01)
        await loop.idle()


def _loop(client: _LoopClient, **kwargs: Any) -> ReceiveLoop:
    kwargs.setdefault("socket", None)
    kwargs.setdefault("quiet_ms", 0)
    kwargs.setdefault("max_ms", 0)
    return ReceiveLoop(client, Texts.model_validate(texts_body()), kwargs.pop("mode", "gate_call"), **kwargs)


@pytest.mark.asyncio
async def test_the_loop_reads_the_tray_with_invites() -> None:
    client = _LoopClient([_tray([])])
    loop = _loop(client, call_model=lambda _p: "", start_turn=_Turns())
    await loop.start()
    await _cycle(loop)
    assert client.inbox.include_invites is True
    await loop.stop()


def test_receive_loop_needs_its_callbacks() -> None:
    client = _LoopClient([_tray([])])
    with pytest.raises(ValueError):
        ReceiveLoop(client, Texts.model_validate(texts_body()), "gate_call", socket=None)
    with pytest.raises(ValueError):
        ReceiveLoop(client, Texts.model_validate(texts_body()), "single_run", socket=None)


@pytest.mark.asyncio
async def test_gate_call_records_dismiss_at_once_and_reply_once_the_card_shows_the_send() -> None:
    gated = _tray([_card("c1", "m1"), _card("c2", "m2")])
    sent = _tray([_card("c1", "m1", your_last_message_at="2026-11-08T09:05:00.000Z", you_owe_reply=False)])
    client = _LoopClient([gated, sent])
    turns = _Turns()
    prompts: list[str] = []

    def call_model(prompt: str) -> str:  # a blocking model call runs in a thread
        prompts.append(prompt)
        return _answer(("c1", "reply"), ("c2", "dismiss"))

    loop = _loop(client, call_model=call_model, start_turn=turns)
    await loop.start()
    await _cycle(loop)
    assert client.decisions.recorded == [
        {"conversation_id": "c2", "decision": "dismiss", "through_message_id": "m2"}
    ]
    assert len(turns.requests) == 1 and turns.requests[0].conversation_id == "c1"
    assert turns.requests[0].prompt.endswith("conversation_id: c1")
    assert prompts[0].startswith(texts_body()["gate_call_preamble"])
    turns.ends[0].set_result(TurnEnd(status="completed", agentchat_calls=AgentChatCallCount(made=3, failed=0)))
    await _eventually(lambda: loop.status().turns == [], "the turn's end to be recorded")
    assert client.decisions.recorded[-1] == {
        "conversation_id": "c1",
        "decision": "reply",
        "through_message_id": "m1",
    }
    status = loop.status()
    assert status.counters["outcomes"] == {"answer_sent": 1} and status.turns == []
    await loop.stop()


@pytest.mark.asyncio
async def test_a_running_turn_holds_its_card_and_a_wait_timeout_starts_no_second_turn() -> None:
    tray = _tray([_card("c1", "m1")])
    client = _LoopClient([tray])
    turns = _Turns()
    model_calls: list[str] = []

    async def call_model(prompt: str) -> str:
        model_calls.append(prompt)
        return _answer(("c1", "reply"))

    loop = _loop(client, call_model=call_model, start_turn=turns)
    await loop.start()
    await _cycle(loop)
    assert len(turns.requests) == 1
    next_end: asyncio.Future[TurnEnd] = asyncio.get_running_loop().create_future()
    turns.ends[0].set_result(TurnEnd(status="still_running", next=next_end))
    await _cycle(loop)  # the card is held: no gate call, no second turn
    assert len(model_calls) == 1 and len(turns.requests) == 1
    assert loop.status().last_cycle is not None and loop.status().last_cycle.outcome == "nothing_owed"
    reads = client.inbox.reads
    cycles = loop.status().coalescer["cycles"]
    next_end.set_result(TurnEnd(status="failed"))
    await _eventually(lambda: loop.status().coalescer["cycles"] > cycles, "the turn's end to wake a cycle")
    await loop.idle()
    # The turn ended: the held card is read again (and gated again).
    assert client.inbox.reads > reads
    await loop.stop()


@pytest.mark.asyncio
async def test_looked_and_did_nothing_is_dismissed_the_second_time_for_the_same_anchor() -> None:
    tray = _tray([_card("c1", "m1")])
    client = _LoopClient([tray])
    turns = _Turns()
    loop = _loop(client, call_model=lambda _p: _answer(("c1", "reply")), start_turn=turns, pace_ms=0)
    await loop.start()
    for index in range(2):
        await _cycle(loop)
        turns.ends[index].set_result(TurnEnd(status="completed", agentchat_calls=AgentChatCallCount(made=3, failed=0)))
        await _eventually(lambda i=index: sum(loop.status().counters["outcomes"].values()) == i + 1)
    await _eventually(lambda: len(client.decisions.recorded) == 1)
    assert client.decisions.recorded == [
        {"conversation_id": "c1", "decision": "dismiss", "category": "no_action_needed", "through_message_id": "m1"}
    ]
    assert loop.status().counters["outcomes"] == {"silent_first": 1, "silent_again": 1}
    await loop.stop()


def _outcomes() -> list[TurnOutcome]:
    return [
        TurnOutcome("record_reply", "answer_sent", "c1", "m1", "open_request"),
        TurnOutcome("record_dismiss", "silent_again", "c2", None, "no_action_needed"),
        TurnOutcome("record_defer", "silent_again", "c3", "m3", None),
        TurnOutcome("none", "silent_first", "c4"),
    ]


_RECORDED = [
    {"conversation_id": "c1", "decision": "reply", "category": "open_request", "through_message_id": "m1"},
    {"conversation_id": "c2", "decision": "dismiss", "category": "no_action_needed"},
    {"conversation_id": "c3", "decision": "defer", "through_message_id": "m3"},
]


def test_apply_outcome_makes_one_recording_call_per_action() -> None:
    recorded: list[dict[str, Any]] = []

    class _SyncDecisions:
        def record(self, conversation_id: str, **kwargs: Any) -> Any:
            recorded.append({"conversation_id": conversation_id, **kwargs})
            return {"id": "dec"}

    class _SyncClient:
        decisions = _SyncDecisions()
        groups = None

    assert [apply_outcome(_SyncClient(), o) for o in _outcomes()] == [True, True, True, False]
    assert recorded == _RECORDED


@pytest.mark.asyncio
async def test_apply_outcome_async_makes_one_recording_call_per_action() -> None:
    client = _LoopClient([_tray([])])
    assert [await apply_outcome_async(client, o) for o in _outcomes()] == [True, True, True, False]
    assert client.decisions.recorded == _RECORDED


@pytest.mark.asyncio
async def test_with_the_calls_unknown_the_second_silent_look_records_defer_not_dismiss() -> None:
    client = _LoopClient([_tray([_card("c1", "m1")])])
    turns = _Turns()
    loop = _loop(client, call_model=lambda _p: _answer(("c1", "reply")), start_turn=turns, pace_ms=0)
    await loop.start()
    for index in range(2):
        await _cycle(loop)
        turns.ends[index].set_result(TurnEnd(status="completed", agentchat_calls=None))
        await _eventually(lambda i=index: sum(loop.status().counters["outcomes"].values()) == i + 1)
    # Unread, back on the server's reminder ladder: no category, the gate's anchor.
    await _eventually(lambda: len(client.decisions.recorded) == 1)
    assert client.decisions.recorded == [{"conversation_id": "c1", "decision": "defer", "through_message_id": "m1"}]
    assert loop.status().counters["outcomes"] == {"silent_first": 1, "silent_again": 1}
    await loop.stop()


@pytest.mark.asyncio
async def test_a_turn_that_sent_and_then_failed_records_reply_and_is_not_gated_again() -> None:
    sent = _tray([_card("c1", "m1", you_owe_reply=False, your_last_message_at="2026-11-08T09:11:30.000Z")])
    client = _LoopClient([_tray([_card("c1", "m1")]), sent])
    turns = _Turns()
    loop = _loop(client, call_model=lambda _p: _answer(("c1", "reply")), start_turn=turns)
    await loop.start()
    await _cycle(loop)
    turns.ends[0].set_result(TurnEnd(status="failed", detail="the runtime hit its time limit after the send"))
    await _eventually(lambda: len(client.decisions.recorded) == 1)
    assert client.decisions.recorded == [
        {"conversation_id": "c1", "decision": "reply", "through_message_id": "m1"}
    ]
    assert loop.status().counters["outcomes"] == {"answer_sent": 1}
    await loop.stop()


@pytest.mark.parametrize("record_ms", [30, 100])
@pytest.mark.asyncio
async def test_an_abandoned_cycle_records_nothing_more_and_each_conversation_once(record_ms: int) -> None:
    # The watchdog abandons cycles mid-recording. A verdict in flight is not
    # aborted (the server may have it) and its card is held out of the next
    # gate until it answers: every conversation is recorded exactly once.
    cards = [_card(f"c{i}", f"m{i}") for i in range(4)]
    recorded: set[str] = set()
    calls: list[str] = []

    class SlowInbox:
        async def tray(self, **_kw: Any) -> Any:
            return _tray([c for c in cards if c["conversation_id"] not in recorded])

    class SlowDecisions:
        async def record(self, conversation_id: str, **_kw: Any) -> Any:
            calls.append(conversation_id)
            await asyncio.sleep(record_ms / 1000.0)
            recorded.add(conversation_id)
            return {"id": "d"}

    client = _LoopClient([_tray([])])
    client.inbox = SlowInbox()  # type: ignore[assignment]
    client.decisions = SlowDecisions()  # type: ignore[assignment]

    def call_model(prompt: str) -> str:
        ids = [c["conversation_id"] for c in cards if c["conversation_id"] in prompt]
        return json.dumps({"decisions": [{"conversation_id": i, "decision": "defer"} for i in ids]})

    loop = _loop(client, call_model=call_model, start_turn=_Turns(), cycle_timeout_ms=50)
    await loop.start()
    loop.wake()
    await _eventually(lambda: loop.status().last_cycle is not None)
    deadline = asyncio.get_running_loop().time() + 10.0
    while len(recorded) < len(cards):
        assert asyncio.get_running_loop().time() < deadline, "not every conversation was recorded"
        loop.wake()
        await asyncio.sleep(0.02)
    await loop.idle()
    loop.wake()  # one more look: nothing is gated again
    await asyncio.sleep(0.02)
    await loop.idle()
    per: dict[str, int] = {}
    for c in calls:
        per[c] = per.get(c, 0) + 1
    assert per == {"c0": 1, "c1": 1, "c2": 1, "c3": 1}
    assert loop.status().recent_cycles[0].outcome == "abandoned"
    await loop.stop()


@pytest.mark.asyncio
async def test_a_release_from_another_thread_starts_the_waiting_turn() -> None:
    import threading

    client = _LoopClient([_tray([_card("c1", "m1"), _card("c2", "m2")])])
    turns = _Turns()
    loop = _loop(client, call_model=lambda _p: _answer(("c1", "reply"), ("c2", "reply")), start_turn=turns)
    await loop.start()
    await _cycle(loop)
    assert len(turns.requests) == 1 and loop.status().counters["turns_waiting"] == 1
    released: list[bool] = []
    worker = threading.Thread(target=lambda: released.append(loop._tracker.release("conversation:c1")))
    worker.start()
    worker.join()
    await _eventually(lambda: len(turns.requests) == 2)
    assert released == [True]
    assert len(turns.requests) == 2 and loop.status().counters["turns_waiting"] == 0
    await loop.stop()


@pytest.mark.asyncio
async def test_release_turn_and_wake_from_another_thread_reach_the_loop() -> None:
    import threading

    client = _LoopClient([_tray([_card("c1", "m1")])])
    turns = _Turns()
    events: list[dict[str, Any]] = []
    loop = _loop(
        client, call_model=lambda _p: _answer(("c1", "reply")), start_turn=turns, on_event=events.append
    )
    await loop.start()
    await _cycle(loop)
    assert len(turns.requests) == 1
    cycles = loop.status().coalescer["cycles"]
    errors: list[BaseException] = []
    results: list[bool] = []

    def other() -> None:
        try:
            results.append(loop.release_turn("conversation:c1"))
            loop.wake()
        except BaseException as err:
            errors.append(err)

    worker = threading.Thread(target=other)
    worker.start()
    worker.join()
    await _eventually(lambda: len(turns.requests) == 2)
    await loop.idle()
    assert errors == [] and results == [True]
    sources = [e["source"] for e in events if e["type"] == "wake"]
    assert "more" in sources and "external" in sources
    # The wake was not lost: a cycle ran, and the released card was gated again.
    assert loop.status().coalescer["cycles"] > cycles and len(turns.requests) == 2
    await loop.stop()


def test_status_shows_whether_the_socket_refuses_new_messages() -> None:
    from agentchatme import RealtimeClient

    socket = RealtimeClient(api_key="sk_test", reconnect=False)
    loop = _loop(_LoopClient([_tray([])]), call_model=lambda _p: "", start_turn=_Turns(), socket=socket)
    shown = loop.status().socket
    assert shown is not None and shown["intake_paused"] is False
    socket._shedding = True  # backpressure refuses new messages
    assert loop.status().socket["intake_paused"] is True  # type: ignore[index]


@pytest.mark.asyncio
async def test_stop_mid_gate_records_no_further_verdict_and_tells_the_model_call() -> None:
    cards = [_card(f"c{i}", f"m{i}") for i in range(3)]
    calls: list[str] = []
    finished: list[str] = []
    c1_on_its_way = asyncio.Event()
    release_c1 = asyncio.Event()

    class HeldDecisions:
        async def record(self, conversation_id: str, **_kw: Any) -> Any:
            calls.append(conversation_id)
            if conversation_id == "c1":
                c1_on_its_way.set()
                await release_c1.wait()
            finished.append(conversation_id)
            return {"id": "d"}

    client = _LoopClient([_tray(cards)])
    client.decisions = HeldDecisions()  # type: ignore[assignment]
    answer = json.dumps({"decisions": [{"conversation_id": c["conversation_id"], "decision": "defer"} for c in cards]})
    loop = _loop(client, call_model=lambda _p: answer, start_turn=_Turns())
    await loop.start()
    loop.wake()
    await asyncio.wait_for(c1_on_its_way.wait(), 5.0)  # c0 recorded, c1 on its way
    stopping = asyncio.ensure_future(loop.stop())
    await _eventually(lambda: loop.status().stopped)
    release_c1.set()  # the verdict on its way finishes (never aborted)
    await asyncio.wait_for(stopping, 5.0)
    assert calls == ["c0", "c1"] and finished == ["c0", "c1"]  # c2 never sent

    told: list[bool] = []
    model_called = asyncio.Event()

    async def waits(_prompt: str, *, cancel: asyncio.Event) -> str:
        model_called.set()
        await cancel.wait()
        told.append(True)
        return answer

    client2 = _LoopClient([_tray(cards)])
    loop2 = _loop(client2, call_model=waits, start_turn=_Turns())
    await loop2.start()
    loop2.wake()
    await asyncio.wait_for(model_called.wait(), 5.0)
    await asyncio.wait_for(loop2.stop(), 5.0)
    assert told == [True] and client2.decisions.recorded == []


@pytest.mark.parametrize("name", ["gate_call_timeout_ms", "cycle_timeout_ms"])
def test_timeouts_none_is_the_default_zero_is_off_negative_is_an_error(name: str) -> None:
    client = _LoopClient([_tray([])])
    default = _loop(client, call_model=lambda _p: "", start_turn=_Turns())
    off = _loop(client, call_model=lambda _p: "", start_turn=_Turns(), **{name: 0})
    infinite = _loop(client, call_model=lambda _p: "", start_turn=_Turns(), **{name: float("inf")})
    if name == "gate_call_timeout_ms":
        assert default._gate_call_timeout_ms == 60_000
        assert off._gate_call_timeout_ms == 0 and infinite._gate_call_timeout_ms == 0
    else:
        assert default._cycle_timeout_s == 600
        assert off._cycle_timeout_s == 0 and infinite._cycle_timeout_s == 0
    with pytest.raises(ValueError):
        _loop(client, call_model=lambda _p: "", start_turn=_Turns(), **{name: -1})


@pytest.mark.asyncio
async def test_a_refused_turn_records_nothing_and_a_sync_turn_runs_in_a_thread() -> None:
    client = _LoopClient([_tray([_card("c1", "m1")])])

    async def refuse(_request: TurnRequest) -> TurnAdmission:
        return TurnAdmission(started=False, error="hosted tools not registered")

    loop = _loop(client, call_model=lambda _p: _answer(("c1", "reply")), start_turn=refuse)
    await loop.start()
    await _cycle(loop)
    summary = loop.status().last_cycle
    assert summary is not None and summary.turns_refused == 1 and client.decisions.recorded == []
    await loop.stop()

    sent = _tray([_card("c1", "m1", you_owe_reply=False)])
    client2 = _LoopClient([_tray([_card("c1", "m1")]), sent])

    def blocking_turn(request: TurnRequest) -> TurnEnd:
        assert request.kind == "reply"
        return TurnEnd(status="completed")

    loop2 = _loop(client2, call_model=lambda _p: _answer(("c1", "reply")), start_turn=blocking_turn)
    await loop2.start()
    await _cycle(loop2)
    await _eventually(lambda: loop2.status().turns == [], "the threaded turn's end to be recorded")
    assert client2.decisions.recorded[-1]["decision"] == "reply"
    await loop2.stop()


@pytest.mark.asyncio
async def test_invites_get_one_turn_each_and_are_dismissed_after_two_silent_turns() -> None:
    invite = {
        "invite_id": "ginv_1",
        "group": {"id": "grp_1", "name": "crew", "member_count": 3},
        "invited_by": "@peer",
        "at": "2026-11-08T09:00:00.000Z",
    }
    client = _LoopClient([_tray([], [invite])])
    turns = _Turns()
    calls: list[str] = []
    loop = _loop(client, call_model=lambda p: calls.append(p) or "", start_turn=turns, pace_ms=0)
    await loop.start()
    for index in range(2):
        await _cycle(loop)
        assert turns.requests[index].kind == "invite"
        assert turns.requests[index].prompt.endswith("invite_id: ginv_1")
        turns.ends[index].set_result(TurnEnd(status="completed", agentchat_calls=AgentChatCallCount(made=2, failed=0)))
        await _eventually(lambda i=index: sum(loop.status().counters["outcomes"].values()) == i + 1)
    await _eventually(lambda: len(client.groups.dismissed) == 1)
    assert calls == []  # invites are not gated
    assert client.groups.dismissed == ["ginv_1"]
    await loop.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "calls",
    [None, AgentChatCallCount(made=2, failed=1)],
    ids=["calls-unknown", "some-calls-failed"],
)
async def test_two_silent_invite_turns_without_confirmed_calls_dismiss_nothing(calls: AgentChatCallCount | None) -> None:
    invite = {
        "invite_id": "ginv_1",
        "group": {"id": "grp_1", "name": "crew", "member_count": 3},
        "invited_by": "@peer",
        "at": "2026-11-08T09:00:00.000Z",
    }
    client = _LoopClient([_tray([], [invite])])
    turns = _Turns()
    loop = _loop(client, call_model=lambda _p: "", start_turn=turns, pace_ms=0)
    await loop.start()
    for index in range(2):
        await _cycle(loop)
        turns.ends[index].set_result(TurnEnd(status="completed", agentchat_calls=calls))
        await _eventually(lambda i=index: sum(loop.status().counters["outcomes"].values()) == i + 1)
    assert client.groups.dismissed == []
    assert loop.status().counters["outcomes"] == {"invite_silent_first": 1, "invite_silent_unconfirmed": 1}
    await loop.stop()


@pytest.mark.asyncio
async def test_muted_only_unready_tools_and_an_unreadable_tray_spend_no_model_call() -> None:
    calls: list[str] = []
    turns = _Turns()
    muted = _LoopClient([_tray([_card("c1", "m1", muted=True)])])
    loop = _loop(muted, call_model=lambda p: calls.append(p) or "", start_turn=turns)
    await loop.start()
    await _cycle(loop)
    assert loop.status().last_cycle.outcome == "nothing_owed"  # type: ignore[union-attr]
    await loop.stop()

    unready = _LoopClient([_tray([_card("c1", "m1")])])
    loop2 = _loop(
        unready,
        call_model=lambda p: calls.append(p) or "",
        start_turn=turns,
        readiness=lambda: Readiness(ready=False, reason="hosted tools not listed"),
    )
    await loop2.start()
    await _cycle(loop2)
    assert loop2.status().last_cycle.outcome == "tools_unavailable"  # type: ignore[union-attr]
    await loop2.stop()

    broken = _LoopClient([OSError("tray down")])
    loop3 = _loop(broken, call_model=lambda p: calls.append(p) or "", start_turn=turns)
    await loop3.start()
    await _cycle(loop3)
    assert loop3.status().last_cycle.outcome == "tray_failed"  # type: ignore[union-attr]
    await loop3.stop()
    assert calls == []


@pytest.mark.asyncio
async def test_served_texts_without_gate_call_fields_gate_with_the_legacy_wording() -> None:
    client = _LoopClient([_tray([_card("c1", "m1")])])
    prompts: list[str] = []
    old = {k: v for k, v in texts_body().items() if not k.startswith("gate_call_")}
    loop = ReceiveLoop(
        client,
        Texts.model_validate(old),
        "gate_call",
        socket=None,
        quiet_ms=0,
        max_ms=0,
        call_model=lambda p: prompts.append(p) or _answer(("c1", "dismiss")),
        start_turn=_Turns(),
    )
    await loop.start()
    await _cycle(loop)
    assert prompts and prompts[0].startswith("You are triaging your AgentChat tray")
    assert client.decisions.recorded[0]["decision"] == "dismiss"
    await loop.stop()


@pytest.mark.asyncio
async def test_stop_tells_running_turns_and_an_outcome_after_stop_is_still_recorded() -> None:
    sent = _tray([_card("c1", "m1", you_owe_reply=False)])
    client = _LoopClient([_tray([_card("c1", "m1")]), sent])
    turns = _Turns()
    loop = _loop(client, call_model=lambda _p: _answer(("c1", "reply")), start_turn=turns)
    await loop.start()
    await _cycle(loop)
    assert not turns.requests[0].stopping.is_set()
    await loop.stop()
    assert turns.requests[0].stopping.is_set()
    turns.ends[0].set_result(TurnEnd(status="completed"))
    await _eventually(lambda: loop.status().turns == [], "the turn's end to be recorded")
    # The turn ended after stop(): what it did is still recorded.
    assert client.decisions.recorded == [
        {"conversation_id": "c1", "decision": "reply", "through_message_id": "m1"}
    ]


# ─────────────── deadlines ───────────────


@pytest.mark.asyncio
async def test_a_gate_call_past_its_deadline_fails_closed_and_records_nothing() -> None:
    client = _LoopClient([_tray([_card("c1", "m1")])])
    hang = asyncio.Event()

    async def call_model(_prompt: str) -> str:
        await hang.wait()
        return _answer(("c1", "dismiss"))

    loop = _loop(client, call_model=call_model, start_turn=_Turns(), gate_call_timeout_ms=30)
    await loop.start()
    await _cycle(loop)
    await asyncio.sleep(0.05)
    await loop.idle()
    summary = loop.status().last_cycle
    assert summary is not None and summary.outcome == "fail_closed"
    assert "timed out" in (summary.failure or "")
    assert client.decisions.recorded == []
    hang.set()
    await loop.stop()


@pytest.mark.asyncio
async def test_a_cycle_that_never_finishes_is_abandoned_and_wakes_are_taken_again() -> None:
    client = _LoopClient([_tray([_card("c1", "m1")])])
    stuck = asyncio.Event()
    checks = 0

    async def readiness() -> Readiness:
        nonlocal checks
        checks += 1
        if checks == 1:
            await stuck.wait()  # never answers
        return Readiness(ready=True)

    loop = _loop(
        client,
        call_model=lambda _p: _answer(("c1", "dismiss")),
        start_turn=_Turns(),
        readiness=readiness,
        cycle_timeout_ms=50,
    )
    await loop.start()
    loop.wake()
    await asyncio.sleep(0.15)
    await loop.idle()
    outcomes = [c.outcome for c in loop.status().recent_cycles]
    assert outcomes == ["abandoned"]
    await _cycle(loop)  # the coalescer takes wakes again
    assert [c.outcome for c in loop.status().recent_cycles] == ["abandoned", "decided"]
    assert client.decisions.recorded[0]["decision"] == "dismiss"
    await loop.stop()


@pytest.mark.asyncio
async def test_the_model_call_is_told_when_its_answer_is_no_longer_wanted() -> None:
    client = _LoopClient([_tray([_card("c1", "m1")])])
    seen: list[asyncio.Event] = []

    async def call_model(_prompt: str, *, cancel: asyncio.Event) -> str:
        seen.append(cancel)
        await cancel.wait()  # a well-behaved runtime stops when told
        return ""

    loop = _loop(client, call_model=call_model, start_turn=_Turns(), gate_call_timeout_ms=30)
    await loop.start()
    await _cycle(loop)
    await asyncio.sleep(0.05)
    assert len(seen) == 1 and seen[0].is_set()
    assert loop.status().last_cycle.outcome == "fail_closed"  # type: ignore[union-attr]
    await loop.stop()


@pytest.mark.asyncio
async def test_a_sync_model_call_past_its_deadline_is_simply_no_longer_waited_for() -> None:
    import threading

    client = _LoopClient([_tray([_card("c1", "m1")])])
    released = threading.Event()
    handles: list[threading.Event] = []

    def call_model(_prompt: str, cancel: threading.Event) -> str:
        handles.append(cancel)
        released.wait(2.0)
        return _answer(("c1", "dismiss"))

    loop = _loop(client, call_model=call_model, start_turn=_Turns(), gate_call_timeout_ms=30)
    await loop.start()
    await _cycle(loop)
    await loop.idle()
    assert loop.status().last_cycle.outcome == "fail_closed"  # type: ignore[union-attr]
    # The cycle stops waiting at the deadline, whether or not the worker
    # thread has started the call yet: wait for it to have.
    await _eventually(lambda: handles, "the model call to start in its worker thread")
    assert handles and handles[0].is_set()
    released.set()
    await asyncio.sleep(0.02)
    assert client.decisions.recorded == []  # the late answer is not acted on
    await loop.stop()


@pytest.mark.asyncio
async def test_an_abandoned_cycle_starts_no_turn_and_records_nothing_more() -> None:
    client = _LoopClient([_tray([_card("c1", "m1")])])
    turns = _Turns()
    release = asyncio.Event()

    async def call_model(_prompt: str) -> str:
        await release.wait()
        return _answer(("c1", "reply"))

    # The gate deadline off: the watchdog is then the call's only bound (a
    # call with a deadline of its own does not count against the cycle's).
    loop = _loop(
        client,
        call_model=call_model,
        start_turn=turns,
        cycle_timeout_ms=40,
        gate_call_timeout_ms=0,
    )
    await loop.start()
    loop.wake()
    await asyncio.sleep(0.15)
    await loop.idle()
    assert loop.status().last_cycle.outcome == "abandoned"  # type: ignore[union-attr]
    release.set()
    await asyncio.sleep(0.02)
    assert turns.requests == [] and client.decisions.recorded == []
    await loop.stop()


@pytest.mark.asyncio
async def test_time_spent_starting_a_turn_does_not_count_against_the_cycle() -> None:
    client = _LoopClient([_tray([_card("c1", "m1")])])

    async def slow_start(_request: TurnRequest) -> TurnAdmission:
        await asyncio.sleep(0.15)  # longer than the cycle deadline
        done: asyncio.Future[TurnEnd] = asyncio.get_running_loop().create_future()
        return TurnAdmission(started=True, done=done)

    loop = _loop(
        client, call_model=lambda _p: _answer(("c1", "reply")), start_turn=slow_start, cycle_timeout_ms=50
    )
    await loop.start()
    loop.wake()
    await asyncio.sleep(0.3)
    await loop.idle()
    summary = loop.status().last_cycle
    assert summary is not None and summary.outcome == "decided" and summary.turns_started == 1
    await loop.stop()


@pytest.mark.asyncio
async def test_an_admission_the_pump_started_does_not_hold_off_the_cycle_watchdog() -> None:
    # A freed slot is handed on by _pump, its own task, outside any cycle. The
    # time its admissions take is not the running cycle's: one that hangs
    # there must not keep the watchdog from abandoning a cycle that hangs.
    client = _LoopClient([_tray([_card("c1", "m1"), _card("c2", "m2")])])
    turns = _Turns()
    never = asyncio.Event()

    async def start_turn(request: TurnRequest) -> TurnAdmission:
        if request.conversation_id == "c2":
            await never.wait()  # the runtime never answers this admission
            return TurnAdmission(started=False, error="never answered")
        return await turns(request)

    checks = 0

    async def readiness() -> Readiness:
        nonlocal checks
        checks += 1
        if checks == 2:
            await never.wait()  # the second cycle never gets past its readiness check
        return Readiness(ready=True)

    loop = _loop(
        client,
        call_model=lambda _p: _answer(("c1", "reply"), ("c2", "reply")),
        start_turn=start_turn,
        readiness=readiness,
        cycle_timeout_ms=50,
    )
    await loop.start()
    await _cycle(loop)
    assert [r.conversation_id for r in turns.requests] == ["c1"]  # c2 waits for the slot
    turns.ends[0].set_result(TurnEnd(status="failed"))  # c1 ends: the pump admits c2, which hangs
    await _eventually(
        lambda: loop.status().counters["turns_waiting"] == 0 and loop.status().counters["turns_active"] == 1,
        "the pump to be admitting c2",
    )
    loop.wake()  # c1 is gated again, and this cycle hangs
    await _eventually(
        lambda: [c.outcome for c in loop.status().recent_cycles] == ["decided", "abandoned"],
        "the hung cycle to be abandoned",
        timeout=2.0,
    )
    assert checks == 2
    never.set()
    await loop.stop()


@pytest.mark.asyncio
async def test_status_keeps_the_recent_cycle_outcomes() -> None:
    client = _LoopClient([_tray([])])
    loop = _loop(client, call_model=lambda _p: "", start_turn=_Turns())
    await loop.start()
    for _ in range(3):
        await _cycle(loop)
    assert [c.outcome for c in loop.status().recent_cycles] == ["empty", "empty", "empty"]
    await loop.stop()


# ─────────────── a truncated tray never spins ───────────────


class _Timers:
    def __init__(self) -> None:
        self.delays: list[float] = []
        self.fns: list[Any] = []

    def __call__(self, delay_ms: float, fn: Any) -> Any:
        if delay_ms > 0:  # the coalescer's own 0 ms windows run for real
            self.delays.append(delay_ms)
            self.fns.append(fn)

            class _Handle:
                def cancel(self) -> None:
                    return None

            return _Handle()
        return asyncio.get_running_loop().call_later(0, fn)


@pytest.mark.asyncio
async def test_a_truncated_tray_without_progress_reads_again_after_a_growing_backoff() -> None:
    truncated = _tray([_card("c1", "m1")], truncated=True)
    client = _LoopClient([truncated])
    timers = _Timers()
    # The gate decides reply, but every turn is refused: no verdict recorded,
    # no turn started — no progress.

    async def refuse(_request: TurnRequest) -> TurnAdmission:
        return TurnAdmission(started=False, error="runtime busy")

    # The deadlines off: they run on set_timer too, and are not under test here.
    loop = _loop(
        client,
        call_model=lambda _p: _answer(("c1", "reply")),
        start_turn=refuse,
        set_timer=timers,
        gate_call_timeout_ms=0,
        cycle_timeout_ms=0,
    )
    await loop.start()
    await _cycle(loop)
    assert timers.delays == [1000]
    timers.fns.pop(0)()  # the backoff fires: another cycle
    await asyncio.sleep(0.02)
    await loop.idle()
    assert timers.delays == [1000, 2000]
    await loop.stop()


@pytest.mark.asyncio
async def test_a_truncated_tray_after_progress_is_read_again_at_once() -> None:
    truncated = _tray([_card("c1", "m1")], truncated=True)
    client = _LoopClient([truncated, _tray([])])
    timers = _Timers()
    loop = _loop(
        client,
        call_model=lambda _p: _answer(("c1", "dismiss")),
        start_turn=_Turns(),
        set_timer=timers,
        gate_call_timeout_ms=0,
        cycle_timeout_ms=0,
    )
    await loop.start()
    await _cycle(loop)
    await asyncio.sleep(0.02)
    await loop.idle()
    assert timers.delays == [] and client.inbox.reads >= 2
    await loop.stop()


@pytest.mark.asyncio
async def test_a_turn_released_while_its_outcome_is_read_records_nothing() -> None:
    sent = _tray([_card("c1", "m1", you_owe_reply=False)])
    client = _LoopClient([_tray([_card("c1", "m1")]), sent])
    turns = _Turns()
    loop = _loop(client, call_model=lambda _p: _answer(("c1", "reply")), start_turn=turns)
    await loop.start()
    await _cycle(loop)
    original = client.inbox.tray

    async def read_then_release(**kwargs: Any) -> Any:
        tray = await original(**kwargs)
        loop.release_turn("conversation:c1")  # an operator lets go meanwhile
        return tray

    client.inbox.tray = read_then_release  # type: ignore[method-assign]
    turns.ends[0].set_result(TurnEnd(status="completed"))
    await asyncio.sleep(0.02)
    assert client.decisions.recorded == []
    await loop.stop()


def test_status_marks_a_turn_running_past_thirty_minutes_overdue() -> None:
    clock = {"now": 0.0}
    loop = _loop(_LoopClient([_tray([])]), call_model=lambda _p: "", start_turn=_Turns(), now=lambda: clock["now"])
    loop._tracker.begin(TrackedTurn.reply("c1"))
    assert [t.overdue for t in loop.status().turns] == [False]
    clock["now"] = 31 * 60_000
    assert [t.overdue for t in loop.status().turns] == [True]


# ─────────────── max_concurrent_turns ───────────────


@pytest.mark.asyncio
async def test_at_most_max_concurrent_turns_run_and_the_rest_wait_for_a_slot() -> None:
    tray = _tray([_card("c1", "m1"), _card("c2", "m2"), _card("c3", "m3")])
    client = _LoopClient([tray])
    turns = _Turns()
    calls: list[str] = []

    def call_model(prompt: str) -> str:
        calls.append(prompt)
        return _answer(("c1", "reply"), ("c2", "reply"), ("c3", "reply"))

    loop = _loop(client, call_model=call_model, start_turn=turns)  # default: 1 at a time
    await loop.start()
    await _cycle(loop)
    assert [r.conversation_id for r in turns.requests] == ["c1"]
    summary = loop.status().last_cycle
    assert summary is not None and summary.turns_started == 1 and summary.turns_queued == 2
    assert loop.status().counters["turns_waiting"] == 2
    await _cycle(loop)  # waiting turns are held out of the gate: no second model call
    assert len(calls) == 1
    turns.ends[0].set_result(TurnEnd(status="failed"))
    await _eventually(lambda: len(turns.requests) == 2, "the second turn to start")
    assert [r.conversation_id for r in turns.requests] == ["c1", "c2"]
    turns.ends[1].set_result(TurnEnd(status="failed"))
    await _eventually(lambda: len(turns.requests) == 3, "the third turn to start")
    assert [r.conversation_id for r in turns.requests] == ["c1", "c2", "c3"]
    await loop.stop()


@pytest.mark.asyncio
async def test_a_turn_gated_while_the_freed_slot_is_handed_on_queues_behind_the_waiting_ones() -> None:
    # The freed slot is handed on by _pump, its own task. A cycle that runs
    # first (here: the one c1's failed turn wakes, which gates c1 again) must
    # not take the slot ahead of c2, which was waiting for it.
    tray = _tray([_card("c1", "m1"), _card("c2", "m2")])
    client = _LoopClient([tray])
    turns = _Turns()
    loop = _loop(client, call_model=lambda _p: _answer(("c1", "reply"), ("c2", "reply")), start_turn=turns)
    hand_on = asyncio.Event()
    pump = loop._pump

    async def held_pump() -> None:  # the window, held open
        await hand_on.wait()
        await pump()

    loop._pump = held_pump  # type: ignore[method-assign]
    await loop.start()
    await _cycle(loop)
    assert [r.conversation_id for r in turns.requests] == ["c1"]
    assert loop.status().counters["turns_waiting"] == 1
    await _cycle(loop)  # cards held out while c1 runs: its end wakes a cycle
    cycles = loop.status().coalescer["cycles"]
    turns.ends[0].set_result(TurnEnd(status="failed"))
    await _eventually(lambda: loop.status().coalescer["cycles"] > cycles, "c1's end to wake a cycle")
    await loop.idle()
    assert [r.conversation_id for r in turns.requests] == ["c1"]  # c1 again waits behind c2
    assert loop.status().counters["turns_waiting"] == 2
    hand_on.set()
    await _eventually(lambda: len(turns.requests) == 2, "c2 to get the slot")
    assert [r.conversation_id for r in turns.requests] == ["c1", "c2"]
    await loop.stop()


@pytest.mark.asyncio
async def test_releasing_a_turn_that_never_ends_frees_its_slot() -> None:
    tray = _tray([_card("c1", "m1"), _card("c2", "m2")])
    client = _LoopClient([tray])
    turns = _Turns()  # its turns end only when the test says so: never, for c1
    loop = _loop(
        client,
        call_model=lambda _p: _answer(("c1", "reply"), ("c2", "reply")),
        start_turn=turns,
    )
    await loop.start()
    await _cycle(loop)
    assert [r.conversation_id for r in turns.requests] == ["c1"]
    assert loop.release_turn("conversation:c1") is True
    await _eventually(lambda: len(turns.requests) == 2, "c2 to get the slot")
    assert [r.conversation_id for r in turns.requests] == ["c1", "c2"]  # c2 got the slot
    turns.ends[0].set_result(TurnEnd(status="completed"))  # a late end records nothing
    await asyncio.sleep(0.02)
    assert client.decisions.recorded == []
    await loop.stop()


@pytest.mark.asyncio
async def test_a_waiting_turn_that_is_released_never_starts() -> None:
    tray = _tray([_card("c1", "m1"), _card("c2", "m2")])
    turns = _Turns()
    timers = _Timers()  # cycles run only when the test fires their timer
    loop = _loop(
        _LoopClient([tray]),
        call_model=lambda _p: _answer(("c1", "reply"), ("c2", "reply")),
        start_turn=turns,
        quiet_ms=5_000,
        max_ms=5_000,
        set_timer=timers,
        gate_call_timeout_ms=0,
        cycle_timeout_ms=0,
    )
    await loop.start()
    loop.wake()
    timers.fns.pop(0)()
    # The gate call runs in a worker thread: wait for the cycle's result.
    await _eventually(lambda: loop.status().counters["turns_waiting"] == 1, "c2 to wait for the slot")
    assert [r.conversation_id for r in turns.requests] == ["c1"]  # c2 waits for the slot
    assert loop.release_turn("conversation:c2") is True
    assert loop.status().counters["turns_waiting"] == 0  # its admission is dropped at once
    turns.ends[0].set_result(TurnEnd(status="failed"))
    await _eventually(lambda: loop.status().counters["turns_active"] == 0, "c1's slot to be freed")
    assert [r.conversation_id for r in turns.requests] == ["c1"]  # the released one never starts
    await loop.stop()


@pytest.mark.asyncio
async def test_two_slots_run_two_turns_at_once() -> None:
    tray = _tray([_card("c1", "m1"), _card("c2", "m2"), _card("c3", "m3")])
    turns = _Turns()
    loop = _loop(
        _LoopClient([tray]),
        call_model=lambda _p: _answer(("c1", "reply"), ("c2", "reply"), ("c3", "reply")),
        start_turn=turns,
        max_concurrent_turns=2,
    )
    await loop.start()
    await _cycle(loop)
    assert [r.conversation_id for r in turns.requests] == ["c1", "c2"]
    await loop.stop()
    with pytest.raises(ValueError):
        _loop(_LoopClient([tray]), call_model=lambda _p: "", start_turn=turns, max_concurrent_turns=0)


# ─────────────── D5 pacing ───────────────

PACING_CASES: list[dict[str, Any]] = [
    {
        "name": "a-silent-turn-is-not-gated-again-within-the-window",
        "second_tray_anchor": "m1",
        "advance_ms": 30_000,
        "unanswered_names_it": False,
        "gated_again": False,
    },
    {
        "name": "after-the-window-it-is-gated-again",
        "second_tray_anchor": "m1",
        "advance_ms": 60_000,
        "unanswered_names_it": False,
        "gated_again": True,
    },
    {
        "name": "a-newer-message-moves-the-anchor-and-it-is-gated-at-once",
        "second_tray_anchor": "m2",
        "advance_ms": 1_000,
        "unanswered_names_it": False,
        "gated_again": True,
    },
    {
        "name": "an-unanswered-wake-naming-it-lifts-the-pacing",
        "second_tray_anchor": "m1",
        "advance_ms": 1_000,
        "unanswered_names_it": True,
        "gated_again": True,
    },
]


@pytest.mark.parametrize("case", PACING_CASES, ids=[c["name"] for c in PACING_CASES])
@pytest.mark.asyncio
async def test_d5_pacing_after_a_silent_turn(case: dict[str, Any]) -> None:
    clock = {"now": 1_000_000.0}
    first = _tray([_card("c1", "m1")])
    second = _tray([_card("c1", case["second_tray_anchor"])])
    client = _LoopClient([first, first, second])  # gate, re-read after the turn, next cycle
    turns = _Turns()
    calls: list[str] = []
    loop = _loop(
        client,
        call_model=lambda p: calls.append(p) or _answer(("c1", "reply")),
        start_turn=turns,
        now=lambda: clock["now"],
    )
    await loop.start()
    await _cycle(loop)
    turns.ends[0].set_result(TurnEnd(status="completed"))  # looked, did nothing
    await _eventually(lambda: loop.status().turns == [], "the turn's end to be counted")
    assert loop.status().counters["outcomes"] == {"silent_first": 1}
    clock["now"] += case["advance_ms"]
    if case["unanswered_names_it"]:
        loop._on_wake_frame(
            {"type": "wake", "payload": {"reason": "unanswered", "conversation_ids": ["c1"], "step": 1, "at": "t"}}
        )
    await _cycle(loop)
    assert (len(calls) == 2) is case["gated_again"]
    if not case["gated_again"]:
        summary = loop.status().last_cycle
        assert summary is not None and summary.outcome == "nothing_owed" and summary.paced == 1
    await loop.stop()


@pytest.mark.asyncio
async def test_d5_pacing_after_an_invite_turn() -> None:
    clock = {"now": 1_000_000.0}
    invite = {"invite_id": "gri_1", "group": {"id": "g", "name": "n", "member_count": 1}, "invited_by": "@p", "at": "t"}
    client = _LoopClient([_tray([], [invite])])
    turns = _Turns()
    loop = _loop(client, call_model=lambda _p: "", start_turn=turns, now=lambda: clock["now"])
    await loop.start()
    await _cycle(loop)
    turns.ends[0].set_result(TurnEnd(status="completed"))
    await _eventually(lambda: loop.status().turns == [], "the invite turn's end to be counted")
    clock["now"] += 30_000
    await _cycle(loop)
    assert len(turns.requests) == 1  # no second invite turn within the window
    loop._on_wake_frame({"type": "wake", "payload": {"reason": "unanswered", "conversation_ids": [], "step": 2, "at": "t"}})
    await _cycle(loop)
    assert len(turns.requests) == 2  # an unanswered wake lifts it
    await loop.stop()


@pytest.mark.asyncio
async def test_a_shared_turn_tracker_holds_the_hosts_own_turns_out_of_the_gate() -> None:
    tracker = TurnTracker()
    assert tracker.begin(TrackedTurn.reply("c1", "m1")) is not None
    calls: list[str] = []
    loop = _loop(
        _LoopClient([_tray([_card("c1", "m1")])]),
        call_model=lambda p: calls.append(p) or "",
        start_turn=_Turns(),
        turn_tracker=tracker,
    )
    await loop.start()
    await _cycle(loop)
    assert calls == [] and loop.status().last_cycle.held_by_turns == 1  # type: ignore[union-attr]
    await loop.stop()


@pytest.mark.asyncio
async def test_the_wake_event_carries_the_wake_reason() -> None:
    events: list[dict[str, Any]] = []
    loop = _loop(_LoopClient([_tray([])]), call_model=lambda _p: "", start_turn=_Turns(), on_event=events.append)
    await loop.start()
    loop._on_wake_frame(
        {"type": "wake", "payload": {"reason": "unanswered", "conversation_ids": ["c1", "c2"], "step": 1, "at": "t"}}
    )
    wakes = [e for e in events if e["type"] == "wake"]
    assert wakes[-1]["reason"] == "unanswered" and wakes[-1]["conversation_ids"] == ["c1", "c2"]
    await loop.stop()


@pytest.mark.asyncio
async def test_single_run_starts_one_run_with_the_served_texts_and_records_nothing() -> None:
    body = texts_body()
    invite = {"invite_id": "ginv_1", "group": {"id": "g", "name": "n", "member_count": 1}, "invited_by": "@p", "at": "t"}
    runs: list[Any] = []

    async def start_run(request: Any) -> RunEnd:
        runs.append(request)
        return RunEnd(status="completed")

    # After a run that started with invites the loop reads the tray once more
    # (the single-run invite rule): here the run answered them.
    client = _LoopClient(
        [
            _tray([_card("c1", "m1")]),
            _tray([], [invite]),
            _tray([]),
            _tray([_card("c1", "m1")], [invite]),
            _tray([]),
            _tray([]),
        ]
    )
    loop = _loop(client, mode="single_run", start_run=start_run)
    await loop.start()
    for _ in range(4):
        await _cycle(loop)
    assert [r.prompt for r in runs] == [
        body["wake_instruction"],
        body["invite_prompt"],
        f"{body['wake_instruction']}\n\n{body['invite_prompt']}",
    ]
    assert loop.status().last_cycle.outcome == "empty"  # type: ignore[union-attr]
    assert client.decisions.recorded == [] and client.groups.dismissed == []
    await loop.stop()


@pytest.mark.asyncio
async def test_single_run_dismisses_an_invite_left_pending_after_two_completed_runs() -> None:
    invite = {"invite_id": "gri_1", "group": {"id": "g", "name": "n", "member_count": 1}, "invited_by": "@p", "at": "t"}
    ends = [RunEnd(status="failed"), RunEnd(status="completed"), RunEnd(status="completed")]
    client = _LoopClient([_tray([], [invite])])
    clock = [0.0]
    loop = _loop(client, mode="single_run", start_run=lambda _r: ends.pop(0), now=lambda: clock[0])
    await loop.start()
    for ended_at in (0.0, 0.0, 61_000.0):  # D5 pacing: the dismissing run a window later
        clock[0] = ended_at
        await _cycle(loop)
    assert client.groups.dismissed == ["gri_1"]
    assert client.decisions.recorded == []
    outcomes = loop.status().counters["outcomes"]
    assert outcomes == {"invite_turn_failed": 1, "invite_silent_first": 1, "invite_silent_again": 1}
    await loop.stop()


@pytest.mark.asyncio
async def test_single_run_with_dismiss_ignored_invites_off_never_dismisses_and_records_no_outcome() -> None:
    invite = {"invite_id": "gri_1", "group": {"id": "g", "name": "n", "member_count": 1}, "invited_by": "@p", "at": "t"}
    events: list[dict[str, Any]] = []
    runs: list[Any] = []

    def start_run(request: Any) -> RunEnd:
        runs.append(request)
        return RunEnd(status="completed")

    client = _LoopClient([_tray([], [invite])])
    loop = _loop(client, mode="single_run", start_run=start_run, dismiss_ignored_invites=False, on_event=events.append)
    await loop.start()
    for _ in range(3):
        await _cycle(loop)
    assert len(runs) == 3
    assert client.groups.dismissed == [] and client.decisions.recorded == []
    assert [e for e in events if e["type"] == "invite_outcome"] == []
    assert client.inbox.reads == 3  # no read after the runs
    await loop.stop()


@pytest.mark.asyncio
async def test_single_run_starts_even_when_the_tray_cannot_be_read() -> None:
    runs: list[Any] = []
    client = _LoopClient([OSError("down")])
    loop = _loop(client, mode="single_run", start_run=lambda r: runs.append(r) or RunEnd(status="failed", detail="x"))
    await loop.start()
    await _cycle(loop)
    assert len(runs) == 1 and runs[0].cards is None
    assert loop.status().counters["runs_failed"] == 1
    await loop.stop()


@pytest.mark.asyncio
async def test_the_socket_wakes_echoes_do_not_and_the_handle_is_learned() -> None:
    ws = MockWebSocket()

    async def connect(_url: str, **_kw: Any) -> MockWebSocket:
        return ws

    from agentchatme.integration import ReceiveLoopSocketOptions

    client = _LoopClient([_tray([])])
    events: list[dict[str, Any]] = []
    loop = _loop(
        client,
        mode="single_run",
        start_run=lambda _r: RunEnd(status="completed"),
        socket=ReceiveLoopSocketOptions(api_key="k", websocket_connect=connect),
        on_event=events.append,
    )
    await loop.start()
    await _flush()
    hello = json.loads(ws.sent[0])
    assert hello["capabilities"] == ["ack", "heartbeat", "rewake"]
    await ws.push({"type": "hello.ok", "capabilities": ["ack", "heartbeat", "rewake"]})
    await _eventually(lambda: loop.status().handle is not None, "the connect wake's tray read")
    await loop.idle()
    assert loop.status().handle == "nova"  # learned from the tray's you
    await ws.push({"type": "message.new", "payload": {"id": "m1", "conversation_id": "c1", "sender": "nova", "seq": 1}})
    await ws.push({"type": "message.new", "payload": {"id": "m2", "conversation_id": "c1", "sender": "peer", "seq": 2}})
    await ws.push({"type": "wake", "payload": {"reason": "unanswered", "conversation_ids": ["c1"], "step": 1, "at": "t"}})
    await _eventually(
        lambda: [json.loads(s).get("type") for s in ws.sent].count("ack") == 2
        and any(e.get("source") == "wake_frame" for e in events),
        "the three frames to be handled",
    )
    sources = [e["source"] for e in events if e["type"] == "wake"]
    assert sources[:1] == ["connect"] and "message" in sources and "wake_frame" in sources
    assert [e for e in events if e["type"] == "echo"]
    assert loop.status().socket["capabilities"] == ["ack", "heartbeat", "rewake"]  # type: ignore[index]
    acks = [json.loads(s)["message_id"] for s in ws.sent if json.loads(s).get("type") == "ack"]
    assert acks == ["m1", "m2"]  # acked on receipt
    await loop.stop()


# ─────────────── system notices: acked, never a wake (contract v1 section 4) ───────────────


def _notice(mid: str, cid: str, seq: int, **data: Any) -> dict[str, Any]:
    """A system notice as the socket carries it: a ``message.new`` whose
    message has ``type: "system"``."""
    return {
        "type": "message.new",
        "payload": {
            "id": mid,
            "conversation_id": cid,
            "sender": "@example-ops",
            "client_msg_id": f"sys_{mid}",
            "seq": seq,
            "type": "system",
            "content": {"data": {"schema_version": 1, **data}},
            "metadata": {},
            "created_at": "2026-11-12T09:00:00.000Z",
        },
    }


def _peer_message(mid: str, cid: str, seq: int) -> dict[str, Any]:
    return {
        "type": "message.new",
        "payload": {
            "id": mid,
            "conversation_id": cid,
            "sender": "@example-scout",
            "seq": seq,
            "type": "text",
            "content": {"text": "are you there?"},
        },
    }


class _SocketLoop:
    """A loop on a mock socket, past its connect cycle (an empty tray)."""

    def __init__(self, mode: str, quiet_ms: int) -> None:
        from agentchatme.integration import ReceiveLoopSocketOptions

        self.ws = MockWebSocket()
        self.client = _LoopClient([_tray([])])
        self.events: list[dict[str, Any]] = []
        self.prompts: list[str] = []
        self.runs: list[Any] = []
        ws = self.ws

        async def connect(_url: str, **_kw: Any) -> MockWebSocket:
            return ws

        def call_model(prompt: str) -> str:
            self.prompts.append(prompt)
            return _answer(("conv_direct0first01", "dismiss"))

        def start_run(request: Any) -> RunEnd:
            self.runs.append(request)
            return RunEnd(status="completed")

        modes: dict[str, Any] = (
            {"call_model": call_model, "start_turn": _Turns()} if mode == "gate_call" else {"start_run": start_run}
        )
        self.loop = _loop(
            self.client,
            mode=mode,
            socket=ReceiveLoopSocketOptions(api_key="k", websocket_connect=connect),
            on_event=self.events.append,
            quiet_ms=quiet_ms,
            max_ms=quiet_ms * 10,
            **modes,
        )

    async def start(self) -> None:
        await self.loop.start()
        await _flush()
        await self.ws.push({"type": "hello.ok", "capabilities": ["ack", "heartbeat", "rewake"]})
        await _eventually(lambda: self.loop.status().coalescer["cycles"] == 1, "the connect cycle")
        await self.loop.idle()

    def acks(self) -> list[str]:
        frames = [json.loads(raw) for raw in self.ws.sent]
        return [f["message_id"] for f in frames if f.get("type") == "ack"]

    def notices(self) -> list[dict[str, Any]]:
        return [e for e in self.events if e["type"] == "notice"]

    def wake_sources(self) -> list[str]:
        return [e["source"] for e in self.events if e["type"] == "wake"]


@pytest.mark.asyncio
async def test_a_group_deleted_notice_is_acked_on_receipt_and_starts_no_cycle_and_no_gate_call() -> None:
    s = _SocketLoop("gate_call", 0)
    await s.start()
    reads = s.client.inbox.reads
    await s.ws.push(_notice("msg_gone0deleted01d", "conv_group0gone0001", 7, event="group_deleted", actor_handle="example-ops"))
    await s.ws.push(
        {
            "type": "group.deleted",
            "payload": {
                "group_id": "conv_group0gone0001",
                "deleted_by_handle": "example-ops",
                "deleted_at": "2026-11-12T09:00:00.000Z",
            },
        }
    )
    await s.ws.push(_notice("msg_crew0joined0001", "conv_group0crew0001", 3, event="member_joined", agent_handle="example-quill"))
    await _eventually(lambda: len(s.notices()) == 2, "both notices handled")
    await asyncio.sleep(0.02)
    await s.loop.idle()
    # Delivered: this machine has them. Nothing else happened.
    assert s.acks() == ["msg_gone0deleted01d", "msg_crew0joined0001"]
    status = s.loop.status()
    assert status.coalescer["cycles"] == 1 and status.coalescer["wakes"] == 1
    assert s.client.inbox.reads == reads
    assert s.prompts == []
    assert s.wake_sources() == ["connect"]
    assert s.notices() == [
        {"type": "notice", "conversation_id": "conv_group0gone0001"},
        {"type": "notice", "conversation_id": "conv_group0crew0001"},
    ]
    assert status.counters["frames"] == 2 and status.counters["notices"] == 2 and status.counters["echoes"] == 0
    await s.loop.stop()


@pytest.mark.asyncio
async def test_a_real_message_in_the_same_burst_as_notices_still_starts_its_cycle() -> None:
    s = _SocketLoop("gate_call", 20)
    await s.start()
    s.client.inbox.trays = [_tray([_card("conv_direct0first01", "msg_nova0first00001a")])]
    # One burst inside the quiet window: a notice, a real message, a notice.
    await s.ws.push(_notice("msg_crew0joined0001", "conv_group0crew0001", 1, event="member_joined", agent_handle="example-quill"))
    await s.ws.push(_peer_message("msg_crew0hello00002", "conv_group0crew0001", 2))
    await s.ws.push(
        _notice("msg_crew0renamed003", "conv_group0crew0001", 3, event="name_changed", new_name="crew", actor_handle="example-ops")
    )
    await _eventually(lambda: len(s.notices()) == 2, "the burst handled")
    await _eventually(lambda: s.loop.status().coalescer["cycles"] == 2, "the message cycle")
    await s.loop.idle()
    status = s.loop.status()
    assert status.coalescer["cycles"] == 2 and status.coalescer["wakes"] == 2
    assert len(s.prompts) == 1
    assert s.client.decisions.recorded == [
        {"conversation_id": "conv_direct0first01", "decision": "dismiss", "through_message_id": "msg_nova0first00001a"}
    ]
    assert s.acks() == ["msg_crew0joined0001", "msg_crew0hello00002", "msg_crew0renamed003"]
    assert s.wake_sources() == ["connect", "message"]
    # The order inside the burst does not matter: a message after a notice wakes too.
    await s.ws.push(_notice("msg_crew0left000004", "conv_group0crew0001", 4, event="member_left", agent_handle="example-quill"))
    await s.ws.push(_peer_message("msg_crew0again00005", "conv_group0crew0001", 5))
    await _eventually(lambda: s.loop.status().coalescer["cycles"] == 3, "the second message cycle")
    assert s.loop.status().counters["notices"] == 3
    await s.loop.stop()


@pytest.mark.asyncio
async def test_single_run_a_notice_starts_no_run_and_a_real_message_does() -> None:
    s = _SocketLoop("single_run", 0)
    await s.start()
    assert s.runs == []  # the connect cycle found an empty tray
    s.client.inbox.trays = [_tray([_card("conv_direct0first01", "msg_nova0first00001a")])]
    await s.ws.push(_notice("msg_crew0joined0001", "conv_group0crew0001", 1, event="member_joined", agent_handle="example-quill"))
    await _eventually(lambda: len(s.notices()) == 1, "the notice handled")
    await asyncio.sleep(0.02)
    await s.loop.idle()
    assert s.loop.status().coalescer["cycles"] == 1 and s.loop.status().coalescer["wakes"] == 1
    assert s.runs == []
    await s.ws.push(_peer_message("msg_direct0next0001", "conv_direct0first01", 1))
    await _eventually(lambda: len(s.runs) == 1, "the run for a real message")
    assert s.acks() == ["msg_crew0joined0001", "msg_direct0next0001"]
    await s.loop.stop()
