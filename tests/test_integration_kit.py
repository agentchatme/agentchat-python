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
    TurnRequest,
    TurnTracker,
    probe_hosted_tools,
    probe_hosted_tools_async,
    sync_manual,
)
from agentchatme.types import Texts

from ._texts import texts as texts_body
from .test_realtime import MockWebSocket, _settle

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
    await _settle()
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
    await _settle()
    assert not waiting.done()
    assert tracker.release("conversation:c1") is True
    assert await asyncio.wait_for(waiting, 1) is None
    assert tracker.release("conversation:c1") is False


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

    async def tray(self, *, wait_ms: int = 0, limit: int | None = None) -> Any:
        self.reads += 1
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


async def _cycle(loop: ReceiveLoop) -> None:
    loop.wake()
    for _ in range(3):
        await asyncio.sleep(0.01)
        await loop.settled()


def _loop(client: _LoopClient, **kwargs: Any) -> ReceiveLoop:
    kwargs.setdefault("socket", None)
    kwargs.setdefault("quiet_ms", 0)
    kwargs.setdefault("max_ms", 0)
    return ReceiveLoop(client, Texts.model_validate(texts_body()), kwargs.pop("mode", "gate_call"), **kwargs)


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
    await asyncio.sleep(0.02)
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
    next_end.set_result(TurnEnd(status="failed"))
    await asyncio.sleep(0.05)
    await loop.settled()
    # The turn ended: the held card is read again (and gated again).
    assert client.inbox.reads > reads
    await loop.stop()


@pytest.mark.asyncio
async def test_looked_and_did_nothing_is_dismissed_the_second_time_for_the_same_anchor() -> None:
    tray = _tray([_card("c1", "m1")])
    client = _LoopClient([tray])
    turns = _Turns()
    loop = _loop(client, call_model=lambda _p: _answer(("c1", "reply")), start_turn=turns)
    await loop.start()
    for index in range(2):
        await _cycle(loop)
        turns.ends[index].set_result(TurnEnd(status="completed"))
        await asyncio.sleep(0.02)
    assert client.decisions.recorded == [
        {"conversation_id": "c1", "decision": "dismiss", "category": "no_action_needed", "through_message_id": "m1"}
    ]
    assert loop.status().counters["outcomes"] == {"silent_first": 1, "silent_again": 1}
    await loop.stop()


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
    await asyncio.sleep(0.05)
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
    loop = _loop(client, call_model=lambda p: calls.append(p) or "", start_turn=turns)
    await loop.start()
    for index in range(2):
        await _cycle(loop)
        assert turns.requests[index].kind == "invite"
        assert turns.requests[index].prompt.endswith("invite_id: ginv_1")
        turns.ends[index].set_result(TurnEnd(status="completed"))
        await asyncio.sleep(0.02)
    assert calls == []  # invites are not gated
    assert client.groups.dismissed == ["ginv_1"]
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
async def test_stop_tells_running_turns() -> None:
    client = _LoopClient([_tray([_card("c1", "m1")])])
    turns = _Turns()
    loop = _loop(client, call_model=lambda _p: _answer(("c1", "reply")), start_turn=turns)
    await loop.start()
    await _cycle(loop)
    assert not turns.requests[0].stopping.is_set()
    await loop.stop()
    assert turns.requests[0].stopping.is_set()
    turns.ends[0].set_result(TurnEnd(status="completed"))
    await asyncio.sleep(0.01)
    assert client.decisions.recorded == []  # nothing recorded once stopped


@pytest.mark.asyncio
async def test_single_run_starts_one_run_with_the_served_texts_and_records_nothing() -> None:
    body = texts_body()
    invite = {"invite_id": "ginv_1", "group": {"id": "g", "name": "n", "member_count": 1}, "invited_by": "@p", "at": "t"}
    runs: list[Any] = []

    async def start_run(request: Any) -> RunEnd:
        runs.append(request)
        return RunEnd(status="completed")

    client = _LoopClient([_tray([_card("c1", "m1")]), _tray([], [invite]), _tray([_card("c1", "m1")], [invite]), _tray([])])
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
    await _settle()
    hello = json.loads(ws.sent[0])
    assert hello["capabilities"] == ["ack", "heartbeat", "rewake"]
    await ws.push({"type": "hello.ok", "capabilities": ["ack", "heartbeat", "rewake"]})
    await asyncio.sleep(0.02)
    await loop.settled()
    assert loop.status().handle == "nova"  # learned from the tray's you
    await ws.push({"type": "message.new", "payload": {"id": "m1", "conversation_id": "c1", "sender": "nova", "seq": 1}})
    await ws.push({"type": "message.new", "payload": {"id": "m2", "conversation_id": "c1", "sender": "peer", "seq": 2}})
    await ws.push({"type": "wake", "payload": {"reason": "unanswered", "conversation_ids": ["c1"], "step": 1, "at": "t"}})
    await asyncio.sleep(0.02)
    sources = [e["source"] for e in events if e["type"] == "wake"]
    assert sources[:1] == ["connect"] and "message" in sources and "wake_frame" in sources
    assert [e for e in events if e["type"] == "echo"]
    assert loop.status().socket["capabilities"] == ["ack", "heartbeat", "rewake"]  # type: ignore[index]
    acks = [json.loads(s)["message_id"] for s in ws.sent if json.loads(s).get("type") == "ack"]
    assert acks == ["m1", "m2"]  # acked on receipt
    await loop.stop()
