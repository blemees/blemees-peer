"""Direct tests of ``Router`` logic with a fake connection.

These run synchronously against an in-process Router, bypassing the
asyncio Unix server. They cover the algorithmic edges (alias
collisions, broadcast vs direct DMs, queue draining on hello, etc.)
without socket overhead. End-to-end wire tests live in
``test_server.py``.
"""

from __future__ import annotations

import fnmatch
from pathlib import Path
from typing import Any

import pytest

from blemees_peer.errors import ALIAS_TAKEN, INVALID_PARAMS, SID_IN_USE, PeerError
from blemees_peer.persistence import Persistence
from blemees_peer.router import Router


class FakeConnection:
    """Stand-in for ``Connection`` that records notifications in memory."""

    def __init__(self) -> None:
        self.sid: str | None = None
        self.home: str | None = None
        self.alias: str | None = None
        self.subscriptions: set[str] = set()
        self.dm_filter: list[str] = ["*"]
        self.wire_from_filter: list[str] | None = None
        self.wire_to_filter: list[str] = ["*"]
        self.notifications: list[tuple[str, dict[str, Any]]] = []

    @property
    def hello_complete(self) -> bool:
        return self.sid is not None

    @property
    def peer_id(self) -> str:
        return f"home:{self.home}"

    @property
    def from_addr(self) -> str:
        return f"home:{self.home}#{self.sid}"

    @property
    def is_watching(self) -> bool:
        return self.wire_from_filter is not None

    def matches_dm_filter(self, from_addr: str) -> bool:
        return any(fnmatch.fnmatchcase(from_addr, p) for p in self.dm_filter)

    def matches_wire_filter(self, from_addr: str, to_addr: str) -> bool:
        if not self.is_watching:
            return False
        from_patterns = self.wire_from_filter or []
        to_patterns = self.wire_to_filter or []
        from_ok = any(fnmatch.fnmatchcase(from_addr, p) for p in from_patterns)
        to_ok = any(fnmatch.fnmatchcase(to_addr, p) for p in to_patterns)
        return from_ok and to_ok

    async def send_notification(self, method: str, params: dict[str, Any]) -> None:
        self.notifications.append((method, params))


@pytest.fixture
def router(tmp_path: Path) -> Router:
    return Router(Persistence(tmp_path / "state"))


SID_A = "sess_AAAAAAAAAAAAAAAA"
SID_B = "sess_BBBBBBBBBBBBBBBB"
SID_C = "sess_CCCCCCCCCCCCCCCC"


# --- hello / goodbye --------------------------------------------------------


async def test_hello_registers_peer(router: Router) -> None:
    conn = FakeConnection()
    result = await router.hello(conn, SID_A, "/tmp/foo")
    assert result["peer_id"] == "home:/tmp/foo"
    assert conn.sid == SID_A
    assert conn.home == "/tmp/foo"
    assert SID_A in router.connections


async def test_hello_rejects_duplicate_sid(router: Router) -> None:
    a = FakeConnection()
    await router.hello(a, SID_A, "/tmp/foo")
    b = FakeConnection()
    with pytest.raises(PeerError) as ei:
        await router.hello(b, SID_A, "/tmp/bar")
    assert ei.value.code == SID_IN_USE


async def test_hello_emits_peer_joined_to_others(router: Router) -> None:
    a = FakeConnection()
    await router.hello(a, SID_A, "/tmp/foo")
    a.notifications.clear()
    b = FakeConnection()
    await router.hello(b, SID_B, "/tmp/bar")
    methods = [m for m, _ in a.notifications]
    assert "peer.peer_joined" in methods


async def test_goodbye_emits_peer_left(router: Router) -> None:
    a = FakeConnection()
    b = FakeConnection()
    await router.hello(a, SID_A, "/tmp/foo")
    await router.hello(b, SID_B, "/tmp/bar")
    a.notifications.clear()
    await router.goodbye(b)
    assert any(m == "peer.peer_left" for m, _ in a.notifications)
    assert SID_B not in router.connections


# --- aliases ----------------------------------------------------------------


async def test_set_alias_claims_and_announces(router: Router) -> None:
    a = FakeConnection()
    b = FakeConnection()
    await router.hello(a, SID_A, "/tmp/foo")
    await router.hello(b, SID_B, "/tmp/foo")
    a.notifications.clear()
    b.notifications.clear()
    await router.set_alias(a, "architect")
    assert a.alias == "architect"
    assert router.aliases_by_home["/tmp/foo"]["architect"] == SID_A
    # Both peers see the change
    assert any(m == "peer.alias_changed" for m, _ in a.notifications)
    assert any(m == "peer.alias_changed" for m, _ in b.notifications)


async def test_set_alias_collision_rejected(router: Router) -> None:
    a = FakeConnection()
    b = FakeConnection()
    await router.hello(a, SID_A, "/tmp/foo")
    await router.hello(b, SID_B, "/tmp/foo")
    await router.set_alias(a, "architect")
    with pytest.raises(PeerError) as ei:
        await router.set_alias(b, "architect")
    assert ei.value.code == ALIAS_TAKEN
    # b's alias unchanged
    assert b.alias is None


async def test_set_alias_in_different_homes_independent(router: Router) -> None:
    a = FakeConnection()
    b = FakeConnection()
    await router.hello(a, SID_A, "/tmp/foo")
    await router.hello(b, SID_B, "/tmp/bar")
    await router.set_alias(a, "architect")
    await router.set_alias(b, "architect")
    assert a.alias == "architect"
    assert b.alias == "architect"


async def test_alias_released_on_disconnect(router: Router) -> None:
    a = FakeConnection()
    await router.hello(a, SID_A, "/tmp/foo")
    await router.set_alias(a, "architect")
    await router.goodbye(a)
    # b can now claim it
    b = FakeConnection()
    await router.hello(b, SID_B, "/tmp/foo")
    res = await router.set_alias(b, "architect")
    assert res["alias"] == "architect"


async def test_set_alias_rejects_invalid_format(router: Router) -> None:
    a = FakeConnection()
    await router.hello(a, SID_A, "/tmp/foo")
    with pytest.raises(PeerError) as ei:
        await router.set_alias(a, "BAD-Alias")
    assert ei.value.code == INVALID_PARAMS


# --- DMs --------------------------------------------------------------------


async def test_send_direct_to_sid(router: Router) -> None:
    sender = FakeConnection()
    target = FakeConnection()
    await router.hello(sender, SID_A, "/tmp/foo")
    await router.hello(target, SID_B, "/tmp/bar")
    target.notifications.clear()
    res = await router.send(sender, f"home:/tmp/bar#{SID_B}", {"hi": "there"}, None)
    assert res["delivered"] == 1
    assert any(m == "peer.message" for m, _ in target.notifications)


async def test_send_broadcast_fans_out_in_home(router: Router) -> None:
    sender = FakeConnection()
    a = FakeConnection()
    b = FakeConnection()
    await router.hello(sender, SID_A, "/tmp/x")
    await router.hello(a, SID_B, "/tmp/y")
    await router.hello(b, SID_C, "/tmp/y")
    a.notifications.clear()
    b.notifications.clear()
    res = await router.send(sender, "home:/tmp/y", {"hello": "all"}, None)
    assert res["delivered"] == 2


async def test_send_to_alias_resolves(router: Router) -> None:
    sender = FakeConnection()
    target = FakeConnection()
    await router.hello(sender, SID_A, "/tmp/foo")
    await router.hello(target, SID_B, "/tmp/bar")
    await router.set_alias(target, "architect")
    target.notifications.clear()
    res = await router.send(sender, "home:/tmp/bar#architect", "hi", None)
    assert res["delivered"] == 1
    assert any(m == "peer.message" for m, _ in target.notifications)


async def test_send_with_no_recipient_queues(router: Router) -> None:
    sender = FakeConnection()
    await router.hello(sender, SID_A, "/tmp/foo")
    res = await router.send(sender, "home:/tmp/elsewhere", "drop", None)
    assert res["delivered"] == 0
    assert router.persistence.dms.has_messages("home:/tmp/elsewhere")


async def test_queued_dm_drained_on_hello(router: Router) -> None:
    sender = FakeConnection()
    await router.hello(sender, SID_A, "/tmp/foo")
    await router.send(sender, "home:/tmp/late", "hi-late", None)
    assert router.persistence.dms.has_messages("home:/tmp/late")
    # Late peer joins
    late = FakeConnection()
    await router.hello(late, SID_B, "/tmp/late")
    methods = [m for m, _ in late.notifications]
    assert methods.count("peer.message") == 1
    assert not router.persistence.dms.has_messages("home:/tmp/late")


async def test_queued_alias_dm_drained_on_set_alias(router: Router) -> None:
    sender = FakeConnection()
    target = FakeConnection()
    await router.hello(sender, SID_A, "/tmp/foo")
    await router.hello(target, SID_B, "/tmp/bar")
    res = await router.send(sender, "home:/tmp/bar#architect", "secret", None)
    assert res["delivered"] == 0
    target.notifications.clear()
    await router.set_alias(target, "architect")
    methods = [m for m, _ in target.notifications]
    assert "peer.message" in methods


async def test_dm_filter_blocks_inbound(router: Router) -> None:
    sender = FakeConnection()
    target = FakeConnection()
    await router.hello(sender, SID_A, "/tmp/foo")
    await router.hello(target, SID_B, "/tmp/bar")
    await router.set_dm_filter(target, ["home:/tmp/elsewhere*"])
    target.notifications.clear()
    res = await router.send(sender, f"home:/tmp/bar#{SID_B}", "blocked", None)
    # Sender's address starts with home:/tmp/foo — filter doesn't match
    assert res["delivered"] == 0
    assert not any(m == "peer.message" for m, _ in target.notifications)
    # And it gets queued because no one accepted it
    assert router.persistence.dms.has_messages(f"home:/tmp/bar#{SID_B}")


# --- topics -----------------------------------------------------------------


async def test_publish_fans_out_to_subscribers(router: Router) -> None:
    publisher = FakeConnection()
    subscriber = FakeConnection()
    await router.hello(publisher, SID_A, "/tmp/foo")
    await router.hello(subscriber, SID_B, "/tmp/bar")
    await router.subscribe(subscriber, "build", 0)
    subscriber.notifications.clear()
    res = await router.publish(publisher, "build", {"status": "green"})
    assert res["subscribers"] == 1
    methods = [m for m, _ in subscriber.notifications]
    assert "peer.message" in methods


async def test_publish_with_no_subscribers_still_persisted(router: Router) -> None:
    publisher = FakeConnection()
    await router.hello(publisher, SID_A, "/tmp/foo")
    res = await router.publish(publisher, "lonely", {"x": 1})
    assert res["subscribers"] == 0
    buf = router.persistence.topics.buffers.get("topic:lonely")
    assert buf is not None and len(buf) == 1


async def test_subscribe_with_replay_emits_history(router: Router) -> None:
    publisher = FakeConnection()
    await router.hello(publisher, SID_A, "/tmp/foo")
    for i in range(3):
        await router.publish(publisher, "build", {"i": i})
    subscriber = FakeConnection()
    await router.hello(subscriber, SID_B, "/tmp/bar")
    subscriber.notifications.clear()
    await router.subscribe(subscriber, "build", replay=2)
    methods = [m for m, _ in subscriber.notifications]
    assert methods.count("peer.message") == 2


async def test_unsubscribe_stops_delivery(router: Router) -> None:
    publisher = FakeConnection()
    subscriber = FakeConnection()
    await router.hello(publisher, SID_A, "/tmp/foo")
    await router.hello(subscriber, SID_B, "/tmp/bar")
    await router.subscribe(subscriber, "build", 0)
    await router.unsubscribe(subscriber, "build")
    subscriber.notifications.clear()
    await router.publish(publisher, "build", "ignored")
    assert not any(m == "peer.message" for m, _ in subscriber.notifications)


# --- discovery / history ---------------------------------------------------


async def test_list_peers_groups_by_home(router: Router) -> None:
    a = FakeConnection()
    b = FakeConnection()
    c = FakeConnection()
    await router.hello(a, SID_A, "/tmp/foo")
    await router.hello(b, SID_B, "/tmp/foo")
    await router.hello(c, SID_C, "/tmp/bar")
    await router.set_alias(a, "architect")
    res = await router.list_peers(a)
    by_home = {p["peer_id"]: p for p in res["peers"]}
    assert "home:/tmp/foo" in by_home
    assert "home:/tmp/bar" in by_home
    foo = by_home["home:/tmp/foo"]
    sids = {s["sid"] for s in foo["sessions"]}
    assert sids == {SID_A, SID_B}
    aliases = {s["alias"] for s in foo["sessions"]}
    assert "architect" in aliases


async def test_list_topics_includes_buffered(router: Router) -> None:
    publisher = FakeConnection()
    subscriber = FakeConnection()
    await router.hello(publisher, SID_A, "/tmp/foo")
    await router.hello(subscriber, SID_B, "/tmp/bar")
    await router.publish(publisher, "build", "x")
    await router.subscribe(subscriber, "build", 0)
    res = await router.list_topics(subscriber)
    names = {t["name"] for t in res["topics"]}
    assert "build" in names


async def test_history_for_topic(router: Router) -> None:
    publisher = FakeConnection()
    await router.hello(publisher, SID_A, "/tmp/foo")
    for i in range(3):
        await router.publish(publisher, "build", {"i": i})
    res = await router.history(publisher, "topic:build", since=None, limit=10)
    assert len(res["messages"]) == 3


# --- wire observation -----------------------------------------------------


async def test_watch_receives_dm(router: Router) -> None:
    sender = FakeConnection()
    target = FakeConnection()
    watcher = FakeConnection()
    await router.hello(sender, SID_A, "/tmp/x")
    await router.hello(target, SID_B, "/tmp/y")
    await router.hello(watcher, SID_C, "/tmp/observer")
    await router.watch(watcher, include=None, from_filter=None, to_filter=None)
    watcher.notifications.clear()
    target.notifications.clear()
    res = await router.send(sender, f"home:/tmp/y#{SID_B}", "hello", None)
    assert res["delivered"] == 1
    methods = [m for m, _ in watcher.notifications]
    assert methods.count("peer.wire_message") == 1
    wire = next(p for m, p in watcher.notifications if m == "peer.wire_message")
    assert wire["body"] == "hello"
    assert wire["delivered"] == 1
    assert wire["queued"] is False
    # Recipient still gets peer.message, not peer.wire_message
    target_methods = [m for m, _ in target.notifications]
    assert "peer.message" in target_methods
    assert "peer.wire_message" not in target_methods


async def test_watch_receives_topic_publish(router: Router) -> None:
    publisher = FakeConnection()
    subscriber = FakeConnection()
    watcher = FakeConnection()
    await router.hello(publisher, SID_A, "/tmp/x")
    await router.hello(subscriber, SID_B, "/tmp/y")
    await router.hello(watcher, SID_C, "/tmp/observer")
    await router.subscribe(subscriber, "build", 0)
    await router.watch(watcher, include=None, from_filter=None, to_filter=None)
    watcher.notifications.clear()
    res = await router.publish(publisher, "build", {"status": "green"})
    assert res["subscribers"] == 1
    wire = [p for m, p in watcher.notifications if m == "peer.wire_message"]
    assert len(wire) == 1
    assert wire[0]["to"] == "topic:build"
    assert wire[0]["delivered"] == 1
    assert wire[0]["queued"] is False


async def test_watch_does_not_echo_sender(router: Router) -> None:
    sender = FakeConnection()
    target = FakeConnection()
    await router.hello(sender, SID_A, "/tmp/x")
    await router.hello(target, SID_B, "/tmp/y")
    await router.watch(sender, include=None, from_filter=None, to_filter=None)
    sender.notifications.clear()
    await router.send(sender, f"home:/tmp/y#{SID_B}", "hi", None)
    methods = [m for m, _ in sender.notifications]
    assert "peer.wire_message" not in methods


async def test_watch_filter_from(router: Router) -> None:
    a = FakeConnection()
    b = FakeConnection()
    watcher = FakeConnection()
    await router.hello(a, SID_A, "/tmp/x")
    await router.hello(b, SID_B, "/tmp/y")
    await router.hello(watcher, SID_C, "/tmp/observer")
    # Only observe messages whose from is from /tmp/x
    await router.watch(watcher, include=None, from_filter=["home:/tmp/x*"], to_filter=None)
    watcher.notifications.clear()
    # b sends to a — should NOT be observed
    await router.send(b, f"home:/tmp/x#{SID_A}", "from-b", None)
    # a sends to b — SHOULD be observed
    await router.send(a, f"home:/tmp/y#{SID_B}", "from-a", None)
    wire_bodies = [p["body"] for m, p in watcher.notifications if m == "peer.wire_message"]
    assert wire_bodies == ["from-a"]


async def test_watch_marks_queued_dm(router: Router) -> None:
    sender = FakeConnection()
    watcher = FakeConnection()
    await router.hello(sender, SID_A, "/tmp/x")
    await router.hello(watcher, SID_B, "/tmp/observer")
    await router.watch(watcher, include=None, from_filter=None, to_filter=None)
    watcher.notifications.clear()
    res = await router.send(sender, "home:/tmp/elsewhere", "drop", None)
    assert res["delivered"] == 0
    wire = next(p for m, p in watcher.notifications if m == "peer.wire_message")
    assert wire["delivered"] == 0
    assert wire["queued"] is True


async def test_unwatch_stops_emission(router: Router) -> None:
    sender = FakeConnection()
    target = FakeConnection()
    watcher = FakeConnection()
    await router.hello(sender, SID_A, "/tmp/x")
    await router.hello(target, SID_B, "/tmp/y")
    await router.hello(watcher, SID_C, "/tmp/observer")
    await router.watch(watcher, include=None, from_filter=None, to_filter=None)
    await router.unwatch(watcher)
    watcher.notifications.clear()
    await router.send(sender, f"home:/tmp/y#{SID_B}", "hi", None)
    methods = [m for m, _ in watcher.notifications]
    assert "peer.wire_message" not in methods


async def test_watch_rejects_unsupported_include(router: Router) -> None:
    watcher = FakeConnection()
    await router.hello(watcher, SID_A, "/tmp/observer")
    with pytest.raises(PeerError) as ei:
        await router.watch(watcher, include=["presence"], from_filter=None, to_filter=None)
    assert ei.value.code == INVALID_PARAMS
