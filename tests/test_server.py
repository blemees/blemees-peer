"""End-to-end tests via the real Unix socket and PeerClient."""

from __future__ import annotations

import pytest

from blemees_peer.client import PeerClient, RemoteError

# --- hello / errors ---------------------------------------------------------


async def test_hello_round_trip(client_factory) -> None:
    client = await client_factory(home="/tmp/peer-test-A")
    info = await client.list_peers()
    assert any(p["peer_id"] == "home:/tmp/peer-test-A" for p in info["peers"])


async def test_unknown_method_returns_method_not_found(client_factory) -> None:
    client = await client_factory()
    with pytest.raises(RemoteError) as ei:
        await client._call("peer.does_not_exist", {})
    assert ei.value.code == -32601


async def test_methods_before_hello_rejected(peerd_server, tmp_socket_path) -> None:
    client = PeerClient(tmp_socket_path)
    await client.connect()
    try:
        with pytest.raises(RemoteError) as ei:
            await client.send(to="home:/tmp/foo", body="x")
        assert ei.value.code == -32000
    finally:
        await client.close()


async def test_hello_with_unnormalized_home_rejected(peerd_server, tmp_socket_path) -> None:
    client = PeerClient(tmp_socket_path)
    await client.connect()
    try:
        with pytest.raises(RemoteError) as ei:
            await client.hello("sess_AAAAAAAAAAAAAAAA", "relative/path")
        assert ei.value.code == -32602
    finally:
        await client.close()


# --- DMs --------------------------------------------------------------------


async def test_dm_direct_delivery(client_factory) -> None:
    a = await client_factory(home="/tmp/peer-test-A")
    b = await client_factory(home="/tmp/peer-test-B")
    res = await a.send(to="home:/tmp/peer-test-B", body="hello")
    assert res["delivered"] == 1
    note = await b.next_notification(timeout=2.0)
    assert note["method"] == "peer.message"
    assert note["params"]["body"] == "hello"


async def test_dm_to_offline_queued_then_drained(client_factory, tmp_socket_path) -> None:
    sender = await client_factory(home="/tmp/peer-test-A")
    res = await sender.send(to="home:/tmp/peer-test-late", body="for-later")
    assert res["delivered"] == 0
    # Now a peer joins that home
    late = PeerClient(tmp_socket_path)
    await late.connect()
    try:
        await late.hello("sess_LLLLLLLLLLLLLLLL", "/tmp/peer-test-late")
        note = await late.next_notification(timeout=2.0)
        # Skip peer.peer_joined notifications if any arrived first
        for _ in range(5):
            if note["method"] == "peer.message":
                break
            note = await late.next_notification(timeout=2.0)
        assert note["method"] == "peer.message"
        assert note["params"]["body"] == "for-later"
    finally:
        await late.close()


async def test_dm_alias_resolves(client_factory) -> None:
    sender = await client_factory(home="/tmp/peer-test-A")
    target = await client_factory(home="/tmp/peer-test-B", alias="architect")
    target.drain_notifications()
    res = await sender.send(to="home:/tmp/peer-test-B#architect", body="hi-arch")
    assert res["delivered"] == 1
    # Find the peer.message among any noise
    found = False
    for _ in range(5):
        note = await target.next_notification(timeout=2.0)
        if note["method"] == "peer.message":
            assert note["params"]["body"] == "hi-arch"
            found = True
            break
    assert found


async def test_dm_filter_blocks(client_factory) -> None:
    sender = await client_factory(home="/tmp/peer-test-A")
    target = await client_factory(home="/tmp/peer-test-B")
    await target.set_dm_filter(["home:/tmp/peer-test-elsewhere*"])
    target.drain_notifications()
    res = await sender.send(to="home:/tmp/peer-test-B", body="filtered")
    # delivered = 0 since the only recipient filtered the from-address out
    assert res["delivered"] == 0


# --- topics -----------------------------------------------------------------


async def test_topic_subscribe_publish(client_factory) -> None:
    a = await client_factory(home="/tmp/peer-test-A")
    b = await client_factory(home="/tmp/peer-test-B")
    await b.subscribe("builds")
    b.drain_notifications()
    res = await a.publish("builds", {"status": "green"})
    assert res["subscribers"] == 1
    note = await b.next_notification(timeout=2.0)
    assert note["method"] == "peer.message"
    assert note["params"]["to"] == "topic:builds"
    assert note["params"]["body"] == {"status": "green"}


async def test_topic_replay(client_factory) -> None:
    publisher = await client_factory(home="/tmp/peer-test-A")
    for i in range(3):
        await publisher.publish("history-topic", {"i": i})
    subscriber = await client_factory(home="/tmp/peer-test-B")
    subscriber.drain_notifications()
    await subscriber.subscribe("history-topic", replay=2)
    seen = []
    for _ in range(3):
        try:
            note = await subscriber.next_notification(timeout=1.0)
        except TimeoutError:
            break
        if note["method"] == "peer.message":
            seen.append(note["params"]["body"])
    assert seen == [{"i": 1}, {"i": 2}]


async def test_topic_publish_with_no_subscribers_persists(client_factory) -> None:
    publisher = await client_factory(home="/tmp/peer-test-A")
    await publisher.publish("orphan", "payload")
    res = await publisher.history("topic:orphan", limit=10)
    assert len(res["messages"]) == 1


# --- aliases & discovery ---------------------------------------------------


async def test_alias_uniqueness(client_factory) -> None:
    a = await client_factory(home="/tmp/peer-test-shared")
    b = await client_factory(home="/tmp/peer-test-shared")
    await a.set_alias("architect")
    with pytest.raises(RemoteError) as ei:
        await b.set_alias("architect")
    assert ei.value.code == -32010


async def test_alias_changed_notification(client_factory) -> None:
    a = await client_factory(home="/tmp/peer-test-A")
    b = await client_factory(home="/tmp/peer-test-B")
    a.drain_notifications()
    b.drain_notifications()
    await a.set_alias("architect")
    # Both should get peer.alias_changed
    seen_methods: list[str] = []
    for _ in range(3):
        try:
            note = await b.next_notification(timeout=1.0)
            seen_methods.append(note["method"])
        except TimeoutError:
            break
    assert "peer.alias_changed" in seen_methods


async def test_list_peers_reflects_state(client_factory) -> None:
    a = await client_factory(home="/tmp/peer-test-A", alias="alpha")
    await client_factory(home="/tmp/peer-test-B", alias="bravo")
    info = await a.list_peers()
    by_home = {p["peer_id"]: p for p in info["peers"]}
    assert by_home["home:/tmp/peer-test-A"]["sessions"][0]["alias"] == "alpha"
    assert by_home["home:/tmp/peer-test-B"]["sessions"][0]["alias"] == "bravo"


# --- wire observation -----------------------------------------------------


async def test_wire_observation_sees_dms_and_topics(client_factory) -> None:
    sender = await client_factory(home="/tmp/peer-test-sender")
    target = await client_factory(home="/tmp/peer-test-target")
    watcher = await client_factory(home="/tmp/peer-test-watcher")
    await target.subscribe("builds")
    res = await watcher.watch()
    assert res["watching"] is True
    watcher.drain_notifications()
    target.drain_notifications()

    # DM
    await sender.send(to="home:/tmp/peer-test-target", body="hi-dm")
    # Topic publish
    await sender.publish("builds", {"x": 1})

    wire_msgs: list[dict] = []
    for _ in range(8):
        try:
            note = await watcher.next_notification(timeout=2.0)
        except TimeoutError:
            break
        if note["method"] == "peer.wire_message":
            wire_msgs.append(note["params"])

    assert len(wire_msgs) == 2
    assert {m["to"] for m in wire_msgs} == {"home:/tmp/peer-test-target", "topic:builds"}
    delivered = {m["to"]: m["delivered"] for m in wire_msgs}
    assert delivered == {"home:/tmp/peer-test-target": 1, "topic:builds": 1}


async def test_wire_observation_unwatch(client_factory) -> None:
    sender = await client_factory(home="/tmp/peer-test-sender")
    await client_factory(home="/tmp/peer-test-target")
    watcher = await client_factory(home="/tmp/peer-test-watcher")
    await watcher.watch()
    await watcher.unwatch()
    watcher.drain_notifications()
    await sender.send(to="home:/tmp/peer-test-target", body="hi")
    # No wire_message should appear within a short window
    seen: list[str] = []
    for _ in range(3):
        try:
            note = await watcher.next_notification(timeout=0.5)
        except TimeoutError:
            break
        seen.append(note["method"])
    assert "peer.wire_message" not in seen
