"""End-to-end tests via the real Unix socket and PeerClient."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging

import pytest

from blemees_peer.addressing import mint_sid
from blemees_peer.client import PeerClient, RemoteError
from blemees_peer.framing import MAX_BODY_BYTES, MAX_LINE_BYTES

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


@pytest.mark.parametrize("case", ["unnormalized_home", "sid_in_use"])
async def test_failed_hello_closes_connection(case, client_factory, tmp_socket_path) -> None:
    # SPEC §8: a failed hello sends the error, then closes the connection.
    sid = mint_sid()
    home = "/tmp/peer-test-hello-fail"
    if case == "sid_in_use":
        await client_factory(sid=sid)
        code = -32011
    else:
        home = "relative/path"
        code = -32602
    reader, writer = await asyncio.open_unix_connection(str(tmp_socket_path), limit=MAX_LINE_BYTES)
    try:
        req = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "peer.hello",
            "params": {"sid": sid, "home": home},
        }
        writer.write((json.dumps(req) + "\n").encode())
        await writer.drain()
        line = await asyncio.wait_for(reader.readuntil(b"\n"), timeout=5.0)
        msg = json.loads(line)
        assert msg["id"] == 1 and msg["error"]["code"] == code, msg
        assert await asyncio.wait_for(reader.read(), timeout=5.0) == b""
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


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


async def test_dm_history_is_undelivered_queue_only(client_factory) -> None:
    # SPEC §5/§7: DM history is the undelivered queue; delivered DMs are not kept.
    sender = await client_factory(home="/tmp/peer-test-A")
    await sender.send(to="home:/tmp/peer-test-hist", body="queued")
    res = await sender.history("home:/tmp/peer-test-hist")
    assert [m["body"] for m in res["messages"]] == ["queued"]
    recipient = await client_factory(home="/tmp/peer-test-hist")
    note = await recipient.next_notification(timeout=2.0)
    assert note["method"] == "peer.message" and note["params"]["body"] == "queued"
    res = await sender.history("home:/tmp/peer-test-hist")
    assert res["messages"] == []


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


# --- line size limit (#2) ----------------------------------------------------


async def test_dm_larger_than_64k_delivered(client_factory) -> None:
    a = await client_factory(home="/tmp/peer-test-A")
    b = await client_factory(home="/tmp/peer-test-B")
    body = "x" * (100 * 1024)
    res = await asyncio.wait_for(a.send(to="home:/tmp/peer-test-B", body=body), timeout=5.0)
    assert res["delivered"] == 1
    note = await b.next_notification(timeout=5.0)
    assert note["method"] == "peer.message"
    assert note["params"]["body"] == body


async def test_oversize_line_keeps_connection_open(peerd_server, tmp_socket_path) -> None:
    reader, writer = await asyncio.open_unix_connection(str(tmp_socket_path), limit=MAX_LINE_BYTES)

    async def call(rid: int, method: str, params: dict) -> dict:
        req = {"jsonrpc": "2.0", "id": rid, "method": method, "params": params}
        writer.write((json.dumps(req) + "\n").encode())
        await writer.drain()
        while True:
            line = await asyncio.wait_for(reader.readuntil(b"\n"), timeout=5.0)
            msg = json.loads(line)
            if msg.get("id") == rid:
                return msg

    try:
        hello = await call(1, "peer.hello", {"sid": mint_sid(), "home": "/tmp/peer-test-big"})
        assert "result" in hello
        writer.write(b"x" * (MAX_LINE_BYTES + 1024) + b"\n")
        await writer.drain()
        # Same connection still answers requests after the oversize line.
        resp = await call(2, "peer.list_peers", {})
        assert "result" in resp
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


async def test_history_too_large_for_one_line_returns_error(client_factory) -> None:
    a = await client_factory(home="/tmp/peer-test-A")
    body = "x" * (100 * 1024)
    for _ in range(45):  # ~4.5 MiB in total
        await a.publish("t", body)
    with pytest.raises(RemoteError) as exc:
        await asyncio.wait_for(a.history("topic:t", limit=100), timeout=5.0)
    assert exc.value.code == -32603
    # The connection still answers after the error.
    res = await asyncio.wait_for(a.history("topic:t", limit=1), timeout=5.0)
    assert len(res["messages"]) == 1


async def test_dm_body_too_large_rejected_not_delivered(client_factory, peerd_server) -> None:
    a = await client_factory(home="/tmp/peer-test-A")
    b = await client_factory(home="/tmp/peer-test-B")
    with pytest.raises(RemoteError) as exc:
        await asyncio.wait_for(
            a.send(to="home:/tmp/peer-test-B", body="x" * (MAX_LINE_BYTES - 200)), timeout=5.0
        )
    assert exc.value.code == -32602
    assert exc.value.data == {"max_body_bytes": MAX_BODY_BYTES}
    with pytest.raises(asyncio.TimeoutError):
        await b.next_notification(timeout=0.5)
    assert not peerd_server.persistence.dms.has_messages("home:/tmp/peer-test-B")

    # A body right at the limit is still delivered.
    body = "x" * (MAX_BODY_BYTES - 2)  # +2 for the JSON quotes
    res = await asyncio.wait_for(a.send(to="home:/tmp/peer-test-B", body=body), timeout=5.0)
    assert res["delivered"] == 1
    note = await b.next_notification(timeout=5.0)
    assert note["method"] == "peer.message"
    assert note["params"]["body"] == body


async def test_publish_body_too_large_rejected(client_factory) -> None:
    a = await client_factory(home="/tmp/peer-test-A")
    with pytest.raises(RemoteError) as exc:
        await asyncio.wait_for(a.publish("t", "x" * MAX_BODY_BYTES), timeout=5.0)
    assert exc.value.code == -32602
    res = await asyncio.wait_for(a.history("topic:t"), timeout=5.0)
    assert res["messages"] == []


async def test_request_too_large_raises_and_connection_still_usable(client_factory) -> None:
    a = await client_factory(home="/tmp/peer-test-A")
    with pytest.raises(RemoteError) as exc:
        await asyncio.wait_for(
            a.send(to="home:/tmp/peer-test-B", body="x" * (MAX_LINE_BYTES + 10)), timeout=5.0
        )
    assert exc.value.code == -32602
    res = await asyncio.wait_for(a.list_peers(), timeout=5.0)
    assert "peers" in res


async def test_unidentifiable_lines_get_errors_with_null_id(peerd_server, tmp_socket_path) -> None:
    reader, writer = await asyncio.open_unix_connection(str(tmp_socket_path), limit=MAX_LINE_BYTES)

    async def send_raw(data: bytes) -> dict:
        writer.write(data + b"\n")
        await writer.drain()
        line = await asyncio.wait_for(reader.readuntil(b"\n"), timeout=5.0)
        return json.loads(line)

    try:
        cases = [
            (b"{not json", -32700),
            (b"[1, 2, 3]", -32600),
            (b'"just a string"', -32600),
            (b"x" * (MAX_LINE_BYTES + 1024), -32700),
        ]
        for data, code in cases:
            msg = await send_raw(data)
            assert "id" in msg and msg["id"] is None, msg
            assert msg["error"]["code"] == code, msg
        # Same connection still answers a normal request afterwards.
        req = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "peer.hello",
            "params": {"sid": mint_sid(), "home": "/tmp/peer-test-null-id"},
        }
        msg = await send_raw(json.dumps(req).encode())
        assert msg["id"] == 1 and "result" in msg
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


async def test_failed_notification_gets_no_response(peerd_server, tmp_socket_path) -> None:
    reader, writer = await asyncio.open_unix_connection(str(tmp_socket_path), limit=MAX_LINE_BYTES)
    try:
        note = {"jsonrpc": "2.0", "method": "peer.does_not_exist", "params": {}}
        req = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "peer.hello",
            "params": {"sid": mint_sid(), "home": "/tmp/peer-test-note"},
        }
        writer.write((json.dumps(note) + "\n" + json.dumps(req) + "\n").encode())
        await writer.drain()
        line = await asyncio.wait_for(reader.readuntil(b"\n"), timeout=5.0)
        msg = json.loads(line)
        assert msg.get("id") == 1 and "result" in msg, msg
    finally:
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()


async def test_client_logs_error_with_null_id(client_factory, caplog) -> None:
    client = await client_factory()
    assert client._writer is not None
    with caplog.at_level(logging.WARNING, logger="blemees-peerd.client"):
        client._writer.write(b"{not json\n")
        await client._writer.drain()
        # A later call still resolves: the null-id error did not fail it.
        info = await client.list_peers()
    assert "peers" in info
    client_logs = [r for r in caplog.records if r.name == "blemees-peerd.client"]
    assert any("-32700" in r.getMessage() for r in client_logs)
