"""PeerClient connection-drop behaviour (#3) and module details not in the wire tests."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from pathlib import Path

import pytest
import pytest_asyncio

from blemees_peer import client as client_module
from blemees_peer.addressing import mint_sid
from blemees_peer.client import PeerClient
from blemees_peer.mcp_server import McpServer, _OfflinePeerClient

# Short bound: before the fix these awaits hang forever.
PROMPT = 2.0


@pytest_asyncio.fixture
async def dying_daemon(tmp_socket_path: Path) -> AsyncIterator[asyncio.Event]:
    """A fake daemon that reads one request line, then drops the connection.

    Yields an event that is set once a connection was dropped.
    """
    dropped = asyncio.Event()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        with contextlib.suppress(Exception):
            await reader.readline()
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()
        dropped.set()

    server = await asyncio.start_unix_server(handle, path=str(tmp_socket_path))
    try:
        yield dropped
    finally:
        server.close()
        await server.wait_closed()


async def _wait_read_loop_done(client: PeerClient) -> None:
    assert client._read_task is not None
    await asyncio.wait_for(asyncio.shield(client._read_task), timeout=PROMPT)


async def test_in_flight_call_fails_when_daemon_drops(dying_daemon, tmp_socket_path) -> None:
    client = PeerClient(tmp_socket_path)
    await client.connect()
    try:
        with pytest.raises(ConnectionError):
            await asyncio.wait_for(client.send(to="home:/tmp/x", body="hi"), timeout=PROMPT)
        assert client._pending == {}
    finally:
        await client.close()


async def test_calls_and_notifications_fail_fast_after_daemon_drops(
    dying_daemon, tmp_socket_path
) -> None:
    client = PeerClient(tmp_socket_path)
    await client.connect()
    try:
        # Wake the fake daemon so it drops the connection, then wait for
        # the client to notice the EOF.
        with contextlib.suppress(ConnectionError):
            await asyncio.wait_for(client.list_peers(), timeout=PROMPT)
        await asyncio.wait_for(dying_daemon.wait(), timeout=PROMPT)
        await _wait_read_loop_done(client)

        with pytest.raises(ConnectionError):
            await asyncio.wait_for(client.send(to="home:/tmp/x", body="hi"), timeout=PROMPT)
        with pytest.raises(ConnectionError):
            await asyncio.wait_for(client._call("peer.list_topics", {}), timeout=PROMPT)
        assert client._pending == {}

        # Every caller raises, not only the first one.
        with pytest.raises(ConnectionError):
            await asyncio.wait_for(client.next_notification(), timeout=PROMPT)
        with pytest.raises(ConnectionError):
            await asyncio.wait_for(client.next_notification(), timeout=PROMPT)
        # The timeout path raises ConnectionError too, not TimeoutError.
        with pytest.raises(ConnectionError):
            await client.next_notification(timeout=PROMPT)
        assert client.drain_notifications() == []
        with pytest.raises(ConnectionError):
            await asyncio.wait_for(client.next_notification(), timeout=PROMPT)
    finally:
        await client.close()


async def test_call_fails_fast_when_write_fails(dying_daemon, tmp_socket_path, monkeypatch) -> None:
    client = PeerClient(tmp_socket_path)
    await client.connect()
    try:

        async def broken_drain() -> None:
            raise BrokenPipeError("broken pipe")

        assert client._writer is not None
        monkeypatch.setattr(client._writer, "drain", broken_drain)
        with pytest.raises(ConnectionError, match="broken pipe"):
            await asyncio.wait_for(client.list_peers(), timeout=PROMPT)
        assert client._pending == {}
    finally:
        await client.close()


async def test_next_notification_raises_when_disconnected_and_queue_empty(
    dying_daemon, tmp_socket_path
) -> None:
    client = PeerClient(tmp_socket_path)
    await client.connect()
    try:
        with contextlib.suppress(ConnectionError):
            await asyncio.wait_for(client.list_peers(), timeout=PROMPT)
        await _wait_read_loop_done(client)
        # Simulate a lost sentinel (py3.11 ``wait_for`` can drop a result).
        client._notifications.get_nowait()
        with pytest.raises(ConnectionError):
            await asyncio.wait_for(client.next_notification(), timeout=PROMPT)
    finally:
        await client.close()


async def test_blocked_notification_waiter_wakes_when_daemon_drops(
    dying_daemon, tmp_socket_path
) -> None:
    client = PeerClient(tmp_socket_path)
    await client.connect()
    try:
        waiters = [asyncio.create_task(client.next_notification()) for _ in range(2)]
        await asyncio.sleep(0)
        with contextlib.suppress(ConnectionError):
            await asyncio.wait_for(client.list_peers(), timeout=PROMPT)
        for waiter in waiters:
            with pytest.raises(ConnectionError):
                await asyncio.wait_for(waiter, timeout=PROMPT)
    finally:
        await client.close()


async def test_calls_fail_after_real_daemon_shutdown(client_factory, peerd_server) -> None:
    client = await client_factory(home="/tmp/peer-test-A")
    await peerd_server.shutdown()
    await _wait_read_loop_done(client)
    with pytest.raises(ConnectionError):
        await asyncio.wait_for(client.list_peers(), timeout=PROMPT)
    with pytest.raises(ConnectionError):
        await asyncio.wait_for(client.next_notification(), timeout=PROMPT)


async def test_mcp_tool_returns_error_after_daemon_drops(dying_daemon, tmp_socket_path) -> None:
    mcp = McpServer(tmp_socket_path, home="/tmp/peer-test-A", sid=mint_sid())
    await mcp.peer.connect()
    try:
        forwarder = asyncio.create_task(mcp._forward_notifications())
        call = {"name": "peer_send", "arguments": {"to": "home:/tmp/x", "body": "hi"}}
        for _ in range(2):
            result = await asyncio.wait_for(mcp._h_tools_call(call), timeout=PROMPT)
            assert result["isError"] is True
            assert "connection closed" in result["content"][0]["text"]
        # The forwarder sees the dropped connection and stops.
        await asyncio.wait_for(forwarder, timeout=PROMPT)
    finally:
        await mcp.peer.close()


async def test_mcp_peer_await_returns_error_after_daemon_drops(
    dying_daemon, tmp_socket_path
) -> None:
    mcp = McpServer(tmp_socket_path, home="/tmp/peer-test-A", sid=mint_sid())
    await mcp.peer.connect()
    try:
        forwarder = asyncio.create_task(mcp._forward_notifications())
        # Blocked before the drop, with no timeout.
        waiting = asyncio.create_task(mcp._h_tools_call({"name": "peer_await", "arguments": {}}))
        await asyncio.sleep(0)
        await mcp._h_tools_call({"name": "peer_list_peers", "arguments": {}})
        await asyncio.wait_for(forwarder, timeout=PROMPT)

        result = await asyncio.wait_for(waiting, timeout=PROMPT)
        assert result["isError"] is True
        assert "connection lost" in result["content"][0]["text"]

        # Called after the drop, with and without a timeout.
        for arguments in ({}, {"timeout": 30}):
            call = {"name": "peer_await", "arguments": arguments}
            result = await asyncio.wait_for(mcp._h_tools_call(call), timeout=PROMPT)
            assert result["isError"] is True
            assert "connection lost" in result["content"][0]["text"]
    finally:
        await mcp.peer.close()


async def test_mcp_peer_await_returns_error_when_daemon_offline(tmp_socket_path) -> None:
    mcp = McpServer(tmp_socket_path, home="/tmp/peer-test-A", sid=mint_sid())
    # What ``run()`` does when ``peer.connect()`` fails at startup.
    mcp.peer = _OfflinePeerClient("no socket")  # type: ignore[assignment]
    forwarder = asyncio.create_task(mcp._forward_notifications())
    await asyncio.wait_for(forwarder, timeout=PROMPT)

    result = await asyncio.wait_for(
        mcp._h_tools_call({"name": "peer_await", "arguments": {}}), timeout=PROMPT
    )
    assert result["isError"] is True
    assert "connection lost" in result["content"][0]["text"]

    # Other tools still report the startup failure.
    call = {"name": "peer_send", "arguments": {"to": "home:/tmp/x", "body": "hi"}}
    result = await asyncio.wait_for(mcp._h_tools_call(call), timeout=PROMPT)
    assert result["isError"] is True
    assert "blemees-peerd unavailable: no socket" in result["content"][0]["text"]


def test_logger_name_matches_daemon_loggers() -> None:
    # Same "blemees-peerd.<module>" scheme as router, connection, etc.
    assert client_module.log.name == "blemees-peerd.client"
