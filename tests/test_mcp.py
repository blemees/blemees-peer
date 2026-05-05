"""End-to-end test of the stdio MCP sidecar via subprocess."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Any

import pytest

from blemees_peer.mcp_server import (
    resolve_alias_at_startup,
    resolve_home_at_startup,
    resolve_sid_at_startup,
)

# --- env var resolvers -----------------------------------------------------


def test_resolve_alias_returns_env(monkeypatch) -> None:
    monkeypatch.setenv("BLEMEES_AGENT_ALIAS", "architect")
    assert resolve_alias_at_startup() == "architect"


def test_resolve_alias_unset_returns_none(monkeypatch) -> None:
    monkeypatch.delenv("BLEMEES_AGENT_ALIAS", raising=False)
    assert resolve_alias_at_startup() is None


def test_resolve_alias_invalid_returns_none(monkeypatch) -> None:
    monkeypatch.setenv("BLEMEES_AGENT_ALIAS", "BAD-Alias")
    assert resolve_alias_at_startup() is None


def test_resolve_alias_reserved_prefix_returns_none(monkeypatch) -> None:
    monkeypatch.setenv("BLEMEES_AGENT_ALIAS", "sess_x")
    assert resolve_alias_at_startup() is None


def test_resolve_sid_returns_env(monkeypatch) -> None:
    monkeypatch.setenv("BLEMEES_AGENT_SID", "sess_AAAAAAAAAAAAAAAA")
    assert resolve_sid_at_startup() == "sess_AAAAAAAAAAAAAAAA"


def test_resolve_sid_invalid_mints_fresh(monkeypatch) -> None:
    monkeypatch.setenv("BLEMEES_AGENT_SID", "not-a-valid-sid")
    sid = resolve_sid_at_startup()
    assert sid.startswith("sess_") and sid != "not-a-valid-sid"


def test_resolve_home_returns_env(monkeypatch) -> None:
    monkeypatch.setenv("BLEMEES_AGENT_HOME", "~/foo")
    assert resolve_home_at_startup() == "~/foo"


# --- subprocess plumbing ---------------------------------------------------


async def _send(proc: asyncio.subprocess.Process, payload: dict[str, Any]) -> None:
    assert proc.stdin is not None
    proc.stdin.write((json.dumps(payload) + "\n").encode("utf-8"))
    await proc.stdin.drain()


async def _recv(proc: asyncio.subprocess.Process) -> dict[str, Any] | None:
    assert proc.stdout is not None
    line = await asyncio.wait_for(proc.stdout.readline(), timeout=10.0)
    if not line:
        return None
    return json.loads(line)


async def _recv_until_id(
    proc: asyncio.subprocess.Process,
    request_id: int,
) -> dict[str, Any]:
    """Read messages until we get the response for *request_id* (skip notifications)."""
    while True:
        msg = await _recv(proc)
        if msg is None:
            pytest.fail("MCP sidecar closed stdout unexpectedly")
        if msg.get("id") == request_id:
            return msg


@pytest.fixture
async def mcp_proc(peerd_server, tmp_socket_path: Path):
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "blemees_peer.mcp",
        "--socket",
        str(tmp_socket_path),
        "--home",
        "/tmp/peer-test-mcp",
        "--sid",
        "sess_MCP1MCP1MCP1MCP1",
        "--log-level",
        "warning",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        yield proc
    finally:
        if proc.returncode is None:
            try:
                if proc.stdin is not None:
                    proc.stdin.close()
            except Exception:
                pass
            try:
                await asyncio.wait_for(proc.wait(), timeout=3.0)
            except TimeoutError:
                proc.kill()
                await proc.wait()


async def test_initialize_and_tools_list(mcp_proc) -> None:
    await _send(mcp_proc, {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}})
    init = await _recv_until_id(mcp_proc, 1)
    assert init["result"]["protocolVersion"]
    assert init["result"]["serverInfo"]["name"] == "blemees-peer-mcp"
    caps = init["result"]["capabilities"]
    assert "tools" in caps
    assert "resources" in caps and caps["resources"]["subscribe"] is True

    await _send(mcp_proc, {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
    listing = await _recv_until_id(mcp_proc, 2)
    tool_names = {t["name"] for t in listing["result"]["tools"]}
    assert {
        "peer_send",
        "peer_publish",
        "peer_subscribe",
        "peer_inbox",
        "peer_await",
        "peer_set_alias",
        "peer_set_dm_filter",
        "peer_list_peers",
        "peer_list_topics",
        "peer_history",
    } <= tool_names


async def test_peer_send_via_tool_call(mcp_proc, peerd_server, tmp_socket_path) -> None:
    # Spin up a recipient on a separate connection so we can verify delivery.
    from blemees_peer.client import PeerClient

    recipient = PeerClient(tmp_socket_path)
    await recipient.connect()
    await recipient.hello("sess_RCPTRCPTRCPTRCPT", "/tmp/peer-test-rcpt")

    try:
        # initialize the MCP server first
        await _send(mcp_proc, {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}})
        await _recv_until_id(mcp_proc, 1)
        await _send(
            mcp_proc,
            {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
        )

        # call peer_send
        await _send(
            mcp_proc,
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "peer_send",
                    "arguments": {
                        "to": "home:/tmp/peer-test-rcpt",
                        "body": "hello-from-mcp",
                    },
                },
            },
        )
        resp = await _recv_until_id(mcp_proc, 2)
        assert resp["result"]["isError"] is False
        # Verify the recipient saw it
        note = await recipient.next_notification(timeout=2.0)
        # Skip peer.peer_joined if it arrived first
        for _ in range(3):
            if note["method"] == "peer.message":
                break
            note = await recipient.next_notification(timeout=2.0)
        assert note["method"] == "peer.message"
        assert note["params"]["body"] == "hello-from-mcp"
    finally:
        await recipient.close()


async def test_alias_claimed_from_env(peerd_server, tmp_socket_path) -> None:
    """The sidecar should auto-claim BLEMEES_AGENT_ALIAS after hello."""
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "blemees_peer.mcp",
        "--socket",
        str(tmp_socket_path),
        "--home",
        "/tmp/peer-test-mcp-alias",
        "--sid",
        "sess_MCPALIASMCPALIAS",
        "--log-level",
        "warning",
        env={
            **__import__("os").environ,
            "BLEMEES_AGENT_ALIAS": "architect",
        },
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        # Drive the MCP handshake so the sidecar finishes its startup
        await _send(proc, {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}})
        await _recv_until_id(proc, 1)
        await _send(
            proc,
            {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
        )

        # Verify via a separate peer that the sidecar's alias is claimed.
        from blemees_peer.client import PeerClient

        observer = PeerClient(tmp_socket_path)
        await observer.connect()
        try:
            await observer.hello("sess_OBSVOBSVOBSVOBSV", "/tmp/peer-test-observer")
            # Give blemees-peerd one tick to settle the auto-claim
            await asyncio.sleep(0.2)
            info = await observer.list_peers()
            sidecar = next(
                p for p in info["peers"] if p["peer_id"] == "home:/tmp/peer-test-mcp-alias"
            )
            assert sidecar["sessions"][0]["alias"] == "architect"
        finally:
            await observer.close()
    finally:
        if proc.returncode is None:
            if proc.stdin is not None:
                proc.stdin.close()
            try:
                await asyncio.wait_for(proc.wait(), timeout=3.0)
            except TimeoutError:
                proc.kill()
                await proc.wait()


async def test_resources_list_and_read(mcp_proc) -> None:
    await _send(mcp_proc, {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}})
    await _recv_until_id(mcp_proc, 1)
    await _send(
        mcp_proc,
        {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
    )
    await _send(
        mcp_proc,
        {"jsonrpc": "2.0", "id": 2, "method": "resources/list", "params": {}},
    )
    resp = await _recv_until_id(mcp_proc, 2)
    uris = {r["uri"] for r in resp["result"]["resources"]}
    assert {"peer://inbox", "peer://peers", "peer://topics"} <= uris

    await _send(
        mcp_proc,
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "resources/read",
            "params": {"uri": "peer://inbox"},
        },
    )
    resp = await _recv_until_id(mcp_proc, 3)
    contents = resp["result"]["contents"]
    assert contents[0]["uri"] == "peer://inbox"
    assert contents[0]["mimeType"] == "application/json"
