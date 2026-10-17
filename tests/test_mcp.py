"""End-to-end test of the stdio MCP sidecar via subprocess."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

import pytest

from blemees_peer.addressing import normalize_path
from blemees_peer.client import RemoteError
from blemees_peer.errors import SID_IN_USE
from blemees_peer.framing import MAX_LINE_BYTES
from blemees_peer.mcp_server import (
    McpServer,
    _OfflinePeerClient,
    _tool_await,
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


def test_resolve_home_invalid_warns_and_uses_cwd(monkeypatch, caplog) -> None:
    monkeypatch.setenv("BLEMEES_AGENT_HOME", "relative/../path")
    with caplog.at_level(logging.WARNING, logger="blemees-peer-mcp"):
        home = resolve_home_at_startup()
    assert home == normalize_path(os.getcwd())
    assert "ignoring BLEMEES_AGENT_HOME='relative/../path'" in caplog.text


def test_resolve_sid_invalid_warns(monkeypatch, caplog) -> None:
    monkeypatch.setenv("BLEMEES_AGENT_SID", "not-a-valid-sid")
    with caplog.at_level(logging.WARNING, logger="blemees-peer-mcp"):
        resolve_sid_at_startup()
    assert "ignoring BLEMEES_AGENT_SID='not-a-valid-sid'" in caplog.text


@pytest.mark.parametrize(
    ("home", "sid"),
    [(None, None), ("", ""), ("~/foo", "sess_AAAAAAAAAAAAAAAA")],
)
def test_resolve_home_and_sid_quiet_when_unset_or_valid(monkeypatch, caplog, home, sid) -> None:
    for name, value in (("BLEMEES_AGENT_HOME", home), ("BLEMEES_AGENT_SID", sid)):
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)
    with caplog.at_level(logging.WARNING, logger="blemees-peer-mcp"):
        resolve_home_at_startup()
        resolve_sid_at_startup()
    assert caplog.records == []


# --- peer_await ------------------------------------------------------------


def _offline_server() -> McpServer:
    # Never connects; the tests below only drive the inbox directly.
    return McpServer(Path("/nonexistent/peerd.sock"), "/tmp/peer-await", "sess_AWAITAWAITAWAITA")


def _deliver(s: McpServer, body: str) -> None:
    s.inbox.append({"method": "peer.message", "params": {"body": body}})
    s._inbox_event.set()


async def test_peer_await_keeps_waiting_when_message_taken() -> None:
    """Two concurrent waiters, one message: the waiter that wakes to an
    empty inbox must keep waiting, not report a timeout it never had.
    """
    s = _offline_server()
    first = asyncio.create_task(_tool_await(s, {}))
    second = asyncio.create_task(_tool_await(s, {}))
    await asyncio.sleep(0.01)

    _deliver(s, "one")
    await asyncio.sleep(0.01)
    done = [t for t in (first, second) if t.done()]
    pending = [t for t in (first, second) if not t.done()]
    assert len(done) == 1 and len(pending) == 1
    assert done[0].result()["params"]["body"] == "one"

    _deliver(s, "two")
    result = await asyncio.wait_for(pending[0], timeout=1.0)
    assert result["params"]["body"] == "two"


async def test_peer_await_times_out_on_empty_inbox() -> None:
    s = _offline_server()
    assert await _tool_await(s, {"timeout": 0.05}) == {"timeout": True}


async def test_peer_await_timeout_is_total_not_per_wake() -> None:
    s = _offline_server()
    taker = asyncio.create_task(_tool_await(s, {}))
    waiter = asyncio.create_task(_tool_await(s, {"timeout": 0.2}))
    await asyncio.sleep(0.01)
    _deliver(s, "one")
    assert (await asyncio.wait_for(taker, timeout=1.0))["params"]["body"] == "one"
    # The timed waiter lost the race and keeps waiting until its deadline
    await asyncio.sleep(0.01)
    assert not waiter.done()
    assert await asyncio.wait_for(waiter, timeout=1.0) == {"timeout": True}


async def test_peer_await_started_before_forwarder_is_woken_when_offline() -> None:
    """A peer_await that starts before the forwarder runs must not miss the wake-up."""
    s = _offline_server()
    s.peer = _OfflinePeerClient("test")  # type: ignore[assignment]
    waiter = asyncio.create_task(_tool_await(s, {}))
    await asyncio.sleep(0.01)
    assert not waiter.done()
    await s._forward_notifications()
    with pytest.raises(RemoteError):
        await asyncio.wait_for(waiter, timeout=1.0)
    # A later call fails at once.
    with pytest.raises(RemoteError):
        await asyncio.wait_for(_tool_await(s, {}), timeout=1.0)


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
    # Mirror the production "no alias" state: BLEMEES_AGENT_ALIAS unset.
    # blemees-agentd only exports it when the caller asked for an alias
    # (see blemees-agent/blemees_agent/backends/__init__.py), so the
    # absence of the env var is the canonical "anonymous" identity.
    # We unset it here defensively because the developer running the
    # test suite may themselves be in a Claude session that exports
    # BLEMEES_AGENT_ALIAS, which would otherwise leak into the child.
    env = {k: v for k, v in __import__("os").environ.items() if k != "BLEMEES_AGENT_ALIAS"}
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
        env=env,
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
        "peer_whoami",
    } <= tool_names


async def test_initialize_instructions_includes_identity(mcp_proc) -> None:
    """The initialize response carries identity context in the 'instructions'
    field so the MCP host can splice the agent's own address into its
    system prompt — closing the gap where a session has no way to learn
    its own sid/alias from a tool call alone.
    """
    await _send(mcp_proc, {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}})
    init = await _recv_until_id(mcp_proc, 1)
    instructions = init["result"].get("instructions")
    assert isinstance(instructions, str) and instructions
    # Identity facts that must be reachable from the system prompt:
    assert "sess_MCP1MCP1MCP1MCP1" in instructions
    assert "home:/tmp/peer-test-mcp" in instructions
    # No alias was set for this fixture, so should mention that path.
    assert "no alias claimed" in instructions
    assert "peer_whoami" in instructions  # advertises the runtime re-check


async def test_initialize_instructions_includes_alias_when_claimed(
    peerd_server,
    tmp_socket_path: Path,
) -> None:
    """When BLEMEES_AGENT_ALIAS is honored at startup, the initialize
    'instructions' field should reflect the claimed alias, not the sid.
    """
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "blemees_peer.mcp",
        "--socket",
        str(tmp_socket_path),
        "--home",
        "/tmp/peer-test-instructions",
        "--sid",
        "sess_INSTRUCT00000000",
        "--log-level",
        "warning",
        env={
            **__import__("os").environ,
            "BLEMEES_AGENT_ALIAS": "planner",
        },
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        await _send(proc, {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}})
        init = await _recv_until_id(proc, 1)
        instructions = init["result"]["instructions"]
        assert "'planner'" in instructions
        assert "sess_INSTRUCT00000000" in instructions
        assert "home:/tmp/peer-test-instructions#planner" in instructions
    finally:
        if proc.returncode is None:
            if proc.stdin is not None:
                proc.stdin.close()
            try:
                await asyncio.wait_for(proc.wait(), timeout=3.0)
            except TimeoutError:
                proc.kill()
                await proc.wait()


async def test_peer_whoami_without_alias(mcp_proc) -> None:
    await _send(mcp_proc, {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}})
    await _recv_until_id(mcp_proc, 1)
    await _send(
        mcp_proc,
        {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
    )
    await _send(
        mcp_proc,
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "peer_whoami", "arguments": {}},
        },
    )
    resp = await _recv_until_id(mcp_proc, 2)
    assert resp["result"]["isError"] is False
    payload = json.loads(resp["result"]["content"][0]["text"])
    assert payload == {
        "peer_id": "home:/tmp/peer-test-mcp",
        "sid": "sess_MCP1MCP1MCP1MCP1",
        "alias": None,
        "address": "home:/tmp/peer-test-mcp#sess_MCP1MCP1MCP1MCP1",
    }


async def test_peer_whoami_tracks_set_alias(mcp_proc) -> None:
    """After peer_set_alias succeeds, peer_whoami must reflect the new
    alias and address — otherwise an agent that calls set_alias has no
    way to learn its post-claim address.
    """
    await _send(mcp_proc, {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}})
    await _recv_until_id(mcp_proc, 1)
    await _send(
        mcp_proc,
        {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
    )
    await _send(
        mcp_proc,
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "peer_set_alias",
                "arguments": {"alias": "courier"},
            },
        },
    )
    resp = await _recv_until_id(mcp_proc, 2)
    assert resp["result"]["isError"] is False

    await _send(
        mcp_proc,
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {"name": "peer_whoami", "arguments": {}},
        },
    )
    resp = await _recv_until_id(mcp_proc, 3)
    payload = json.loads(resp["result"]["content"][0]["text"])
    assert payload["alias"] == "courier"
    assert payload["address"] == "home:/tmp/peer-test-mcp#courier"

    # Clearing the alias should drop us back to the sid form.
    await _send(
        mcp_proc,
        {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {"name": "peer_set_alias", "arguments": {"alias": ""}},
        },
    )
    await _recv_until_id(mcp_proc, 4)
    await _send(
        mcp_proc,
        {
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {"name": "peer_whoami", "arguments": {}},
        },
    )
    resp = await _recv_until_id(mcp_proc, 5)
    payload = json.loads(resp["result"]["content"][0]["text"])
    assert payload["alias"] is None
    assert payload["address"].endswith("#sess_MCP1MCP1MCP1MCP1")


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


async def test_sid_in_use_retries_with_fresh_sid(peerd_server, tmp_socket_path) -> None:
    """A stale holder of BLEMEES_AGENT_SID must not leave the sidecar without a session."""
    from blemees_peer.addressing import is_valid_sid
    from blemees_peer.client import PeerClient

    taken = "sess_TAKENTAKENTAKEN1"
    holder = PeerClient(tmp_socket_path)
    await holder.connect()
    await holder.hello(taken, "/tmp/peer-test-holder")
    env = {k: v for k, v in os.environ.items() if k != "BLEMEES_AGENT_ALIAS"}
    env["BLEMEES_AGENT_SID"] = taken
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "blemees_peer.mcp",
        "--socket",
        str(tmp_socket_path),
        "--home",
        "/tmp/peer-test-mcp-retry",
        "--log-level",
        "warning",
        env=env,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        await _init(proc)
        me = await _call_tool(proc, 2, "peer_whoami", {})
        assert me["sid"] != taken and is_valid_sid(me["sid"])
        assert me["address"] == f"home:/tmp/peer-test-mcp-retry#{me['sid']}"
        res = await _call_tool(proc, 3, "peer_send", {"to": "home:/tmp/peer-test-holder"})
        assert res["delivered"] == 1
        note = await holder.next_notification(timeout=2.0)
        for _ in range(3):
            if note["method"] == "peer.message":
                break
            note = await holder.next_notification(timeout=2.0)
        assert note["params"]["from"] == f"home:/tmp/peer-test-mcp-retry#{me['sid']}"
    finally:
        await holder.close()
        if proc.returncode is None:
            if proc.stdin is not None:
                proc.stdin.close()
            try:
                await asyncio.wait_for(proc.wait(), timeout=3.0)
            except TimeoutError:
                proc.kill()
                await proc.wait()
    assert proc.stderr is not None
    stderr = (await proc.stderr.read()).decode()
    assert taken in stderr and me["sid"] in stderr, stderr


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


# --- line size limit (#2) ----------------------------------------------------


async def test_tool_call_larger_than_64k(mcp_proc, peerd_server, tmp_socket_path) -> None:
    from blemees_peer.client import PeerClient

    recipient = PeerClient(tmp_socket_path)
    await recipient.connect()
    await recipient.hello("sess_BIGRBIGRBIGRBIGR", "/tmp/peer-test-big-rcpt")
    body = "x" * (100 * 1024)
    try:
        await _send(mcp_proc, {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}})
        await _recv_until_id(mcp_proc, 1)
        await _send(
            mcp_proc,
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "peer_send",
                    "arguments": {"to": "home:/tmp/peer-test-big-rcpt", "body": body},
                },
            },
        )
        resp = await _recv_until_id(mcp_proc, 2)
        assert resp["result"]["isError"] is False
        for _ in range(4):
            note = await recipient.next_notification(timeout=5.0)
            if note["method"] == "peer.message":
                break
        assert note["method"] == "peer.message"
        assert note["params"]["body"] == body
    finally:
        await recipient.close()


async def test_oversize_stdin_line_gets_parse_error_and_serve_continues(mcp_proc) -> None:
    assert mcp_proc.stdin is not None
    mcp_proc.stdin.write(b"x" * (MAX_LINE_BYTES + 1024) + b"\n")
    await mcp_proc.stdin.drain()
    err = await _recv(mcp_proc)
    assert err is not None
    assert "id" in err and err["id"] is None, err
    assert err["error"]["code"] == -32700, err
    await _send(mcp_proc, {"jsonrpc": "2.0", "id": 7, "method": "ping", "params": {}})
    assert "result" in await _recv_until_id(mcp_proc, 7)


async def test_invalid_json_stdin_line_gets_parse_error_with_null_id(mcp_proc) -> None:
    assert mcp_proc.stdin is not None
    mcp_proc.stdin.write(b"{not json\n")
    await mcp_proc.stdin.drain()
    err = await _recv(mcp_proc)
    assert err is not None
    assert "id" in err and err["id"] is None, err
    assert err["error"]["code"] == -32700, err
    await _send(mcp_proc, {"jsonrpc": "2.0", "id": 8, "method": "ping", "params": {}})
    assert "result" in await _recv_until_id(mcp_proc, 8)


async def test_message_without_id_or_method_gets_invalid_request_with_null_id(
    mcp_proc,
) -> None:
    for msg in ({"jsonrpc": "2.0", "params": {}}, {"jsonrpc": "2.0", "method": 42}):
        await _send(mcp_proc, msg)
        err = await _recv(mcp_proc)
        assert err is not None
        assert "id" in err and err["id"] is None, err
        assert err["error"]["code"] == -32600, err
    # A valid notification still gets no reply: the next message is the
    # response to the ping.
    await _send(mcp_proc, {"jsonrpc": "2.0", "method": "notifications/initialized"})
    await _send(mcp_proc, {"jsonrpc": "2.0", "id": 9, "method": "ping", "params": {}})
    reply = await _recv(mcp_proc)
    assert reply is not None and reply.get("id") == 9 and "result" in reply, reply


# --- peer_inbox / peer_await over MCP --------------------------------------


async def _init(proc: asyncio.subprocess.Process) -> None:
    # The initialize response comes after the sidecar's peer.hello
    await _send(proc, {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}})
    await _recv_until_id(proc, 1)
    await _send(proc, {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}})


async def _call_tool(
    proc: asyncio.subprocess.Process,
    request_id: int,
    name: str,
    arguments: dict[str, Any],
) -> Any:
    await _send(
        proc,
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        },
    )
    resp = await _recv_until_id(proc, request_id)
    assert resp["result"]["isError"] is False
    return json.loads(resp["result"]["content"][0]["text"])


async def _dm_sidecar(tmp_socket_path: Path, body: str) -> Any:
    """Connect a second peer and DM the mcp_proc sidecar; return the client."""
    from blemees_peer.client import PeerClient

    sender = PeerClient(tmp_socket_path)
    await sender.connect()
    await sender.hello("sess_SENDSENDSENDSEND", "/tmp/peer-test-sender")
    res = await sender.send(to="home:/tmp/peer-test-mcp#sess_MCP1MCP1MCP1MCP1", body=body)
    assert res["delivered"] == 1
    return sender


async def test_peer_await_returns_message_via_tool_call(mcp_proc, tmp_socket_path) -> None:
    await _init(mcp_proc)
    sender = await _dm_sidecar(tmp_socket_path, "await-me")
    try:
        # The sender's peer.peer_joined can arrive first; skip it
        for request_id in range(2, 6):
            note = await _call_tool(mcp_proc, request_id, "peer_await", {"timeout": 5})
            if note.get("method") == "peer.message":
                break
        assert note["method"] == "peer.message"
        assert note["params"]["body"] == "await-me"
        assert note["params"]["from"] == "home:/tmp/peer-test-sender#sess_SENDSENDSENDSEND"
    finally:
        await sender.close()


async def test_peer_await_timeout_via_tool_call(mcp_proc) -> None:
    await _init(mcp_proc)
    result = await _call_tool(mcp_proc, 2, "peer_await", {"timeout": 0.1})
    assert result == {"timeout": True}


async def test_peer_inbox_returns_message_via_tool_call(mcp_proc, tmp_socket_path) -> None:
    await _init(mcp_proc)
    sender = await _dm_sidecar(tmp_socket_path, "inbox-me")
    try:
        messages: list[dict[str, Any]] = []
        for request_id in range(2, 52):
            out = await _call_tool(mcp_proc, request_id, "peer_inbox", {})
            messages += [n for n in out["messages"] if n["method"] == "peer.message"]
            if messages:
                break
            await asyncio.sleep(0.05)
        assert [n["params"]["body"] for n in messages] == ["inbox-me"]
        # Drained: a second call returns no messages
        out = await _call_tool(mcp_proc, 100, "peer_inbox", {})
        assert out == {"messages": []}
    finally:
        await sender.close()


# --- no usable blemees-peerd (#15, #19) --------------------------------------


async def _spawn_sidecar(socket_path: Path, home: str, *args: str) -> asyncio.subprocess.Process:
    env = {k: v for k, v in os.environ.items() if k != "BLEMEES_AGENT_ALIAS"}
    return await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "blemees_peer.mcp",
        "--socket",
        str(socket_path),
        "--home",
        home,
        "--log-level",
        "warning",
        *args,
        env=env,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )


async def _stop_sidecar(proc: asyncio.subprocess.Process) -> None:
    if proc.returncode is None:
        if proc.stdin is not None:
            proc.stdin.close()
        try:
            await asyncio.wait_for(proc.wait(), timeout=3.0)
        except TimeoutError:
            proc.kill()
            await proc.wait()


async def _call_tool_error(
    proc: asyncio.subprocess.Process,
    request_id: int,
    name: str,
    arguments: dict[str, Any],
) -> str:
    """Call a tool that must fail at once; return its error text."""
    await _send(
        proc,
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/call",
            "params": {"name": name, "arguments": arguments},
        },
    )
    try:
        resp = await asyncio.wait_for(_recv_until_id(proc, request_id), timeout=5.0)
    except TimeoutError:
        pytest.fail(f"{name} gave no response in 5s")
    assert resp["result"]["isError"] is True, resp
    return resp["result"]["content"][0]["text"]


@pytest.mark.parametrize("socket_kind", ["missing", "not_a_socket"])
async def test_peer_await_errors_when_peerd_absent(tmp_socket_path, socket_kind) -> None:
    """With no blemees-peerd at startup, peer_await without a timeout must fail, not hang."""
    if socket_kind == "not_a_socket":
        tmp_socket_path.write_text("")
    proc = await _spawn_sidecar(tmp_socket_path, "/tmp/peer-test-offline")
    try:
        # Do not wait for the initialize response: peer_await goes out
        # while the sidecar still starts up.
        await _send(proc, {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}})
        assert "blemees-peerd" in await _call_tool_error(proc, 2, "peer_await", {})
        text = await _call_tool_error(proc, 3, "peer_list_peers", {})
        assert "blemees-peerd unavailable" in text
    finally:
        await _stop_sidecar(proc)


@pytest.mark.parametrize("retry", ["connect_fails", "hello_rejected"])
async def test_failed_sid_in_use_retry_goes_offline(tmp_socket_path, retry) -> None:
    """If the hello retry after sid_in_use fails, the sidecar must be offline."""
    hellos: list[str] = []

    async def reject_hello(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        # Stand-in for blemees-peerd: reject every hello with sid_in_use.
        line = await reader.readline()
        if line:
            request = json.loads(line)
            hellos.append(request["params"]["sid"])
            if retry == "connect_fails":
                # Stop listening first, so the retry cannot connect.
                server.close()
                tmp_socket_path.unlink(missing_ok=True)
            error = {"code": SID_IN_USE, "message": "sid already in use"}
            response = {"jsonrpc": "2.0", "id": request["id"], "error": error}
            writer.write((json.dumps(response) + "\n").encode())
            await writer.drain()
        writer.close()

    server = await asyncio.start_unix_server(reject_hello, path=str(tmp_socket_path))
    proc = await _spawn_sidecar(tmp_socket_path, "/tmp/peer-test-retry-fails")
    try:
        await _init(proc)
        assert "blemees-peerd" in await _call_tool_error(proc, 2, "peer_await", {})
        text = await _call_tool_error(proc, 3, "peer_list_peers", {})
        assert "blemees-peerd unavailable" in text
    finally:
        await _stop_sidecar(proc)
        server.close()
        await server.wait_closed()
    assert len(hellos) == (1 if retry == "connect_fails" else 2)
