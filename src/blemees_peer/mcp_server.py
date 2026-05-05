"""Stdio MCP sidecar — exposes blemees-peer/1 to agents as MCP tools + resources.

This is a minimal hand-rolled MCP server that runs on stdin/stdout.
It speaks JSON-RPC 2.0 with the MCP host (Claude Code, Codex, etc.)
and bridges every tool call to the ``blemees-peerd`` daemon via ``PeerClient``.

We avoid the official ``mcp`` SDK to keep the sidecar's dependency
surface zero — same ethos as the daemon — and because the protocol
surface we need is small.

Logging goes to stderr only; stdout is reserved for the MCP wire.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import sys
from collections import deque
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from blemees_peer._version import __version__
from blemees_peer.addressing import is_normalized_path, normalize_path
from blemees_peer.client import PeerClient, RemoteError

log = logging.getLogger("blemees-peer-mcp")

MCP_PROTOCOL_VERSION = "2024-11-05"
INBOX_MAXLEN = 1000


class StdioJsonRpc:
    """Minimal newline-delimited JSON-RPC 2.0 server on stdin/stdout."""

    def __init__(self) -> None:
        self.handlers: dict[str, Callable[[dict[str, Any]], Awaitable[Any]]] = {}
        self.notification_handlers: dict[str, Callable[[dict[str, Any]], Awaitable[None]]] = {}
        self._stdout_lock = asyncio.Lock()
        self._stdin_reader: asyncio.StreamReader | None = None
        self._inflight: set[asyncio.Task[None]] = set()

    def on_request(self, method: str, fn: Callable[[dict[str, Any]], Awaitable[Any]]) -> None:
        self.handlers[method] = fn

    def on_notification(
        self,
        method: str,
        fn: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        self.notification_handlers[method] = fn

    async def serve(self) -> None:
        loop = asyncio.get_running_loop()
        reader = asyncio.StreamReader()
        protocol = asyncio.StreamReaderProtocol(reader)
        await loop.connect_read_pipe(lambda: protocol, sys.stdin)
        self._stdin_reader = reader

        while True:
            try:
                line = await reader.readuntil(b"\n")
            except asyncio.IncompleteReadError:
                return
            text = line.decode("utf-8", errors="replace").strip()
            if not text:
                continue
            try:
                msg = json.loads(text)
            except json.JSONDecodeError as e:
                await self._send_error(None, -32700, f"parse error: {e.msg}")
                continue
            if not isinstance(msg, dict):
                await self._send_error(None, -32600, "request must be a JSON object")
                continue
            task = asyncio.create_task(self._handle_message(msg))
            self._inflight.add(task)
            task.add_done_callback(self._inflight.discard)

    async def _handle_message(self, msg: dict[str, Any]) -> None:
        method = msg.get("method")
        params = msg.get("params") or {}
        request_id = msg.get("id")

        if not isinstance(method, str):
            await self._send_error(request_id, -32600, "method must be a string")
            return
        if not isinstance(params, dict):
            await self._send_error(request_id, -32602, "params must be an object")
            return

        if request_id is None:
            handler = self.notification_handlers.get(method)
            if handler is None:
                log.debug("unhandled notification: %s", method)
                return
            try:
                await handler(params)
            except Exception:
                log.exception("notification handler %s crashed", method)
            return

        handler = self.handlers.get(method)
        if handler is None:
            await self._send_error(request_id, -32601, f"method not found: {method!r}")
            return
        try:
            result = await handler(params)
        except Exception as e:
            log.exception("request handler %s crashed", method)
            await self._send_error(request_id, -32603, f"internal error: {e}")
            return
        await self._send_response(request_id, result)

    async def _send_response(self, request_id: Any, result: Any) -> None:
        await self._write({"jsonrpc": "2.0", "id": request_id, "result": result})

    async def _send_error(
        self,
        request_id: Any,
        code: int,
        message: str,
        data: Any | None = None,
    ) -> None:
        if request_id is None:
            log.warning("dropping unidentifiable error: %s %s", code, message)
            return
        err: dict[str, Any] = {"code": code, "message": message}
        if data is not None:
            err["data"] = data
        await self._write({"jsonrpc": "2.0", "id": request_id, "error": err})

    async def send_notification(self, method: str, params: dict[str, Any]) -> None:
        await self._write({"jsonrpc": "2.0", "method": method, "params": params})

    async def _write(self, message: dict[str, Any]) -> None:
        line = json.dumps(message, separators=(",", ":"), ensure_ascii=False) + "\n"
        async with self._stdout_lock:
            sys.stdout.write(line)
            sys.stdout.flush()


class McpServer:
    """The blemees-peer MCP sidecar."""

    def __init__(
        self,
        socket_path: Path,
        home: str,
        sid: str,
        alias: str | None = None,
    ) -> None:
        self.socket_path = socket_path
        self.home = home
        self.sid = sid
        self.initial_alias = alias
        self.peer = PeerClient(socket_path)
        self.rpc = StdioJsonRpc()
        self.inbox: deque[dict[str, Any]] = deque(maxlen=INBOX_MAXLEN)
        self.subscribed_resources: set[str] = set()
        self._inbox_event = asyncio.Event()
        self._initialized = False
        self._register_handlers()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def run(self) -> int:
        """Run the sidecar until stdin closes or blemees-peerd disconnects."""
        try:
            await self.peer.connect()
        except (FileNotFoundError, ConnectionRefusedError) as e:
            log.error("could not connect to blemees-peerd at %s: %s", self.socket_path, e)
            # Keep MCP serving so the host gets a useful error rather than
            # a sidecar that crashes immediately. Tools will return errors.
            self.peer = _OfflinePeerClient(str(e))  # type: ignore[assignment]

        try:
            hello = await self.peer.hello(self.sid, self.home)
            log.info("hello ok: %s", hello)
        except (RemoteError, RuntimeError) as e:
            log.error("hello failed: %s", e)

        if self.initial_alias:
            try:
                await self.peer.set_alias(self.initial_alias)
                log.info("claimed alias %r at startup", self.initial_alias)
            except (RemoteError, RuntimeError) as e:
                # Most likely cause: another live session in the same
                # home already holds this alias. Don't crash; the
                # session is still addressable by sid and the agent
                # can reclaim the alias later via the peer_set_alias
                # tool when the conflicting session exits.
                log.warning("could not claim startup alias %r: %s", self.initial_alias, e)

        forwarder = asyncio.create_task(self._forward_notifications(), name="peer-forwarder")
        try:
            await self.rpc.serve()
        finally:
            forwarder.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await forwarder
            with contextlib.suppress(Exception):
                await self.peer.close()
        return 0

    async def _forward_notifications(self) -> None:
        """Pull notifications from the peer client and stash them in the inbox."""
        while True:
            try:
                note = await self.peer.next_notification()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("notification fetch failed")
                return
            method = note.get("method")
            params = note.get("params") or {}
            self.inbox.append({"method": method, "params": params})
            self._inbox_event.set()
            # Push resource updates if subscribed
            if method == "peer.message" and "peer://inbox" in self.subscribed_resources:
                await self.rpc.send_notification(
                    "notifications/resources/updated",
                    {"uri": "peer://inbox"},
                )
            if (
                method in ("peer.peer_joined", "peer.peer_left", "peer.alias_changed")
                and "peer://peers" in self.subscribed_resources
            ):
                await self.rpc.send_notification(
                    "notifications/resources/updated",
                    {"uri": "peer://peers"},
                )

    # ------------------------------------------------------------------
    # MCP method handlers
    # ------------------------------------------------------------------

    def _register_handlers(self) -> None:
        rpc = self.rpc
        rpc.on_request("initialize", self._h_initialize)
        rpc.on_notification("notifications/initialized", self._h_initialized)
        rpc.on_request("tools/list", self._h_tools_list)
        rpc.on_request("tools/call", self._h_tools_call)
        rpc.on_request("resources/list", self._h_resources_list)
        rpc.on_request("resources/read", self._h_resources_read)
        rpc.on_request("resources/subscribe", self._h_resources_subscribe)
        rpc.on_request("resources/unsubscribe", self._h_resources_unsubscribe)
        rpc.on_request("ping", self._h_ping)

    async def _h_initialize(self, params: dict[str, Any]) -> dict[str, Any]:
        self._initialized = True
        return {
            "protocolVersion": MCP_PROTOCOL_VERSION,
            "capabilities": {
                "tools": {"listChanged": False},
                "resources": {"subscribe": True, "listChanged": False},
            },
            "serverInfo": {
                "name": "blemees-peer-mcp",
                "version": __version__,
            },
        }

    async def _h_initialized(self, params: dict[str, Any]) -> None:
        log.info(
            "MCP initialized; peer identity sid=%s home=%s socket=%s",
            self.sid,
            self.home,
            self.socket_path,
        )

    async def _h_ping(self, params: dict[str, Any]) -> dict[str, Any]:
        return {}

    async def _h_tools_list(self, params: dict[str, Any]) -> dict[str, Any]:
        return {"tools": _TOOLS}

    async def _h_tools_call(self, params: dict[str, Any]) -> dict[str, Any]:
        name = params.get("name")
        arguments = params.get("arguments") or {}
        if not isinstance(name, str):
            return _tool_error("name must be a string")
        if not isinstance(arguments, dict):
            return _tool_error("arguments must be an object")
        impl = _TOOL_IMPLS.get(name)
        if impl is None:
            return _tool_error(f"unknown tool: {name}")
        try:
            result = await impl(self, arguments)
        except RemoteError as e:
            return _tool_error(f"blemees-peerd error {e.code}: {e.message}")
        except Exception as e:
            log.exception("tool %s crashed", name)
            return _tool_error(f"internal error: {e}")
        return _tool_result(result)

    async def _h_resources_list(self, params: dict[str, Any]) -> dict[str, Any]:
        return {"resources": _RESOURCES}

    async def _h_resources_read(self, params: dict[str, Any]) -> dict[str, Any]:
        uri = params.get("uri")
        if uri == "peer://inbox":
            payload = list(self.inbox)
        elif uri == "peer://peers":
            try:
                payload = await self.peer.list_peers()
            except RemoteError as e:
                payload = {"error": e.message}
        elif uri == "peer://topics":
            try:
                payload = await self.peer.list_topics()
            except RemoteError as e:
                payload = {"error": e.message}
        else:
            raise ValueError(f"unknown resource: {uri}")
        return {
            "contents": [
                {
                    "uri": uri,
                    "mimeType": "application/json",
                    "text": json.dumps(payload, ensure_ascii=False, indent=2),
                }
            ]
        }

    async def _h_resources_subscribe(self, params: dict[str, Any]) -> dict[str, Any]:
        uri = params.get("uri")
        if not isinstance(uri, str):
            raise ValueError("uri must be a string")
        self.subscribed_resources.add(uri)
        return {}

    async def _h_resources_unsubscribe(self, params: dict[str, Any]) -> dict[str, Any]:
        uri = params.get("uri")
        if not isinstance(uri, str):
            raise ValueError("uri must be a string")
        self.subscribed_resources.discard(uri)
        return {}


# ----------------------------------------------------------------------
# Tool definitions
# ----------------------------------------------------------------------


def _tool_result(payload: Any) -> dict[str, Any]:
    if isinstance(payload, str):
        text = payload
    else:
        text = json.dumps(payload, ensure_ascii=False, indent=2)
    return {"content": [{"type": "text", "text": text}], "isError": False}


def _tool_error(message: str) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": message}], "isError": True}


async def _tool_send(s: McpServer, args: dict[str, Any]) -> Any:
    return await s.peer.send(
        to=args["to"],
        body=args.get("body"),
        reply_to=args.get("reply_to"),
    )


async def _tool_publish(s: McpServer, args: dict[str, Any]) -> Any:
    return await s.peer.publish(topic=args["topic"], body=args.get("body"))


async def _tool_subscribe(s: McpServer, args: dict[str, Any]) -> Any:
    return await s.peer.subscribe(
        topic=args["topic"],
        replay=int(args.get("replay", 0)),
    )


async def _tool_unsubscribe(s: McpServer, args: dict[str, Any]) -> Any:
    return await s.peer.unsubscribe(topic=args["topic"])


async def _tool_inbox(s: McpServer, args: dict[str, Any]) -> Any:
    limit = int(args.get("limit", 100))
    out: list[dict[str, Any]] = []
    while s.inbox and len(out) < limit:
        out.append(s.inbox.popleft())
    if not s.inbox:
        s._inbox_event.clear()
    return {"messages": out}


async def _tool_await(s: McpServer, args: dict[str, Any]) -> Any:
    timeout = args.get("timeout")
    timeout_s = float(timeout) if timeout is not None else None
    if s.inbox:
        msg = s.inbox.popleft()
        if not s.inbox:
            s._inbox_event.clear()
        return msg
    s._inbox_event.clear()
    try:
        if timeout_s is None:
            await s._inbox_event.wait()
        else:
            await asyncio.wait_for(s._inbox_event.wait(), timeout=timeout_s)
    except TimeoutError:
        return {"timeout": True}
    if s.inbox:
        msg = s.inbox.popleft()
        if not s.inbox:
            s._inbox_event.clear()
        return msg
    return {"timeout": True}


async def _tool_set_alias(s: McpServer, args: dict[str, Any]) -> Any:
    return await s.peer.set_alias(args.get("alias", ""))


async def _tool_set_dm_filter(s: McpServer, args: dict[str, Any]) -> Any:
    patterns = args.get("patterns") or []
    if not isinstance(patterns, list):
        raise ValueError("patterns must be a list")
    return await s.peer.set_dm_filter([str(p) for p in patterns])


async def _tool_list_peers(s: McpServer, args: dict[str, Any]) -> Any:
    return await s.peer.list_peers()


async def _tool_list_topics(s: McpServer, args: dict[str, Any]) -> Any:
    return await s.peer.list_topics()


async def _tool_history(s: McpServer, args: dict[str, Any]) -> Any:
    return await s.peer.history(
        address=args["address"],
        since=args.get("since"),
        limit=int(args.get("limit", 100)),
    )


_TOOL_IMPLS: dict[str, Callable[[McpServer, dict[str, Any]], Awaitable[Any]]] = {
    "peer_send": _tool_send,
    "peer_publish": _tool_publish,
    "peer_subscribe": _tool_subscribe,
    "peer_unsubscribe": _tool_unsubscribe,
    "peer_inbox": _tool_inbox,
    "peer_await": _tool_await,
    "peer_set_alias": _tool_set_alias,
    "peer_set_dm_filter": _tool_set_dm_filter,
    "peer_list_peers": _tool_list_peers,
    "peer_list_topics": _tool_list_topics,
    "peer_history": _tool_history,
}


_TOOLS: list[dict[str, Any]] = [
    {
        "name": "peer_send",
        "description": (
            "Send a direct message to another peer. Address forms: "
            "'home:~/path' (broadcast to all sessions at that path), "
            "'home:~/path#sess_xxx' (specific session by sid), "
            "'home:~/path#alias' (specific session by alias)."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "to": {"type": "string"},
                "body": {},
                "reply_to": {"type": "string"},
            },
            "required": ["to"],
        },
    },
    {
        "name": "peer_publish",
        "description": "Publish a message to a topic.",
        "inputSchema": {
            "type": "object",
            "properties": {"topic": {"type": "string"}, "body": {}},
            "required": ["topic"],
        },
    },
    {
        "name": "peer_subscribe",
        "description": (
            "Subscribe to a topic. If 'replay' > 0, the last N persisted "
            "messages are re-emitted as inbox notifications."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "topic": {"type": "string"},
                "replay": {"type": "integer", "minimum": 0, "default": 0},
            },
            "required": ["topic"],
        },
    },
    {
        "name": "peer_unsubscribe",
        "description": "Unsubscribe from a topic.",
        "inputSchema": {
            "type": "object",
            "properties": {"topic": {"type": "string"}},
            "required": ["topic"],
        },
    },
    {
        "name": "peer_inbox",
        "description": (
            "Drain queued peer notifications (messages, joins/leaves, "
            "alias changes). Returns up to 'limit' (default 100)."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {"type": "integer", "minimum": 1, "default": 100},
            },
        },
    },
    {
        "name": "peer_await",
        "description": (
            "Block until a notification arrives, then return it. If "
            "'timeout' (seconds) is given and elapses, returns {timeout: true}."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {"timeout": {"type": "number", "minimum": 0}},
        },
    },
    {
        "name": "peer_set_alias",
        "description": (
            "Claim a human-meaningful alias for this session, scoped "
            "to the current cwd. Pass empty string to clear."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {"alias": {"type": "string"}},
            "required": ["alias"],
        },
    },
    {
        "name": "peer_set_dm_filter",
        "description": (
            "Restrict inbound DMs to messages whose 'from' address "
            "matches at least one fnmatch pattern. Default ['*'] = "
            "accept all; [] = block all."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "patterns": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["patterns"],
        },
    },
    {
        "name": "peer_list_peers",
        "description": "List all currently-connected peers.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "peer_list_topics",
        "description": "List all topics with active subscribers.",
        "inputSchema": {"type": "object", "properties": {}},
    },
    {
        "name": "peer_history",
        "description": (
            "Replay persisted messages for a topic (full ring buffer) "
            "or DM address (queued/undelivered messages only)."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "address": {"type": "string"},
                "since": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "default": 100},
            },
            "required": ["address"],
        },
    },
]


_RESOURCES: list[dict[str, Any]] = [
    {
        "uri": "peer://inbox",
        "name": "Peer message inbox",
        "description": "Buffered notifications from the peer mesh, oldest first.",
        "mimeType": "application/json",
    },
    {
        "uri": "peer://peers",
        "name": "Connected peers",
        "description": "Live list of peers currently connected to blemees-peerd.",
        "mimeType": "application/json",
    },
    {
        "uri": "peer://topics",
        "name": "Topics",
        "description": "Topics with at least one subscriber, plus last_message_at.",
        "mimeType": "application/json",
    },
]


# ----------------------------------------------------------------------
# Offline shim
# ----------------------------------------------------------------------


class _OfflinePeerClient:
    """Stand-in for ``PeerClient`` when blemees-peerd was unreachable at startup.

    Every method raises ``RemoteError`` so the host sees a clear
    "blemees-peerd unavailable" message rather than the sidecar crashing.
    Replaces the real client only after ``peer.connect()`` failed.
    """

    def __init__(self, reason: str) -> None:
        self.reason = reason
        self._notifications: asyncio.Queue[dict[str, Any]] = asyncio.Queue()

    async def connect(self) -> None:
        raise RemoteError(-32099, self.reason)

    async def close(self) -> None:
        return None

    async def hello(self, *args, **kwargs) -> dict[str, Any]:
        raise RemoteError(-32099, f"blemees-peerd unavailable: {self.reason}")

    async def next_notification(self, timeout: float | None = None) -> dict[str, Any]:
        return await self._notifications.get()

    def __getattr__(self, name: str) -> Callable[..., Awaitable[dict[str, Any]]]:
        async def stub(*args: Any, **kwargs: Any) -> dict[str, Any]:
            raise RemoteError(-32099, f"blemees-peerd unavailable: {self.reason}")
        return stub


# ----------------------------------------------------------------------
# Identity resolution
# ----------------------------------------------------------------------


def resolve_home_at_startup() -> str:
    """Resolve the peer-identity home per SPEC §3.

    Reads ``BLEMEES_AGENT_HOME`` if set and well-formed; otherwise
    normalizes ``os.getcwd()``. The env var is a generic agent-context
    convention exported by harnesses like ``blemees-agentd`` — not a peer
    concept — and is consumed opportunistically because some MCP hosts
    spawn stdio subprocesses with a cwd unrelated to the agent's
    actual working directory.
    """
    env = os.environ.get("BLEMEES_AGENT_HOME")
    if env and is_normalized_path(env):
        return env
    return normalize_path(os.getcwd())


def resolve_sid_at_startup() -> str:
    """Resolve the session ID per SPEC §3.

    Reads ``BLEMEES_AGENT_SID`` if set and well-formed (so a harness
    like ``blemees-agentd`` can give every spawned agent a stable sid that
    transitively reaches its MCP subprocesses); otherwise mints a
    fresh sid.
    """
    from blemees_peer.addressing import is_valid_sid, mint_sid

    env = os.environ.get("BLEMEES_AGENT_SID")
    if env and is_valid_sid(env):
        return env
    return mint_sid()


def resolve_alias_at_startup() -> str | None:
    """Resolve a startup alias per SPEC §3.

    Reads ``BLEMEES_AGENT_ALIAS`` and validates it against the alias
    grammar (lowercase letter start, ``[a-z0-9_-]`` body, ≤ 64 chars,
    not starting with ``sess_``). Returns the alias if valid, else
    ``None``. The sidecar uses this to call ``peer.set_alias`` once
    after ``peer.hello``; failures (collision, malformed) are
    non-fatal — the agent can still address peers by sid.
    """
    from blemees_peer.addressing import is_valid_alias

    env = os.environ.get("BLEMEES_AGENT_ALIAS")
    if not env:
        return None
    if not is_valid_alias(env):
        log.warning(
            "ignoring BLEMEES_AGENT_ALIAS=%r: must match lowercase [a-z][a-z0-9_-]{0,63}",
            env,
        )
        return None
    return env
