"""Asyncio Unix-socket server: parses JSON-RPC and dispatches to ``Router``."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from blemees_peer.addressing import is_normalized_path, is_valid_sid
from blemees_peer.connection import Connection
from blemees_peer.errors import (
    INTERNAL_ERROR,
    INVALID_PARAMS,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
    PARSE_ERROR,
    PeerError,
)
from blemees_peer.persistence import Persistence
from blemees_peer.router import Router

log = logging.getLogger("blemees-peerd.server")


MAX_LINE_BYTES = 4 * 1024 * 1024  # 4 MiB; messages larger than this are rejected


class Server:
    """The blemees-peerd Unix-socket server.

    Wires asyncio I/O to the synchronous ``Router`` logic, handles
    JSON-RPC framing, and manages connection lifecycles.
    """

    def __init__(
        self,
        socket_path: Path,
        persistence: Persistence,
        socket_mode: int = 0o600,
    ) -> None:
        self.socket_path = socket_path
        self.persistence = persistence
        self.socket_mode = socket_mode
        self.router = Router(persistence)
        self._server: asyncio.AbstractServer | None = None
        self._connections: set[Connection] = set()
        self._handlers: dict[str, Handler] = self._build_handlers()

    async def start(self) -> None:
        # Refuse to start if the socket file exists and is live; remove if stale.
        if self.socket_path.exists():
            if not self.socket_path.is_socket():
                raise RuntimeError(
                    f"{self.socket_path} exists and is not a socket — refusing to clobber"
                )
            try:
                # Probe: try to connect; if that fails, the socket is stale.
                reader, writer = await asyncio.open_unix_connection(str(self.socket_path))
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()
                _ = reader
                raise RuntimeError(
                    f"another blemees-peerd is already listening on {self.socket_path}"
                )
            except (FileNotFoundError, ConnectionRefusedError):
                self.socket_path.unlink(missing_ok=True)

        self.persistence.load_all()
        self.socket_path.parent.mkdir(parents=True, exist_ok=True)
        self._server = await asyncio.start_unix_server(
            self._handle_connection, path=str(self.socket_path)
        )
        os.chmod(self.socket_path, self.socket_mode)
        log.info("listening on %s", self.socket_path)

    async def serve_forever(self) -> None:
        if self._server is None:
            raise RuntimeError("server not started")
        async with self._server:
            await self._server.serve_forever()

    async def shutdown(self) -> None:
        if self._server is not None:
            self._server.close()
            with contextlib.suppress(Exception):
                await self._server.wait_closed()
        # Close connections in parallel
        if self._connections:
            await asyncio.gather(
                *(c.close() for c in list(self._connections)),
                return_exceptions=True,
            )
            self._connections.clear()
        self.socket_path.unlink(missing_ok=True)
        log.info("shut down")

    async def _handle_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        conn = Connection(reader, writer)
        self._connections.add(conn)
        conn.start_writer()
        try:
            while True:
                try:
                    line = await reader.readuntil(b"\n")
                except asyncio.IncompleteReadError as e:
                    if e.partial:
                        log.debug("client disconnected with partial line; discarding")
                    break
                except asyncio.LimitOverrunError:
                    await self._send_error(conn, None, PARSE_ERROR, "line exceeds size limit")
                    break
                if len(line) > MAX_LINE_BYTES:
                    await self._send_error(conn, None, PARSE_ERROR, "line exceeds size limit")
                    break
                await self._dispatch_line(conn, line)
        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception:
            log.exception("connection handler crashed for %s", conn.peer_id)
        finally:
            await self.router.goodbye(conn)
            await conn.close()
            self._connections.discard(conn)

    async def _dispatch_line(self, conn: Connection, line: bytes) -> None:
        text = line.decode("utf-8", errors="replace").strip()
        if not text:
            return
        try:
            request = json.loads(text)
        except json.JSONDecodeError as e:
            await self._send_error(conn, None, PARSE_ERROR, f"invalid JSON: {e.msg}")
            return
        if not isinstance(request, dict):
            await self._send_error(conn, None, INVALID_REQUEST, "request must be a JSON object")
            return
        await self._dispatch(conn, request)

    async def _dispatch(self, conn: Connection, request: dict[str, Any]) -> None:
        request_id = request.get("id")
        method = request.get("method")
        params = request.get("params") or {}

        if request.get("jsonrpc") != "2.0":
            await self._send_error(conn, request_id, INVALID_REQUEST, "jsonrpc must be '2.0'")
            return
        if not isinstance(method, str):
            await self._send_error(conn, request_id, INVALID_REQUEST, "method must be a string")
            return
        if not isinstance(params, dict):
            await self._send_error(conn, request_id, INVALID_PARAMS, "params must be an object")
            return

        handler = self._handlers.get(method)
        if handler is None:
            await self._send_error(
                conn,
                request_id,
                METHOD_NOT_FOUND,
                f"unknown method: {method!r}",
            )
            return

        try:
            result = await handler(conn, params)
        except PeerError as e:
            await self._send_error(conn, request_id, e.code, e.message, e.data)
            return
        except Exception:
            log.exception("handler %s crashed", method)
            await self._send_error(conn, request_id, INTERNAL_ERROR, "internal error")
            return

        if request_id is not None:
            await conn.send_response(request_id, result)

    async def _send_error(
        self,
        conn: Connection,
        request_id: Any,
        code: int,
        message: str,
        data: Any | None = None,
    ) -> None:
        if request_id is None:
            # No id → can't return a structured error; just log and drop.
            log.warning("dropping unidentifiable error: %s %s", code, message)
            return
        await conn.send_error(request_id, code, message, data)

    # ------------------------------------------------------------------
    # Handler bindings
    # ------------------------------------------------------------------

    def _build_handlers(self) -> dict[str, Handler]:
        return {
            "peer.hello": self._h_hello,
            "peer.set_alias": self._h_set_alias,
            "peer.set_dm_filter": self._h_set_dm_filter,
            "peer.send": self._h_send,
            "peer.publish": self._h_publish,
            "peer.subscribe": self._h_subscribe,
            "peer.unsubscribe": self._h_unsubscribe,
            "peer.list_peers": self._h_list_peers,
            "peer.list_topics": self._h_list_topics,
            "peer.history": self._h_history,
        }

    async def _h_hello(self, conn: Connection, params: dict[str, Any]) -> dict[str, Any]:
        if conn.hello_complete:
            raise PeerError(INVALID_REQUEST, "peer.hello already sent on this connection")
        sid = params.get("sid")
        home = params.get("home")
        if not isinstance(sid, str) or not is_valid_sid(sid):
            raise PeerError(INVALID_PARAMS, "sid must match 'sess_' + 16 url-safe base64 chars")
        if not isinstance(home, str) or not is_normalized_path(home):
            raise PeerError(INVALID_PARAMS, "home must be normalized per SPEC §2")
        return await self.router.hello(conn, sid, home)

    async def _h_set_alias(self, conn: Connection, params: dict[str, Any]) -> dict[str, Any]:
        alias = params.get("alias")
        if not isinstance(alias, str):
            raise PeerError(INVALID_PARAMS, "alias must be a string (empty to clear)")
        return await self.router.set_alias(conn, alias)

    async def _h_set_dm_filter(self, conn: Connection, params: dict[str, Any]) -> dict[str, Any]:
        patterns = params.get("patterns")
        if not isinstance(patterns, list):
            raise PeerError(INVALID_PARAMS, "patterns must be a list of strings")
        return await self.router.set_dm_filter(conn, patterns)

    async def _h_send(self, conn: Connection, params: dict[str, Any]) -> dict[str, Any]:
        to = params.get("to")
        body = params.get("body")
        reply_to = params.get("reply_to")
        if not isinstance(to, str):
            raise PeerError(INVALID_PARAMS, "to must be a string address")
        if reply_to is not None and not isinstance(reply_to, str):
            raise PeerError(INVALID_PARAMS, "reply_to must be a string or omitted")
        return await self.router.send(conn, to, body, reply_to)

    async def _h_publish(self, conn: Connection, params: dict[str, Any]) -> dict[str, Any]:
        topic = params.get("topic")
        body = params.get("body")
        if not isinstance(topic, str):
            raise PeerError(INVALID_PARAMS, "topic must be a string")
        return await self.router.publish(conn, topic, body)

    async def _h_subscribe(self, conn: Connection, params: dict[str, Any]) -> dict[str, Any]:
        topic = params.get("topic")
        replay = params.get("replay", 0)
        if not isinstance(topic, str):
            raise PeerError(INVALID_PARAMS, "topic must be a string")
        return await self.router.subscribe(conn, topic, replay)

    async def _h_unsubscribe(self, conn: Connection, params: dict[str, Any]) -> dict[str, Any]:
        topic = params.get("topic")
        if not isinstance(topic, str):
            raise PeerError(INVALID_PARAMS, "topic must be a string")
        return await self.router.unsubscribe(conn, topic)

    async def _h_list_peers(self, conn: Connection, params: dict[str, Any]) -> dict[str, Any]:
        return await self.router.list_peers(conn)

    async def _h_list_topics(self, conn: Connection, params: dict[str, Any]) -> dict[str, Any]:
        return await self.router.list_topics(conn)

    async def _h_history(self, conn: Connection, params: dict[str, Any]) -> dict[str, Any]:
        address = params.get("address")
        since = params.get("since")
        limit = params.get("limit", 100)
        if not isinstance(address, str):
            raise PeerError(INVALID_PARAMS, "address must be a string")
        if since is not None and not isinstance(since, str):
            raise PeerError(INVALID_PARAMS, "since must be a string or omitted")
        return await self.router.history(conn, address, since, limit)


Handler = Callable[[Connection, dict[str, Any]], Awaitable[dict[str, Any]]]
