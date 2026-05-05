"""Async ``peer/1`` client.

Used by the MCP sidecar, by tests, and as a reference implementation
of the protocol. Speaks newline-delimited JSON-RPC 2.0 over the
``blemees-peerd`` Unix socket.

Usage:

    async with PeerClient(socket_path) as client:
        await client.hello(sid, cwd)
        await client.set_alias("architect")
        await client.send(to="home:~/foo", body={"text": "hi"})
        msg = await client.next_notification(timeout=5.0)

"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import Any

log = logging.getLogger("blemees-blemees-peerd.client")


class RemoteError(Exception):
    """Raised when the server returns a JSON-RPC error response."""

    def __init__(self, code: int, message: str, data: Any | None = None) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.data = data


class PeerClient:
    """Async client for the ``peer/1`` JSON-RPC protocol."""

    def __init__(self, socket_path: Path | str) -> None:
        self.socket_path = Path(socket_path)
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._next_id = 0
        self._pending: dict[int, asyncio.Future[Any]] = {}
        self._notifications: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._read_task: asyncio.Task[None] | None = None
        self._closed = False

    async def connect(self) -> None:
        if self._writer is not None:
            return
        self._reader, self._writer = await asyncio.open_unix_connection(str(self.socket_path))
        self._read_task = asyncio.create_task(self._read_loop(), name="peer-client-read")

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._writer is not None:
            try:
                self._writer.close()
                await self._writer.wait_closed()
            except Exception:
                pass
        if self._read_task is not None and not self._read_task.done():
            try:
                await asyncio.wait_for(self._read_task, timeout=1.0)
            except (TimeoutError, Exception):
                self._read_task.cancel()
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(ConnectionError("blemees-peerd connection closed"))
        self._pending.clear()

    async def __aenter__(self) -> PeerClient:
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _call(self, method: str, params: dict[str, Any]) -> Any:
        if self._writer is None:
            raise RuntimeError("client not connected")
        self._next_id += 1
        rid = self._next_id
        fut: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._pending[rid] = fut
        message = {"jsonrpc": "2.0", "id": rid, "method": method, "params": params}
        line = json.dumps(message, separators=(",", ":"), ensure_ascii=False) + "\n"
        self._writer.write(line.encode("utf-8"))
        await self._writer.drain()
        return await fut

    async def _read_loop(self) -> None:
        assert self._reader is not None
        try:
            while True:
                try:
                    line = await self._reader.readuntil(b"\n")
                except (asyncio.IncompleteReadError, ConnectionResetError):
                    return
                text = line.decode("utf-8", errors="replace").strip()
                if not text:
                    continue
                try:
                    msg = json.loads(text)
                except json.JSONDecodeError:
                    log.warning("could not decode line from blemees-peerd: %r", text)
                    continue
                if not isinstance(msg, dict):
                    continue
                if "id" in msg and ("result" in msg or "error" in msg):
                    fut = self._pending.pop(msg["id"], None)
                    if fut is None or fut.done():
                        continue
                    if "error" in msg:
                        err = msg["error"]
                        fut.set_exception(
                            RemoteError(
                                code=err.get("code", 0),
                                message=err.get("message", "unknown error"),
                                data=err.get("data"),
                            )
                        )
                    else:
                        fut.set_result(msg["result"])
                else:
                    await self._notifications.put(msg)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("client read loop crashed")

    # ------------------------------------------------------------------
    # Methods
    # ------------------------------------------------------------------

    async def hello(self, sid: str, home: str) -> dict[str, Any]:
        return await self._call("peer.hello", {"sid": sid, "home": home})

    async def set_alias(self, alias: str) -> dict[str, Any]:
        return await self._call("peer.set_alias", {"alias": alias})

    async def set_dm_filter(self, patterns: list[str]) -> dict[str, Any]:
        return await self._call("peer.set_dm_filter", {"patterns": patterns})

    async def send(
        self,
        to: str,
        body: Any,
        reply_to: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"to": to, "body": body}
        if reply_to is not None:
            params["reply_to"] = reply_to
        return await self._call("peer.send", params)

    async def publish(self, topic: str, body: Any) -> dict[str, Any]:
        return await self._call("peer.publish", {"topic": topic, "body": body})

    async def subscribe(self, topic: str, replay: int = 0) -> dict[str, Any]:
        return await self._call("peer.subscribe", {"topic": topic, "replay": replay})

    async def unsubscribe(self, topic: str) -> dict[str, Any]:
        return await self._call("peer.unsubscribe", {"topic": topic})

    async def list_peers(self) -> dict[str, Any]:
        return await self._call("peer.list_peers", {})

    async def list_topics(self) -> dict[str, Any]:
        return await self._call("peer.list_topics", {})

    async def history(
        self,
        address: str,
        since: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"address": address, "limit": limit}
        if since is not None:
            params["since"] = since
        return await self._call("peer.history", params)

    # ------------------------------------------------------------------
    # Notification access
    # ------------------------------------------------------------------

    async def next_notification(
        self,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Return the next queued notification, blocking up to *timeout* seconds.

        Raises ``asyncio.TimeoutError`` if *timeout* elapses with no
        notification queued.
        """
        if timeout is None:
            return await self._notifications.get()
        return await asyncio.wait_for(self._notifications.get(), timeout=timeout)

    def drain_notifications(self) -> list[dict[str, Any]]:
        """Pop every currently-queued notification without blocking."""
        out: list[dict[str, Any]] = []
        while True:
            try:
                out.append(self._notifications.get_nowait())
            except asyncio.QueueEmpty:
                break
        return out
