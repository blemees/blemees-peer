"""Per-connection state and outbound JSON-RPC framing.

A ``Connection`` is created when a client opens the socket and is
torn down when the client disconnects. It holds the session's
identity (sid, home, alias), its subscription set, its DM filter,
and an outbound queue used to multiplex responses and notifications
onto the wire without interleaving them.
"""

from __future__ import annotations

import asyncio
import fnmatch
import json
import logging
from typing import Any

log = logging.getLogger("blemees-peerd.connection")


class Connection:
    """One peer connection."""

    def __init__(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        self.reader = reader
        self.writer = writer
        self.sid: str | None = None
        self.home: str | None = None
        self.alias: str | None = None
        self.subscriptions: set[str] = set()
        self.dm_filter: list[str] = ["*"]
        self.outbox: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        self.closed = False
        self._writer_task: asyncio.Task[None] | None = None

    @property
    def hello_complete(self) -> bool:
        return self.sid is not None

    @property
    def peer_id(self) -> str:
        if self.home is None:
            return "<unidentified>"
        return f"home:{self.home}"

    @property
    def from_addr(self) -> str:
        if self.home is None or self.sid is None:
            raise RuntimeError("connection has no identity yet")
        return f"home:{self.home}#{self.sid}"

    def matches_dm_filter(self, from_addr: str) -> bool:
        return any(fnmatch.fnmatchcase(from_addr, p) for p in self.dm_filter)

    async def send_response(self, request_id: Any, result: Any) -> None:
        await self._enqueue({"jsonrpc": "2.0", "id": request_id, "result": result})

    async def send_error(
        self,
        request_id: Any,
        code: int,
        message: str,
        data: Any | None = None,
    ) -> None:
        err: dict[str, Any] = {"code": code, "message": message}
        if data is not None:
            err["data"] = data
        await self._enqueue({"jsonrpc": "2.0", "id": request_id, "error": err})

    async def send_notification(self, method: str, params: Any) -> None:
        await self._enqueue({"jsonrpc": "2.0", "method": method, "params": params})

    async def _enqueue(self, message: dict[str, Any]) -> None:
        if self.closed:
            return
        await self.outbox.put(message)

    def start_writer(self) -> None:
        self._writer_task = asyncio.create_task(self._write_loop())

    async def _write_loop(self) -> None:
        try:
            while True:
                msg = await self.outbox.get()
                if msg is None:
                    return
                line = json.dumps(msg, separators=(",", ":"), ensure_ascii=False) + "\n"
                try:
                    self.writer.write(line.encode("utf-8"))
                    await self.writer.drain()
                except (ConnectionResetError, BrokenPipeError):
                    self.closed = True
                    return
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("write loop crashed for %s", self.peer_id)
            self.closed = True

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        await self.outbox.put(None)
        if self._writer_task is not None:
            try:
                await asyncio.wait_for(self._writer_task, timeout=2.0)
            except (TimeoutError, asyncio.CancelledError):
                self._writer_task.cancel()
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception:
            pass
