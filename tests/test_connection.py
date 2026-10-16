"""``Connection.close()`` under cancellation and stuck transports (#10).

These use a fake ``StreamWriter`` so a test can make ``drain()`` or
``wait_closed()`` block forever, as a peer that stops reading would.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from blemees_peer import connection as connection_mod
from blemees_peer.connection import Connection


class FakeWriter:
    """Stand-in for ``asyncio.StreamWriter`` with blocking knobs."""

    def __init__(self, *, drain_blocks: bool = False, wait_closed_blocks: bool = False) -> None:
        self.drain_blocks = drain_blocks
        self.wait_closed_blocks = wait_closed_blocks
        self.written: list[bytes] = []
        self.closed = False

    def write(self, data: bytes) -> None:
        self.written.append(data)

    async def drain(self) -> None:
        if self.drain_blocks:
            await asyncio.Event().wait()

    def close(self) -> None:
        self.closed = True

    async def wait_closed(self) -> None:
        if self.wait_closed_blocks:
            await asyncio.Event().wait()


def _connection(writer: FakeWriter) -> Connection:
    reader: Any = None
    return Connection(reader, writer)  # type: ignore[arg-type]


async def test_close_propagates_cancel_and_still_closes_transport() -> None:
    writer = FakeWriter(drain_blocks=True)
    conn = _connection(writer)
    conn.start_writer()
    await conn.send_notification("peer.test", {})
    await asyncio.sleep(0)  # writer loop is now stuck in drain()

    closing = asyncio.create_task(conn.close())
    await asyncio.sleep(0.01)
    closing.cancel()
    await asyncio.wait([closing], timeout=1.0)

    assert closing.cancelled()
    assert writer.closed
    assert conn._writer_task is not None and conn._writer_task.done()


async def test_close_is_bounded_when_wait_closed_never_returns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(connection_mod, "CLOSE_TIMEOUT_S", 0.05)
    writer = FakeWriter(wait_closed_blocks=True)
    conn = _connection(writer)
    conn.start_writer()

    await asyncio.wait_for(conn.close(), timeout=1.0)

    assert writer.closed
