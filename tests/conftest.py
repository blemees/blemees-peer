"""Shared pytest fixtures."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import shutil
import tempfile
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path

import pytest
import pytest_asyncio

from blemees_peer.addressing import mint_sid
from blemees_peer.client import PeerClient
from blemees_peer.persistence import Persistence
from blemees_peer.server import Server

logging.basicConfig(level=logging.WARNING)


@pytest.fixture
def tmp_state_dir(tmp_path: Path) -> Path:
    return tmp_path / "state"


@pytest.fixture
def tmp_socket_path() -> Path:
    """A short socket path under /tmp.

    pytest's ``tmp_path`` lives under ``/private/var/folders/...`` on
    macOS which can blow past Unix's 104-char socket name limit, so we
    use ``/tmp`` directly for the socket and clean it up afterwards.
    """
    short_dir = tempfile.mkdtemp(prefix="bp-", dir="/tmp")
    socket_path = Path(short_dir) / "peerd.sock"
    yield socket_path
    shutil.rmtree(short_dir, ignore_errors=True)


@pytest_asyncio.fixture
async def peerd_server(
    tmp_state_dir: Path,
    tmp_socket_path: Path,
) -> AsyncIterator[Server]:
    persistence = Persistence(tmp_state_dir)
    server = Server(tmp_socket_path, persistence)
    await server.start()
    serve_task = asyncio.create_task(server.serve_forever())
    try:
        yield server
    finally:
        serve_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await serve_task
        await server.shutdown()


@pytest_asyncio.fixture
async def client_factory(
    peerd_server: Server,
    tmp_socket_path: Path,
) -> AsyncIterator[Callable[..., Awaitable[PeerClient]]]:
    """Yield an async factory that produces connected, hello'd PeerClients."""
    clients: list[PeerClient] = []

    async def make(
        sid: str | None = None,
        home: str = "/tmp/peer-test-home-A",
        alias: str | None = None,
    ) -> PeerClient:
        client = PeerClient(tmp_socket_path)
        await client.connect()
        await client.hello(sid or mint_sid(), home)
        if alias is not None:
            await client.set_alias(alias)
        clients.append(client)
        # Give blemees-peerd a moment to deliver any join notifications to other peers
        await asyncio.sleep(0)
        return client

    try:
        yield make
    finally:
        for c in clients:
            with contextlib.suppress(Exception):
                await c.close()
