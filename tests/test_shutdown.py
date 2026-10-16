"""Shutdown with live clients connected.

Regression tests for blemees/blemees-peer#1: on Python >= 3.12,
``AbstractServer.wait_closed()`` waits for every connection handler, so
shutdown deadlocked while any client sat idle on the socket. The other
fixtures disconnect clients before teardown, which hid the bug.
"""

from __future__ import annotations

import asyncio
import contextlib
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

from blemees_peer.client import PeerClient
from blemees_peer.persistence import Persistence
from blemees_peer.server import Server

SHUTDOWN_BUDGET_S = 2.0


async def test_shutdown_with_idle_clients_is_prompt(
    tmp_state_dir: Path,
    tmp_socket_path: Path,
) -> None:
    server = Server(tmp_socket_path, Persistence(tmp_state_dir))
    await server.start()
    serve_task = asyncio.create_task(server.serve_forever())

    hello_client = PeerClient(tmp_socket_path)
    await hello_client.connect()
    await hello_client.hello("sess_AAAAAAAAAAAAAAAA", "/tmp/peer-test-A")
    raw_client = PeerClient(tmp_socket_path)
    await raw_client.connect()
    # Let the server's handlers reach their idle readuntil().
    await asyncio.sleep(0.05)
    assert len(server._connections) == 2

    try:
        await asyncio.wait_for(server.shutdown(), timeout=SHUTDOWN_BUDGET_S)
        await asyncio.wait_for(serve_task, timeout=SHUTDOWN_BUDGET_S)
        assert not server._connections
        assert not server._handler_tasks
        assert not tmp_socket_path.exists()
    finally:
        # Close clients first: on a regression this unblocks the stuck
        # handlers, so the test fails instead of hanging.
        await hello_client.close()
        await raw_client.close()
        serve_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await serve_task


def test_daemon_exits_promptly_on_sigterm_with_idle_client(
    tmp_state_dir: Path,
    tmp_socket_path: Path,
) -> None:
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "blemees_peer.daemon",
            "--socket",
            str(tmp_socket_path),
            "--state-dir",
            str(tmp_state_dir),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        deadline = time.monotonic() + 10.0
        while not tmp_socket_path.exists():
            assert proc.poll() is None, "daemon exited before listening"
            assert time.monotonic() < deadline, "daemon never created its socket"
            time.sleep(0.05)
        client.connect(str(tmp_socket_path))
        time.sleep(0.2)

        started = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        try:
            returncode = proc.wait(timeout=SHUTDOWN_BUDGET_S)
        except subprocess.TimeoutExpired:
            raise AssertionError(
                f"daemon still running {SHUTDOWN_BUDGET_S}s after SIGTERM with a client connected"
            ) from None
        elapsed = time.monotonic() - started
    finally:
        client.close()
        if proc.poll() is None:
            proc.kill()
            proc.wait()
    stderr = proc.stderr.read() if proc.stderr else ""
    if proc.stderr:
        proc.stderr.close()

    assert returncode == 0, stderr
    assert elapsed < SHUTDOWN_BUDGET_S
    assert "Task was destroyed but it is pending" not in stderr
    assert "Traceback" not in stderr
