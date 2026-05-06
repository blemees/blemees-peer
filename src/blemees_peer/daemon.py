"""``blemees-peerd`` CLI entry point."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
import sys
from pathlib import Path

from blemees_peer._version import __version__
from blemees_peer.persistence import (
    Persistence,
    default_socket_path,
    default_state_dir,
)
from blemees_peer.server import Server

log = logging.getLogger("blemees-peerd")


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="blemees-peerd",
        description="blemees-peer daemon — peer messaging for blemees agents",
    )
    p.add_argument(
        "--socket",
        type=Path,
        default=None,
        help="Unix socket path (default: $XDG_RUNTIME_DIR/blemees/peerd.sock)",
    )
    p.add_argument(
        "--state-dir",
        type=Path,
        default=None,
        help="state directory (default: $XDG_STATE_HOME/blemees/peerd)",
    )
    p.add_argument(
        "--dm-maxlen",
        type=int,
        default=Persistence.DEFAULT_DM_MAXLEN,
        help="max queued DMs per recipient address (default: %(default)s)",
    )
    p.add_argument(
        "--topic-maxlen",
        type=int,
        default=Persistence.DEFAULT_TOPIC_MAXLEN,
        help="max retained messages per topic ring buffer (default: %(default)s)",
    )
    p.add_argument(
        "--log-level",
        choices=["debug", "info", "warning", "error"],
        default="info",
    )
    p.add_argument("--version", action="version", version=f"blemees-peerd {__version__}")
    return p


async def _run(server: Server) -> int:
    loop = asyncio.get_running_loop()
    stop = loop.create_future()

    def _signal_handler() -> None:
        if not stop.done():
            stop.set_result(None)

    for sig in (signal.SIGINT, signal.SIGTERM):
        # Windows lacks add_signal_handler; we don't target Windows but
        # tolerate platforms that throw rather than crashing on import.
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _signal_handler)

    await server.start()
    serve_task = asyncio.create_task(server.serve_forever())
    try:
        await stop
    finally:
        serve_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await serve_task
        await server.shutdown()
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    socket_path = args.socket or default_socket_path()
    state_dir = args.state_dir or default_state_dir()
    persistence = Persistence(
        state_dir,
        dm_maxlen=args.dm_maxlen,
        topic_maxlen=args.topic_maxlen,
    )
    server = Server(socket_path, persistence)
    try:
        return asyncio.run(_run(server))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
