"""``blemees-peer-mcp`` CLI entry point: stdio MCP sidecar."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from blemees_peer._version import __version__
from blemees_peer.mcp_server import (
    McpServer,
    resolve_alias_at_startup,
    resolve_home_at_startup,
    resolve_sid_at_startup,
)
from blemees_peer.persistence import default_socket_path


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="blemees-peer-mcp",
        description="Stdio MCP sidecar for blemees-peer; bridges agents to blemees-peerd.",
    )
    p.add_argument("--socket", type=Path, default=None, help="blemees-peerd socket path")
    p.add_argument(
        "--home",
        default=None,
        help="override the peer-identity home (else $BLEMEES_AGENT_HOME or os.getcwd())",
    )
    p.add_argument(
        "--sid",
        default=None,
        help="override the session ID (else $BLEMEES_AGENT_SID or freshly minted)",
    )
    p.add_argument(
        "--alias",
        default=None,
        help="claim this alias at startup (else $BLEMEES_AGENT_ALIAS, else none)",
    )
    p.add_argument(
        "--log-level",
        choices=["debug", "info", "warning", "error"],
        default="info",
    )
    p.add_argument(
        "--log-file",
        type=Path,
        default=None,
        help="write logs to this file in addition to stderr",
    )
    p.add_argument("--version", action="version", version=f"blemees-peer-mcp {__version__}")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stderr)]
    if args.log_file is not None:
        args.log_file.parent.mkdir(parents=True, exist_ok=True)
        handlers.append(logging.FileHandler(args.log_file))
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=handlers,
    )

    socket_path = args.socket or default_socket_path()
    home = args.home or resolve_home_at_startup()
    sid = args.sid or resolve_sid_at_startup()
    alias = args.alias if args.alias is not None else resolve_alias_at_startup()

    server = McpServer(socket_path=socket_path, home=home, sid=sid, alias=alias)
    try:
        return asyncio.run(server.run())
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
