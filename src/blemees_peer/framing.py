"""Newline-delimited framing shared by the daemon, client and MCP sidecar."""

from __future__ import annotations

import asyncio
import math
from typing import Any

MAX_LINE_BYTES = 4 * 1024 * 1024  # 4 MiB; messages larger than this are rejected
# Largest peer.send / peer.publish body peerd accepts. The headroom leaves
# space for the peer.message / peer.wire_message envelope around the body.
MAX_BODY_BYTES = MAX_LINE_BYTES - 64 * 1024


def is_valid_id(value: Any) -> bool:
    """JSON-RPC 2.0 ids are a string, a number or null.

    A bool is not a number. NaN and Infinity are not JSON numbers: Python's
    parser accepts them, but an echo of one would make the reply invalid JSON.
    """
    if isinstance(value, bool):
        return False
    if isinstance(value, float):
        return math.isfinite(value)
    return value is None or isinstance(value, (str, int))


def is_response(msg: dict[str, Any]) -> bool:
    """True for a response object: ``result`` or ``error`` and no ``method``."""
    return "method" not in msg and ("result" in msg or "error" in msg)


class LineTooLongError(Exception):
    """A line exceeded the reader's limit; it was discarded up to its newline."""


async def read_line(reader: asyncio.StreamReader) -> bytes:
    """Read one ``\\n``-terminated line.

    A line longer than the reader's limit raises ``LineTooLongError`` after its
    bytes, up to and including the next newline, are consumed, so the caller
    can keep reading. EOF raises ``asyncio.IncompleteReadError`` as usual.
    """
    try:
        return await reader.readuntil(b"\n")
    except asyncio.LimitOverrunError as e:
        consumed = e.consumed
    # On overrun the bytes stay in the buffer. Drop what was scanned, then
    # retry until the newline itself is consumed.
    while True:
        await reader.readexactly(consumed)
        try:
            await reader.readuntil(b"\n")
        except asyncio.LimitOverrunError as e:
            consumed = e.consumed
        else:
            raise LineTooLongError
