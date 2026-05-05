"""JSON-RPC error codes used by ``blemees-peerd``.

Standard JSON-RPC 2.0 codes (-32700 through -32603) are reserved.
Application-specific codes start at -32000 (the lower bound for the
"Server error" range) and use stable numbers documented in the SPEC
so clients can switch on them.
"""

from __future__ import annotations

# JSON-RPC 2.0 standard codes
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603

# blemees-peerd application codes (must stay in sync with SPEC §8)
HELLO_REQUIRED = -32000
ALIAS_TAKEN = -32010
SID_IN_USE = -32011


class PeerError(Exception):
    """A JSON-RPC error raised by router/server handlers.

    Carries a numeric *code* and human-readable *message*; the server
    serializes these into a JSON-RPC error response.
    """

    def __init__(self, code: int, message: str, data: object | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data

    def to_wire(self) -> dict[str, object]:
        out: dict[str, object] = {"code": self.code, "message": self.message}
        if self.data is not None:
            out["data"] = self.data
        return out
