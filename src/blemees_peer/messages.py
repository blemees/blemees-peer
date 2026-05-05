"""Message types and message-id minting."""

from __future__ import annotations

import secrets
import time
from dataclasses import dataclass, field
from typing import Any


def mint_message_id() -> str:
    return f"msg_{secrets.token_urlsafe(12)}"


def now_ts() -> float:
    return time.time()


@dataclass(slots=True)
class Message:
    """A single message in flight (DM or topic).

    ``from_addr`` is always the canonical sid form (``home:.../#sess_...``)
    for stable correlation across alias changes; ``from_alias`` is a
    snapshot of the sender's alias at send time, or ``None``.
    """

    message_id: str
    from_addr: str
    from_alias: str | None
    to: str
    body: Any
    reply_to: str | None
    ts: float = field(default_factory=now_ts)

    def to_wire(self) -> dict[str, Any]:
        """Render as a JSON-friendly dict matching the SPEC ``peer.message`` shape."""
        return {
            "message_id": self.message_id,
            "from": self.from_addr,
            "from_alias": self.from_alias,
            "to": self.to,
            "body": self.body,
            "reply_to": self.reply_to,
            "ts": self.ts,
        }

    @classmethod
    def from_wire(cls, data: dict[str, Any]) -> Message:
        return cls(
            message_id=data["message_id"],
            from_addr=data["from"],
            from_alias=data.get("from_alias"),
            to=data["to"],
            body=data["body"],
            reply_to=data.get("reply_to"),
            ts=float(data.get("ts", now_ts())),
        )

    def to_persisted(self) -> dict[str, Any]:
        """Persisted form is the same shape as the wire form."""
        return self.to_wire()

    @classmethod
    def from_persisted(cls, data: dict[str, Any]) -> Message:
        return cls.from_wire(data)
