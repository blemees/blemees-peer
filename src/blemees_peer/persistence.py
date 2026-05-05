"""Disk-backed message storage: bounded DM queues and topic ring buffers.

For v0.1 we keep this deliberately simple: each address maps to one
``PersistentDeque`` whose entire contents are atomically rewritten on
every mutation. With the default bounds (100 messages per DM queue,
1000 per topic ring buffer) write cost stays bounded; switching to
append + periodic compaction is a v0.2 concern only if real-world
throughput pushes the issue.

Addresses are hashed (sha256, first 16 hex chars) to avoid leaking
filesystem paths via on-disk filenames.
"""

from __future__ import annotations

import collections
import hashlib
import json
import os
from pathlib import Path

from blemees_peer.messages import Message


def hash_address(address: str) -> str:
    return hashlib.sha256(address.encode("utf-8")).hexdigest()[:16]


class PersistentDeque:
    """A bounded ``collections.deque`` mirrored to a JSONL file."""

    def __init__(self, path: Path, maxlen: int) -> None:
        self.path = path
        self.maxlen = maxlen
        self.deque: collections.deque[Message] = collections.deque(maxlen=maxlen)

    def __len__(self) -> int:
        return len(self.deque)

    def __iter__(self):
        return iter(self.deque)

    def load(self) -> None:
        if not self.path.exists():
            return
        with self.path.open(encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    data = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(data, dict) or "message_id" not in data:
                    continue
                try:
                    self.deque.append(Message.from_persisted(data))
                except (KeyError, TypeError, ValueError):
                    continue

    def append(self, msg: Message) -> None:
        self.deque.append(msg)
        self._flush()

    def drain(self) -> list[Message]:
        """Remove and return every queued message. Deletes the file."""
        msgs = list(self.deque)
        self.deque.clear()
        self.path.unlink(missing_ok=True)
        return msgs

    def clear(self) -> None:
        self.deque.clear()
        self.path.unlink(missing_ok=True)

    def history(self, since: str | None = None, limit: int = 100) -> list[Message]:
        """Return up to *limit* messages, optionally starting after *since*.

        If ``since`` is given but not found, returns the trailing
        ``limit`` messages — same effect as if the caller had no
        cursor.
        """
        if limit <= 0:
            return []
        items = list(self.deque)
        if since is None:
            return items[-limit:]
        for i, msg in enumerate(items):
            if msg.message_id == since:
                return items[i + 1 : i + 1 + limit]
        return items[-limit:]

    def _flush(self) -> None:
        if not self.deque:
            self.path.unlink(missing_ok=True)
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as f:
            for msg in self.deque:
                f.write(json.dumps(msg.to_persisted(), separators=(",", ":")))
                f.write("\n")
        os.replace(tmp, self.path)


class AddressableStore:
    """Lazy address → ``PersistentDeque`` map.

    Used for both DM queues (one deque per recipient address) and
    topic ring buffers (one deque per topic name). The only difference
    is the on-disk subdirectory and the default ``maxlen``.
    """

    def __init__(self, root: Path, maxlen: int) -> None:
        self.root = root
        self.maxlen = maxlen
        self.buffers: dict[str, PersistentDeque] = {}

    def get(self, address: str) -> PersistentDeque:
        buf = self.buffers.get(address)
        if buf is None:
            path = self.root / f"{hash_address(address)}.jsonl"
            buf = PersistentDeque(path, self.maxlen)
            buf.load()
            self.buffers[address] = buf
        return buf

    def has_messages(self, address: str) -> bool:
        buf = self.buffers.get(address)
        return buf is not None and len(buf) > 0

    def load_all(self) -> None:
        """Cold-start scan: rehydrate every persisted buffer in this store."""
        if not self.root.exists():
            return
        for path in sorted(self.root.glob("*.jsonl")):
            buf = PersistentDeque(path, self.maxlen)
            buf.load()
            if not buf.deque:
                path.unlink(missing_ok=True)
                continue
            address = buf.deque[0].to
            self.buffers.setdefault(address, buf)

    def active_addresses(self) -> list[str]:
        return [a for a, buf in self.buffers.items() if len(buf) > 0]


class Persistence:
    """Top-level container: DM queues + topic ring buffers."""

    DEFAULT_DM_MAXLEN = 100
    DEFAULT_TOPIC_MAXLEN = 1000

    def __init__(
        self,
        state_dir: Path,
        dm_maxlen: int = DEFAULT_DM_MAXLEN,
        topic_maxlen: int = DEFAULT_TOPIC_MAXLEN,
    ) -> None:
        self.state_dir = state_dir
        self.dms = AddressableStore(state_dir / "dms", dm_maxlen)
        self.topics = AddressableStore(state_dir / "topics", topic_maxlen)

    def load_all(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.dms.load_all()
        self.topics.load_all()


def default_state_dir() -> Path:
    """Resolve ``$XDG_STATE_HOME/blemees/peerd`` per the XDG spec.

    The ``blemees`` parent dir is shared across the suite
    (``blemees-agentd``, ``blemees-peerd``, ``blemees-tui``) so all
    components keep their state under one user-visible folder.
    """
    xdg = os.environ.get("XDG_STATE_HOME")
    base = Path(xdg) if xdg else Path.home() / ".local" / "state"
    return base / "blemees" / "peerd"


def default_socket_path() -> Path:
    """Resolve the conventional blemees-peerd socket path.

    Returns ``$XDG_RUNTIME_DIR/blemees/peerd.sock`` when XDG_RUNTIME_DIR
    is set, falling back to ``/tmp/blemees-peerd-<uid>.sock``. Both
    ``blemees-peerd`` and any client resolve in this order.
    """
    xdg = os.environ.get("XDG_RUNTIME_DIR")
    if xdg and os.path.isdir(xdg):
        return Path(xdg) / "blemees" / "peerd.sock"
    return Path(f"/tmp/blemees-peerd-{os.getuid()}.sock")
