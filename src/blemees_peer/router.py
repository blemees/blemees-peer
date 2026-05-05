"""In-memory routing logic.

The ``Router`` owns the live registry of peers, aliases, and topic
subscriptions. It is the only thing that knows how to turn a
``peer.send`` or ``peer.publish`` into actual notifications going
out to the right ``Connection``s, and it delegates persistence to
the ``Persistence`` layer.

The router is single-threaded by design (it runs inside an asyncio
event loop) so it does not lock its dictionaries — handlers may
``await`` on outbox writes, but state mutations themselves are
synchronous.
"""

from __future__ import annotations

import logging
from typing import Any

from blemees_peer.addressing import (
    ALIAS_PATTERN,
    Address,
    HomeAddress,
    InvalidAddressError,
    TopicAddress,
    is_valid_alias,
    parse_address,
)
from blemees_peer.connection import Connection
from blemees_peer.errors import (
    ALIAS_TAKEN,
    INVALID_PARAMS,
    SID_IN_USE,
    PeerError,
)
from blemees_peer.messages import Message, mint_message_id, now_ts
from blemees_peer.persistence import Persistence

log = logging.getLogger("blemees-peerd.router")


class Router:
    """Authoritative in-memory state for a running ``blemees-peerd``."""

    def __init__(self, persistence: Persistence) -> None:
        self.persistence = persistence
        self.connections: dict[str, Connection] = {}  # sid -> Connection
        self.by_home: dict[str, set[str]] = {}  # home -> {sid, ...}
        self.aliases_by_home: dict[str, dict[str, str]] = {}  # home -> {alias: sid}
        self.topic_subs: dict[str, set[str]] = {}  # topic -> {sid, ...}

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------

    async def hello(self, conn: Connection, sid: str, home: str) -> dict[str, Any]:
        if sid in self.connections:
            raise PeerError(SID_IN_USE, f"sid {sid!r} already in use")
        conn.sid = sid
        conn.home = home
        self.connections[sid] = conn
        self.by_home.setdefault(home, set()).add(sid)

        # Drain any DMs queued for this peer's broadcast or sid address
        await self._drain_dm_queue(conn, f"home:{home}")
        await self._drain_dm_queue(conn, f"home:{home}#{sid}")

        await self._broadcast(
            "peer.peer_joined",
            {
                "peer_id": f"home:{home}",
                "sid": sid,
                "alias": None,
                "ts": now_ts(),
            },
            exclude_sid=sid,
        )
        return {
            "peer_id": f"home:{home}",
            "sid": sid,
            "server_version": _server_version(),
        }

    async def goodbye(self, conn: Connection) -> None:
        if conn.sid is None:
            return
        sid = conn.sid
        home = conn.home
        previous_alias = conn.alias

        self.connections.pop(sid, None)
        if home is not None:
            sids = self.by_home.get(home)
            if sids is not None:
                sids.discard(sid)
                if not sids:
                    self.by_home.pop(home, None)
            if previous_alias is not None:
                aliases = self.aliases_by_home.get(home)
                if aliases is not None and aliases.get(previous_alias) == sid:
                    aliases.pop(previous_alias, None)
                    if not aliases:
                        self.aliases_by_home.pop(home, None)

        for topic in list(conn.subscriptions):
            subs = self.topic_subs.get(topic)
            if subs is not None:
                subs.discard(sid)
                if not subs:
                    self.topic_subs.pop(topic, None)
        conn.subscriptions.clear()

        if home is not None:
            await self._broadcast(
                "peer.peer_left",
                {
                    "peer_id": f"home:{home}",
                    "sid": sid,
                    "alias": previous_alias,
                    "ts": now_ts(),
                },
                exclude_sid=sid,
            )

    # ------------------------------------------------------------------
    # Alias management
    # ------------------------------------------------------------------

    async def set_alias(self, conn: Connection, alias: str) -> dict[str, Any]:
        _require_hello(conn)
        assert conn.sid is not None and conn.home is not None
        previous = conn.alias
        aliases = self.aliases_by_home.setdefault(conn.home, {})

        if alias == "":
            new_alias: str | None = None
        else:
            if not is_valid_alias(alias):
                raise PeerError(
                    INVALID_PARAMS,
                    f"alias must match {ALIAS_PATTERN.pattern!r} and not start with 'sess_'",
                )
            owner = aliases.get(alias)
            if owner is not None and owner != conn.sid:
                raise PeerError(ALIAS_TAKEN, f"alias {alias!r} already in use")
            new_alias = alias

        # Release previous alias for this connection
        if previous is not None and aliases.get(previous) == conn.sid:
            aliases.pop(previous, None)
        if new_alias is not None:
            aliases[new_alias] = conn.sid
        if not aliases:
            self.aliases_by_home.pop(conn.home, None)
        conn.alias = new_alias

        # If we just claimed a new alias, drain any DMs queued for it
        if new_alias is not None and new_alias != previous:
            await self._drain_dm_queue(conn, f"home:{conn.home}#{new_alias}")

        await self._broadcast(
            "peer.alias_changed",
            {
                "peer_id": f"home:{conn.home}",
                "sid": conn.sid,
                "alias": new_alias,
                "previous": previous,
                "ts": now_ts(),
            },
        )
        return {"ok": True, "alias": new_alias}

    # ------------------------------------------------------------------
    # DM filter
    # ------------------------------------------------------------------

    async def set_dm_filter(
        self,
        conn: Connection,
        patterns: list[str],
    ) -> dict[str, Any]:
        _require_hello(conn)
        if not isinstance(patterns, list) or not all(isinstance(p, str) for p in patterns):
            raise PeerError(INVALID_PARAMS, "patterns must be a list of strings")
        conn.dm_filter = list(patterns)
        return {"ok": True, "patterns": list(patterns)}

    # ------------------------------------------------------------------
    # DMs
    # ------------------------------------------------------------------

    async def send(
        self,
        conn: Connection,
        to: str,
        body: Any,
        reply_to: str | None,
    ) -> dict[str, Any]:
        _require_hello(conn)
        try:
            addr = parse_address(to)
        except InvalidAddressError as e:
            raise PeerError(INVALID_PARAMS, str(e)) from None
        if not isinstance(addr, HomeAddress):
            raise PeerError(INVALID_PARAMS, "peer.send target must be a home: address")

        msg = Message(
            message_id=mint_message_id(),
            from_addr=conn.from_addr,
            from_alias=conn.alias,
            to=to,
            body=body,
            reply_to=reply_to,
        )

        recipients = self._resolve_dm_recipients(addr)
        delivered = 0
        for sid in recipients:
            if sid == conn.sid:
                continue
            target = self.connections.get(sid)
            if target is None:
                continue
            if not target.matches_dm_filter(msg.from_addr):
                continue
            await target.send_notification("peer.message", msg.to_wire())
            delivered += 1

        if delivered == 0:
            self.persistence.dms.get(to).append(msg)

        return {"message_id": msg.message_id, "delivered": delivered}

    def _resolve_dm_recipients(self, addr: HomeAddress) -> list[str]:
        if addr.discriminator is None:
            return list(self.by_home.get(addr.home, set()))
        # Try alias first (alias namespace is disjoint from sid namespace
        # by validation rules), then fall back to sid lookup.
        aliases = self.aliases_by_home.get(addr.home, {})
        sid = aliases.get(addr.discriminator)
        if sid is not None:
            return [sid]
        if addr.discriminator in self.connections:
            owner = self.connections[addr.discriminator]
            if owner.home == addr.home:
                return [addr.discriminator]
        return []

    async def _drain_dm_queue(self, conn: Connection, address: str) -> None:
        if not self.persistence.dms.has_messages(address):
            return
        buf = self.persistence.dms.get(address)
        msgs = buf.drain()
        for msg in msgs:
            if not conn.matches_dm_filter(msg.from_addr):
                continue
            await conn.send_notification("peer.message", msg.to_wire())

    # ------------------------------------------------------------------
    # Topics
    # ------------------------------------------------------------------

    async def publish(
        self,
        conn: Connection,
        topic: str,
        body: Any,
    ) -> dict[str, Any]:
        _require_hello(conn)
        topic = _validate_topic(topic)
        full_addr = f"topic:{topic}"
        msg = Message(
            message_id=mint_message_id(),
            from_addr=conn.from_addr,
            from_alias=conn.alias,
            to=full_addr,
            body=body,
            reply_to=None,
        )
        # Always persist to the ring buffer, even if no subscribers
        self.persistence.topics.get(full_addr).append(msg)

        delivered = 0
        for sid in list(self.topic_subs.get(topic, set())):
            if sid == conn.sid:
                continue
            target = self.connections.get(sid)
            if target is None:
                continue
            await target.send_notification("peer.message", msg.to_wire())
            delivered += 1
        return {"message_id": msg.message_id, "subscribers": delivered}

    async def subscribe(
        self,
        conn: Connection,
        topic: str,
        replay: int = 0,
    ) -> dict[str, Any]:
        _require_hello(conn)
        topic = _validate_topic(topic)
        if not isinstance(replay, int) or replay < 0:
            raise PeerError(INVALID_PARAMS, "replay must be a non-negative integer")
        assert conn.sid is not None
        self.topic_subs.setdefault(topic, set()).add(conn.sid)
        conn.subscriptions.add(topic)

        if replay > 0:
            full_addr = f"topic:{topic}"
            buf = self.persistence.topics.get(full_addr)
            history = list(buf)[-replay:]
            for msg in history:
                await conn.send_notification("peer.message", msg.to_wire())

        return {"topic": topic, "ok": True}

    async def unsubscribe(self, conn: Connection, topic: str) -> dict[str, Any]:
        _require_hello(conn)
        topic = _validate_topic(topic)
        assert conn.sid is not None
        subs = self.topic_subs.get(topic)
        if subs is not None:
            subs.discard(conn.sid)
            if not subs:
                self.topic_subs.pop(topic, None)
        conn.subscriptions.discard(topic)
        return {"ok": True}

    # ------------------------------------------------------------------
    # Discovery / history
    # ------------------------------------------------------------------

    async def list_peers(self, conn: Connection) -> dict[str, Any]:
        _require_hello(conn)
        peers: list[dict[str, Any]] = []
        for home, sids in self.by_home.items():
            sessions = []
            for sid in sids:
                c = self.connections.get(sid)
                if c is None:
                    continue
                sessions.append(
                    {
                        "sid": sid,
                        "alias": c.alias,
                        "subscriptions": sorted(c.subscriptions),
                    }
                )
            if sessions:
                peers.append({"peer_id": f"home:{home}", "sessions": sessions})
        return {"peers": peers}

    async def list_topics(self, conn: Connection) -> dict[str, Any]:
        _require_hello(conn)
        topics: list[dict[str, Any]] = []
        all_topics = set(self.topic_subs.keys())
        for full_addr in self.persistence.topics.active_addresses():
            if full_addr.startswith("topic:"):
                all_topics.add(full_addr[len("topic:") :])
        for name in sorted(all_topics):
            full_addr = f"topic:{name}"
            buf = self.persistence.topics.buffers.get(full_addr)
            last_ts = None
            if buf is not None and buf.deque:
                last_ts = buf.deque[-1].ts
            topics.append(
                {
                    "name": name,
                    "subscribers": len(self.topic_subs.get(name, set())),
                    "last_message_at": last_ts,
                }
            )
        return {"topics": topics}

    async def history(
        self,
        conn: Connection,
        address: str,
        since: str | None,
        limit: int,
    ) -> dict[str, Any]:
        _require_hello(conn)
        if not isinstance(limit, int) or limit <= 0 or limit > 1000:
            raise PeerError(INVALID_PARAMS, "limit must be a positive integer ≤ 1000")
        try:
            addr = parse_address(address)
        except InvalidAddressError as e:
            raise PeerError(INVALID_PARAMS, str(e)) from None
        full_addr = address
        buf = None
        if isinstance(addr, TopicAddress):
            buf = self.persistence.topics.buffers.get(full_addr)
        else:
            buf = self.persistence.dms.buffers.get(full_addr)
        if buf is None:
            return {"messages": []}
        return {"messages": [m.to_wire() for m in buf.history(since, limit)]}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _broadcast(
        self,
        method: str,
        params: dict[str, Any],
        exclude_sid: str | None = None,
    ) -> None:
        for sid, conn in list(self.connections.items()):
            if exclude_sid is not None and sid == exclude_sid:
                continue
            if not conn.hello_complete:
                continue
            await conn.send_notification(method, params)


def _validate_topic(topic: str) -> str:
    if not isinstance(topic, str) or not topic:
        raise PeerError(INVALID_PARAMS, "topic must be a non-empty string")
    if topic.startswith("/") or "#" in topic:
        raise PeerError(INVALID_PARAMS, "topic must not start with '/' or contain '#'")
    if len(topic.encode("utf-8")) > 256:
        raise PeerError(INVALID_PARAMS, "topic name must be ≤ 256 bytes UTF-8")
    return topic


def _require_hello(conn: Connection) -> None:
    if not conn.hello_complete:
        raise PeerError(-32000, "peer.hello must be the first method on a connection")


def _server_version() -> str:
    from blemees_peer._version import __version__

    return __version__


# Re-exported aliases for tests / introspection
__all__ = ["Address", "Router"]
