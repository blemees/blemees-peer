"""``PeerClient`` module details not covered by the wire tests."""

from __future__ import annotations

from blemees_peer import client


def test_logger_name_matches_daemon_loggers() -> None:
    # Same "blemees-peerd.<module>" scheme as router, connection, etc.
    assert client.log.name == "blemees-peerd.client"
