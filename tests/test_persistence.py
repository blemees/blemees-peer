"""Persistence layer: PersistentDeque, AddressableStore, cold start."""

from __future__ import annotations

from pathlib import Path

from blemees_peer.messages import Message, mint_message_id, now_ts
from blemees_peer.persistence import (
    AddressableStore,
    Persistence,
    PersistentDeque,
    hash_address,
)


def _make_msg(to: str = "home:~/foo", body: str = "hi") -> Message:
    return Message(
        message_id=mint_message_id(),
        from_addr="home:~/bar#sess_AAAAAAAAAAAAAAAA",
        from_alias=None,
        to=to,
        body=body,
        reply_to=None,
        ts=now_ts(),
    )


def test_persistent_deque_append_and_load(tmp_path: Path) -> None:
    path = tmp_path / "x.jsonl"
    buf = PersistentDeque(path, maxlen=5)
    msg1 = _make_msg(body="one")
    msg2 = _make_msg(body="two")
    buf.append(msg1)
    buf.append(msg2)
    assert path.exists()

    fresh = PersistentDeque(path, maxlen=5)
    fresh.load()
    assert [m.body for m in fresh.deque] == ["one", "two"]


def test_persistent_deque_bounded(tmp_path: Path) -> None:
    path = tmp_path / "x.jsonl"
    buf = PersistentDeque(path, maxlen=3)
    for i in range(5):
        buf.append(_make_msg(body=f"m{i}"))
    assert [m.body for m in buf.deque] == ["m2", "m3", "m4"]

    fresh = PersistentDeque(path, maxlen=3)
    fresh.load()
    assert [m.body for m in fresh.deque] == ["m2", "m3", "m4"]


def test_persistent_deque_drain_removes_file(tmp_path: Path) -> None:
    path = tmp_path / "x.jsonl"
    buf = PersistentDeque(path, maxlen=10)
    buf.append(_make_msg())
    drained = buf.drain()
    assert len(drained) == 1
    assert not path.exists()
    assert len(buf) == 0


def test_persistent_deque_history_since(tmp_path: Path) -> None:
    path = tmp_path / "x.jsonl"
    buf = PersistentDeque(path, maxlen=10)
    msgs = [_make_msg(body=f"m{i}") for i in range(5)]
    for m in msgs:
        buf.append(m)
    cursor = msgs[1].message_id
    out = buf.history(since=cursor, limit=10)
    assert [m.body for m in out] == ["m2", "m3", "m4"]


def test_persistent_deque_history_unknown_since_returns_tail(tmp_path: Path) -> None:
    path = tmp_path / "x.jsonl"
    buf = PersistentDeque(path, maxlen=10)
    for i in range(5):
        buf.append(_make_msg(body=f"m{i}"))
    out = buf.history(since="msg_does_not_exist", limit=2)
    assert [m.body for m in out] == ["m3", "m4"]


def test_addressable_store_load_all(tmp_path: Path) -> None:
    root = tmp_path / "dms"
    root.mkdir()
    addr = "home:~/proj"
    msg = _make_msg(to=addr)
    # Manually create one buffer
    store = AddressableStore(root, maxlen=10)
    store.get(addr).append(msg)

    # Reload from scratch
    store2 = AddressableStore(root, maxlen=10)
    store2.load_all()
    assert addr in store2.buffers
    assert [m.body for m in store2.buffers[addr]] == ["hi"]


def test_addressable_store_skips_empty_files(tmp_path: Path) -> None:
    root = tmp_path / "dms"
    root.mkdir()
    # Create a stray empty file
    (root / "deadbeef.jsonl").write_text("")
    store = AddressableStore(root, maxlen=10)
    store.load_all()
    assert store.buffers == {}
    # Empty file should be removed
    assert not (root / "deadbeef.jsonl").exists()


def test_persistence_top_level_load(tmp_path: Path) -> None:
    p = Persistence(tmp_path / "state")
    msg_dm = _make_msg(to="home:~/foo")
    msg_topic = _make_msg(to="topic:build")
    p.dms.get("home:~/foo").append(msg_dm)
    p.topics.get("topic:build").append(msg_topic)

    p2 = Persistence(tmp_path / "state")
    p2.load_all()
    assert "home:~/foo" in p2.dms.buffers
    assert "topic:build" in p2.topics.buffers


def test_hash_address_is_stable() -> None:
    a = hash_address("home:~/foo")
    b = hash_address("home:~/foo")
    c = hash_address("home:~/bar")
    assert a == b
    assert a != c
    assert len(a) == 16
