import os
from pathlib import Path

from storage_core.kv.recovery import replay
from storage_core.kv.store import KVStore


def test_replay_empty_log_returns_empty_state(tmp_path: Path) -> None:
    log_path = tmp_path / "test.log"
    state = replay(log_path)
    assert state == {}


def test_replay_rebuilds_state_from_sets_and_deletes(tmp_path: Path) -> None:
    log_path = tmp_path / "test.log"

    with KVStore(log_path, fsync_every_write=False) as store:
        store.set(b"a", b"1")
        store.set(b"b", b"2")
        store.set(b"a", b"1-updated")
        store.delete(b"b")

    state = replay(log_path)
    assert state == {b"a": b"1-updated"}


def test_store_reopens_with_state_intact(tmp_path: Path) -> None:
    log_path = tmp_path / "test.log"

    with KVStore(log_path, fsync_every_write=False) as store:
        store.set(b"x", b"one")
        store.set(b"y", b"two")

    with KVStore(log_path, fsync_every_write=False) as reopened:
        assert reopened.get(b"x") == b"one"
        assert reopened.get(b"y") == b"two"
        assert len(reopened) == 2


def test_recovery_truncates_torn_write(tmp_path: Path) -> None:
    log_path = tmp_path / "test.log"

    with KVStore(log_path, fsync_every_write=False) as store:
        store.set(b"good", b"entry")

    clean_size = os.path.getsize(log_path)

    # Simulate a crash mid-append by appending a garbage tail.
    with open(log_path, "ab") as f:
        f.write(b"\x01\x01\xff\xff\xff\x7f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")

    assert os.path.getsize(log_path) > clean_size

    state = replay(log_path)
    assert state == {b"good": b"entry"}
    # File should have been truncated back to the clean prefix.
    assert os.path.getsize(log_path) == clean_size


def test_reopening_after_torn_write_yields_correct_store(tmp_path: Path) -> None:
    log_path = tmp_path / "test.log"

    with KVStore(log_path, fsync_every_write=False) as store:
        store.set(b"a", b"1")

    with open(log_path, "ab") as f:
        f.write(b"\x01\x01\xff\xff\xff\x7f\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")

    with KVStore(log_path, fsync_every_write=False) as store:
        assert store.get(b"a") == b"1"
        store.set(b"b", b"2")

    with KVStore(log_path, fsync_every_write=False) as store:
        assert store.get(b"a") == b"1"
        assert store.get(b"b") == b"2"
