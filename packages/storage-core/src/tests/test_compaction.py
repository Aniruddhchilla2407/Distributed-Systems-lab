import os
from pathlib import Path

from storage_core.kv.store import KVStore


def test_compaction_preserves_current_state(tmp_path: Path) -> None:
    log_path = tmp_path / "test.log"

    with KVStore(log_path, fsync_every_write=False) as store:
        store.set(b"a", b"1")
        store.set(b"b", b"2")
        store.set(b"a", b"1-updated")
        store.delete(b"b")
        store.set(b"c", b"3")

        store.compact()

        assert store.get(b"a") == b"1-updated"
        assert store.get(b"b") is None
        assert store.get(b"c") == b"3"
        assert len(store) == 2


def test_compaction_shrinks_log_size(tmp_path: Path) -> None:
    log_path = tmp_path / "test.log"

    with KVStore(log_path, fsync_every_write=False) as store:
        for i in range(50):
            store.set(b"same-key", f"value-{i}".encode())
        pre_compact_size = os.path.getsize(log_path)

        store.compact()
        post_compact_size = os.path.getsize(log_path)

    assert post_compact_size < pre_compact_size


def test_state_survives_compaction_and_reopen(tmp_path: Path) -> None:
    log_path = tmp_path / "test.log"

    with KVStore(log_path, fsync_every_write=False) as store:
        store.set(b"a", b"1")
        store.set(b"b", b"2")
        store.delete(b"a")
        store.compact()

    with KVStore(log_path, fsync_every_write=False) as reopened:
        assert reopened.get(b"a") is None
        assert reopened.get(b"b") == b"2"
        assert len(reopened) == 1


def test_writes_after_compaction_are_durable(tmp_path: Path) -> None:
    log_path = tmp_path / "test.log"

    with KVStore(log_path, fsync_every_write=False) as store:
        store.set(b"a", b"1")
        store.compact()
        store.set(b"d", b"4")

    with KVStore(log_path, fsync_every_write=False) as reopened:
        assert reopened.get(b"a") == b"1"
        assert reopened.get(b"d") == b"4"