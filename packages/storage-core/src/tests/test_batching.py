import threading
import time
from pathlib import Path

from storage_core.kv.store import KVStore


def test_batched_writes_are_all_durable(tmp_path: Path) -> None:
    log_path = tmp_path / "test.log"

    with KVStore(log_path, fsync_every_write=True, batch_window_ms=20) as store:
        threads = [
            threading.Thread(target=store.set, args=(f"k{i}".encode(), f"v{i}".encode()))
            for i in range(50)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(store) == 50

    # Reopen and confirm every write survived -- proves batched fsyncs
    # actually made it to disk before set() returned to each caller.
    with KVStore(log_path, fsync_every_write=True, batch_window_ms=20) as reopened:
        for i in range(50):
            assert reopened.get(f"k{i}".encode()) == f"v{i}".encode()


def test_batching_flushes_full_batch_immediately(tmp_path: Path) -> None:
    log_path = tmp_path / "test.log"

    # batch_max_size=5, window is long -- a burst of 5 should flush on
    # size, not wait for the timer.
    with KVStore(
        log_path, fsync_every_write=True, batch_window_ms=5000, batch_max_size=5
    ) as store:
        start = time.monotonic()
        threads = [
            threading.Thread(target=store.set, args=(f"k{i}".encode(), b"v")) for i in range(5)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        elapsed = time.monotonic() - start

        # Should complete well under the 5-second window since the batch
        # filled up and triggered an immediate flush.
        assert elapsed < 2.0
        assert len(store) == 5


def test_non_batched_mode_still_works_as_before(tmp_path: Path) -> None:
    log_path = tmp_path / "test.log"

    with KVStore(log_path, fsync_every_write=False, batch_window_ms=None) as store:
        store.set(b"a", b"1")
        store.delete(b"a")
        store.set(b"b", b"2")

    with KVStore(log_path, fsync_every_write=False, batch_window_ms=None) as reopened:
        assert reopened.get(b"a") is None
        assert reopened.get(b"b") == b"2"


def test_batching_without_fsync_does_not_block_caller(tmp_path: Path) -> None:
    log_path = tmp_path / "test.log"

    # fsync_every_write=False + batching: writes should return immediately
    # without waiting on the flush thread at all.
    with KVStore(
        log_path, fsync_every_write=False, batch_window_ms=5000, batch_max_size=1000
    ) as store:
        start = time.monotonic()
        for i in range(20):
            store.set(f"k{i}".encode(), b"v")
        elapsed = time.monotonic() - start

        assert elapsed < 1.0
        assert len(store) == 20


def test_compact_flushes_pending_batched_writes_first(tmp_path: Path) -> None:
    log_path = tmp_path / "test.log"

    with KVStore(log_path, fsync_every_write=True, batch_window_ms=5000) as store:
        store.set(b"a", b"1")
        store.compact()  # should not hang waiting on its own pending write
        assert store.get(b"a") == b"1"