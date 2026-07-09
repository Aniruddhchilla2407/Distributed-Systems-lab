"""WAL-backed key-value store.

Two write modes, controlled by `batch_window_ms`:

- batch_window_ms=None (default): every set/delete synchronously appends
  and (if fsync_every_write) fsyncs before returning -- one disk trip per
  write. Simple, but under concurrent load each writer queues behind the
  previous one's fsync.

- batch_window_ms=N: a background thread fsyncs at most once every N
  milliseconds (or immediately once `batch_max_size` writes have piled
  up, whichever comes first), covering every write that arrived in that
  window with a single fsync. Callers still block until their specific
  write is confirmed durable -- they just now share the disk trip with
  whoever else was waiting in the same window. This trades a small,
  bounded amount of added latency (up to batch_window_ms) for much
  higher throughput under concurrent writers, without weakening the
  durability guarantee: a write is never acknowledged until it's synced.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path

from storage_core.kv.compaction import compact
from storage_core.kv.recovery import replay
from storage_core.log.entry import EntryType, LogEntry
from storage_core.log.writer import LogWriter


class KVStore:
    def __init__(
        self,
        path: str | Path,
        *,
        fsync_every_write: bool = True,
        batch_window_ms: float | None = None,
        batch_max_size: int = 100,
    ) -> None:
        self.path = Path(path)
        self._fsync_every_write = fsync_every_write
        self._batch_window_ms = batch_window_ms
        self._batch_max_size = batch_max_size

        self._cv = threading.Condition()
        self._state = replay(self.path)
        # In batching mode, LogWriter itself never auto-fsyncs -- the
        # KVStore's flush loop / batch logic below controls fsync timing.
        self._writer = LogWriter(
            self.path, fsync_every_write=fsync_every_write and batch_window_ms is None
        )

        self._appended_count = 0
        self._flushed_count = 0
        self._closed = False
        self._flush_thread: threading.Thread | None = None

        if batch_window_ms is not None:
            self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
            self._flush_thread.start()

    def _flush_loop(self) -> None:
        assert self._batch_window_ms is not None
        interval = self._batch_window_ms / 1000
        while True:
            time.sleep(interval)
            with self._cv:
                if self._closed:
                    return
                if self._appended_count > self._flushed_count:
                    self._writer.fsync()
                    self._flushed_count = self._appended_count
                    self._cv.notify_all()

    def get(self, key: bytes) -> bytes | None:
        with self._cv:
            return self._state.get(key)

    def set(self, key: bytes, value: bytes) -> None:
        entry = LogEntry.create(EntryType.SET, key, value)
        self._write_and_wait(entry, key, value, delete=False)

    def delete(self, key: bytes) -> bool:
        """Delete `key` if present. Returns True if it existed."""
        with self._cv:
            if key not in self._state:
                return False
        entry = LogEntry.create(EntryType.DELETE, key)
        self._write_and_wait(entry, key, b"", delete=True)
        return True

    def _write_and_wait(self, entry: LogEntry, key: bytes, value: bytes, delete: bool) -> None:
        with self._cv:
            if self._batch_window_ms is None:
                # Synchronous path: append (and fsync, if enabled) inline,
                # same behavior as before batching existed.
                self._writer.append(entry)
                if delete:
                    self._state.pop(key, None)
                else:
                    self._state[key] = value
                return

            # Batched path: buffer the write and update in-memory state
            # immediately (readers see it right away), then wait for the
            # background flush thread (or a full batch) to fsync it.
            self._writer.append(entry, fsync=False)
            if delete:
                self._state.pop(key, None)
            else:
                self._state[key] = value

            self._appended_count += 1
            my_seq = self._appended_count

            if not self._fsync_every_write:
                # Durability not requested -- don't make the caller wait.
                return

            if self._appended_count - self._flushed_count >= self._batch_max_size:
                # Batch is already full -- flush now instead of waiting
                # for the timer, so a burst doesn't sit idle.
                self._writer.fsync()
                self._flushed_count = self._appended_count
                self._cv.notify_all()
                return

            while self._flushed_count < my_seq and not self._closed:
                self._cv.wait()

    def __contains__(self, key: bytes) -> bool:
        with self._cv:
            return key in self._state

    def keys(self) -> list[bytes]:
        with self._cv:
            return list(self._state.keys())

    def __len__(self) -> int:
        with self._cv:
            return len(self._state)

    def compact(self) -> None:
        """Rewrite the WAL to only contain live keys. Flushes any pending
        batched writes first, then blocks concurrent writes for the
        duration, since the writer's file handle gets swapped."""
        with self._cv:
            if self._appended_count > self._flushed_count:
                self._writer.fsync()
                self._flushed_count = self._appended_count
                self._cv.notify_all()

            self._writer.close()
            compact(self.path, dict(self._state))
            self._writer = LogWriter(
                self.path,
                fsync_every_write=self._fsync_every_write and self._batch_window_ms is None,
            )
            self._appended_count = 0
            self._flushed_count = 0

    def close(self) -> None:
        with self._cv:
            if self._appended_count > self._flushed_count:
                self._writer.fsync()
                self._flushed_count = self._appended_count
            self._closed = True
            self._cv.notify_all()

        if self._flush_thread is not None:
            self._flush_thread.join(timeout=1.0)

        with self._cv:
            self._writer.close()

    def __enter__(self) -> KVStore:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
