"""Append-only log writer.

Owns the single file handle that entries are appended to. Not thread-safe
by design — callers that need concurrent writers should serialize through
a lock (KVStore does this, with optional batching -- see kv/store.py)
rather than this class trying to guess a locking strategy.
"""

from __future__ import annotations

import os
from pathlib import Path

from storage_core.log.entry import LogEntry


class LogWriter:
    def __init__(self, path: str | Path, *, fsync_every_write: bool = True) -> None:
        """Open (or create) the log file for appending.

        fsync_every_write: if True, every append() calls os.fsync after
        writing, guaranteeing durability at the cost of throughput. Set to
        False for benchmarking raw append speed, or when a caller (like
        KVStore's batching mode) manages fsync timing itself via fsync().
        """
        self.path = Path(path)
        self.fsync_every_write = fsync_every_write
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # 'ab' = append, create if missing, never truncate existing content.
        self._file = open(self.path, "ab", buffering=0)

    def append(self, entry: LogEntry, *, fsync: bool | None = None) -> int:
        """Write one entry to the end of the log.

        fsync: overrides self.fsync_every_write for this call if given.
        Pass fsync=False to buffer the write without syncing (used by
        KVStore's batched-write path, which fsyncs once for many entries).

        Returns the byte offset at which this entry starts.
        """
        offset = self._file.tell()
        data = entry.encode()
        self._file.write(data)
        should_fsync = self.fsync_every_write if fsync is None else fsync
        if should_fsync:
            os.fsync(self._file.fileno())
        return offset

    def fsync(self) -> None:
        """Force any buffered writes to disk. Used by KVStore's background
        flush thread to sync a whole batch of appended entries at once."""
        os.fsync(self._file.fileno())

    def tell(self) -> int:
        """Current end-of-file byte offset (where the next entry would land)."""
        return self._file.tell()

    def truncate(self, offset: int) -> None:
        """Truncate the log to `offset` bytes. Used by recovery to discard
        a torn write left over from a crash mid-append."""
        self._file.truncate(offset)
        self._file.seek(offset)

    def close(self) -> None:
        self._file.close()

    def __enter__(self) -> "LogWriter":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()