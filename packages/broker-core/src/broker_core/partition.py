"""A single partition: one append-only log file, addressed by byte offset.

This is intentionally the thinnest possible layer over storage_core's log
primitives -- a partition IS a storage_core log, just with a key that can
be absent (a message key is optional, unlike a KV store's key) and using
EntryType.PRODUCE instead of SET/DELETE.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from storage_core.log.entry import EntryType, LogEntry
from storage_core.log.reader import LogReader
from storage_core.log.writer import LogWriter


@dataclass(frozen=True, slots=True)
class ConsumerRecord:
    """A single message read back from a partition."""

    partition: int
    offset: int
    next_offset: int
    key: bytes | None
    value: bytes
    timestamp_ms: int


class Partition:
    def __init__(
        self, path: str | Path, partition_id: int, *, fsync_every_write: bool = True
    ) -> None:
        self.path = Path(path)
        self.partition_id = partition_id
        self._writer = LogWriter(self.path, fsync_every_write=fsync_every_write)
        self._reader = LogReader(self.path)

    def produce(self, key: bytes | None, value: bytes) -> int:
        """Append a message to this partition. Returns its offset."""
        entry = LogEntry.create(EntryType.PRODUCE, key or b"", value)
        return self._writer.append(entry)

    def read_from(self, start_offset: int) -> Iterator[ConsumerRecord]:
        """Sequentially yield every message from start_offset to the current end."""
        for offset, entry in self._reader.iterate_from(start_offset):
            yield ConsumerRecord(
                partition=self.partition_id,
                offset=offset,
                next_offset=offset + entry.total_size,
                key=entry.key or None,
                value=entry.value,
                timestamp_ms=entry.timestamp_ms,
            )

    def read_at(self, offset: int) -> ConsumerRecord:
        entry = self._reader.read_at(offset)
        return ConsumerRecord(
            partition=self.partition_id,
            offset=offset,
            next_offset=offset + entry.total_size,
            key=entry.key or None,
            value=entry.value,
            timestamp_ms=entry.timestamp_ms,
        )

    def end_offset(self) -> int:
        """Byte offset just past the last written entry -- where the next produce() lands."""
        return self._writer.tell()

    def close(self) -> None:
        self._writer.close()

    def __enter__(self) -> "Partition":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()