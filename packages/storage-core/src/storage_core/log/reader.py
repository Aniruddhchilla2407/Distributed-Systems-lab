"""Log reader.

Two read modes:
  - iterate_from(offset): sequential scan, used by KV recovery (from 0) and
    broker consumers (from their last committed offset).
  - read_at(offset): random access to a single entry, used when a caller
    already knows exactly where an entry lives (e.g. an index lookup).

Both modes share the same corruption-handling contract described in
docs/log-format.md: a short/invalid header or a checksum mismatch is
treated as "end of valid log" (a torn write from a crash), not raised as
a fatal error, so recovery can simply stop reading rather than crash.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

from storage_core.log.entry import HEADER_SIZE, CorruptEntryError, LogEntry


class LogReader:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch(exist_ok=True)

    def iterate_from(self, start_offset: int = 0) -> Iterator[tuple[int, LogEntry]]:
        """Yield (offset, entry) pairs from start_offset to end of file.

        Stops silently (does not raise) as soon as it hits a short header,
        a length mismatch, or a checksum failure -- these all indicate a
        torn write, which is the expected shape of a crash mid-append.
        """
        with open(self.path, "rb") as f:
            f.seek(start_offset)
            offset = start_offset
            while True:
                header = f.read(HEADER_SIZE)
                if len(header) < HEADER_SIZE:
                    return  # clean EOF or torn header -- stop either way

                try:
                    _version, _etype, key_len, value_len, _ts, _crc = LogEntry.decode_header(header)
                except CorruptEntryError:
                    return

                payload = f.read(key_len + value_len)
                if len(payload) < key_len + value_len:
                    return  # torn write: header promised more than exists

                key, value = payload[:key_len], payload[key_len:]
                try:
                    entry = LogEntry.decode(header, key, value)
                except CorruptEntryError:
                    return

                yield offset, entry
                offset = f.tell()

    def read_at(self, offset: int) -> LogEntry:
        """Read a single entry known to start at `offset`.

        Raises CorruptEntryError if the entry at that offset is invalid.
        Unlike iterate_from, this is used when the caller expects a valid
        entry to exist there, so failures are raised rather than swallowed.
        """
        with open(self.path, "rb") as f:
            f.seek(offset)
            header = f.read(HEADER_SIZE)
            _version, _etype, key_len, value_len, _ts, _crc = LogEntry.decode_header(header)
            payload = f.read(key_len + value_len)
            key, value = payload[:key_len], payload[key_len:]
            return LogEntry.decode(header, key, value)

    def find_valid_prefix_end(self) -> int:
        """Return the byte offset marking the end of the valid entry prefix.

        Walks the whole log via iterate_from(0) and returns the offset just
        past the last valid entry -- i.e. where a torn write (if any) begins.
        Used by recovery to truncate the file back to a known-good state.
        """
        last_good_end = 0
        for offset, entry in self.iterate_from(0):
            last_good_end = offset + entry.total_size
        return last_good_end
