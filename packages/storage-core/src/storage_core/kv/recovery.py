"""Crash recovery: replay the WAL to rebuild in-memory state.

See docs/log-format.md "Recovery / corruption handling" for the contract
this implements: the log is prefix-durable, so replay walks entries until
it hits a torn write, then truncates the file back to the last valid entry
before handing control back to a fresh LogWriter.
"""

from __future__ import annotations

import os
from pathlib import Path

from storage_core.log.entry import EntryType
from storage_core.log.reader import LogReader


def replay(path: str | Path) -> dict[bytes, bytes]:
    """Replay the WAL at `path`, rebuilding the in-memory key -> value state.

    Also truncates the file on disk back to its last valid entry, discarding
    any torn write left over from a crash mid-append, so the LogWriter that
    opens the file next starts from a clean, fully-valid tail.
    """
    reader = LogReader(path)
    state: dict[bytes, bytes] = {}

    valid_end = 0
    for offset, entry in reader.iterate_from(0):
        if entry.entry_type is EntryType.SET:
            state[entry.key] = entry.value
        elif entry.entry_type is EntryType.DELETE:
            state.pop(entry.key, None)
        # SNAPSHOT_MARKER carries no key/value change; it's just a boundary.
        valid_end = offset + entry.total_size

    actual_size = os.path.getsize(path)
    if actual_size > valid_end:
        # Torn write from a crash mid-append -- discard the incomplete tail.
        with open(path, "r+b") as f:
            f.truncate(valid_end)

    return state