"""Log compaction.

Over time the WAL accumulates overwritten values and DELETE tombstones
that no longer contribute to current state. Compaction rewrites the log
to contain exactly one SET per live key -- nothing more.
"""

from __future__ import annotations

import os
from pathlib import Path

from storage_core.log.entry import EntryType, LogEntry
from storage_core.log.writer import LogWriter


def compact(path: str | Path, state: dict[bytes, bytes]) -> None:
    """Rewrite the log at `path` to contain only the entries needed to
    reconstruct `state`: one SET per live key, no tombstones, no stale
    overwritten values.

    Writes to a temp file first and atomically renames it over the
    original (os.replace is atomic on POSIX and Windows for same-volume
    renames), so a crash mid-compaction leaves the original log untouched
    rather than half-overwritten.
    """
    path = Path(path)
    tmp_path = path.with_suffix(path.suffix + ".compact.tmp")
    if tmp_path.exists():
        tmp_path.unlink()

    with LogWriter(tmp_path) as writer:
        for key, value in state.items():
            writer.append(LogEntry.create(EntryType.SET, key, value))

    os.replace(tmp_path, path)