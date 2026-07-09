from pathlib import Path

from storage_core.log.entry import EntryType, LogEntry
from storage_core.log.reader import LogReader
from storage_core.log.writer import LogWriter


def test_append_and_iterate_round_trip(tmp_path: Path) -> None:
    log_path = tmp_path / "test.log"
    entries = [
        LogEntry.create(EntryType.SET, b"a", b"1"),
        LogEntry.create(EntryType.SET, b"b", b"2"),
        LogEntry.create(EntryType.DELETE, b"a"),
    ]

    with LogWriter(log_path, fsync_every_write=False) as writer:
        offsets = [writer.append(e) for e in entries]

    reader = LogReader(log_path)
    read_back = list(reader.iterate_from(0))

    assert len(read_back) == 3
    for (offset, entry), expected_offset, expected in zip(
        read_back, offsets, entries, strict=True
    ):
        assert offset == expected_offset
        assert entry.entry_type == expected.entry_type
        assert entry.key == expected.key
        assert entry.value == expected.value


def test_offsets_are_monotonically_increasing(tmp_path: Path) -> None:
    log_path = tmp_path / "test.log"
    with LogWriter(log_path, fsync_every_write=False) as writer:
        o1 = writer.append(LogEntry.create(EntryType.SET, b"k1", b"v" * 10))
        o2 = writer.append(LogEntry.create(EntryType.SET, b"k2", b"v" * 20))

    assert o1 == 0
    assert o2 > o1


def test_read_at_specific_offset(tmp_path: Path) -> None:
    log_path = tmp_path / "test.log"
    with LogWriter(log_path, fsync_every_write=False) as writer:
        writer.append(LogEntry.create(EntryType.SET, b"first", b"x"))
        second_offset = writer.append(LogEntry.create(EntryType.SET, b"second", b"y"))

    reader = LogReader(log_path)
    entry = reader.read_at(second_offset)
    assert entry.key == b"second"
    assert entry.value == b"y"


def test_iterate_from_middle_offset_skips_earlier_entries(tmp_path: Path) -> None:
    log_path = tmp_path / "test.log"
    with LogWriter(log_path, fsync_every_write=False) as writer:
        writer.append(LogEntry.create(EntryType.SET, b"first", b"x"))
        second_offset = writer.append(LogEntry.create(EntryType.SET, b"second", b"y"))

    reader = LogReader(log_path)
    remaining = list(reader.iterate_from(second_offset))
    assert len(remaining) == 1
    assert remaining[0][1].key == b"second"


def test_torn_write_tail_is_ignored_on_iteration(tmp_path: Path) -> None:
    log_path = tmp_path / "test.log"
    with LogWriter(log_path, fsync_every_write=False) as writer:
        writer.append(LogEntry.create(EntryType.SET, b"good", b"entry"))

    # Simulate a crash mid-append: a header claiming a large payload that
    # never actually got written.
    with open(log_path, "ab") as f:
        f.write(b"\x01\x01" + (500).to_bytes(4, "little") + (0).to_bytes(4, "little"))
        f.write((0).to_bytes(8, "little"))
        f.write((0).to_bytes(4, "little"))
        # Note: no payload bytes follow, and file ends here -- torn write.

    reader = LogReader(log_path)
    entries = list(reader.iterate_from(0))
    assert len(entries) == 1
    assert entries[0][1].key == b"good"


def test_find_valid_prefix_end_on_clean_log(tmp_path: Path) -> None:
    log_path = tmp_path / "test.log"
    with LogWriter(log_path, fsync_every_write=False) as writer:
        writer.append(LogEntry.create(EntryType.SET, b"a", b"1"))
        writer.append(LogEntry.create(EntryType.SET, b"b", b"2"))
        clean_end = writer.tell()

    reader = LogReader(log_path)
    assert reader.find_valid_prefix_end() == clean_end