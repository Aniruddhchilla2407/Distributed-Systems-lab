from pathlib import Path

from broker_core.partition import Partition


def test_produce_and_read_from_start(tmp_path: Path) -> None:
    partition = Partition(tmp_path / "p0.log", partition_id=0, fsync_every_write=False)
    partition.produce(b"key1", b"value1")
    partition.produce(b"key2", b"value2")

    records = list(partition.read_from(0))
    assert len(records) == 2
    assert records[0].key == b"key1"
    assert records[0].value == b"value1"
    assert records[0].partition == 0
    assert records[1].key == b"key2"


def test_produce_without_key(tmp_path: Path) -> None:
    partition = Partition(tmp_path / "p0.log", partition_id=0, fsync_every_write=False)
    partition.produce(None, b"unkeyed-value")

    records = list(partition.read_from(0))
    assert records[0].key is None
    assert records[0].value == b"unkeyed-value"


def test_read_at_specific_offset(tmp_path: Path) -> None:
    partition = Partition(tmp_path / "p0.log", partition_id=0, fsync_every_write=False)
    partition.produce(b"k1", b"v1")
    second_offset = partition.produce(b"k2", b"v2")

    record = partition.read_at(second_offset)
    assert record.key == b"k2"
    assert record.value == b"v2"


def test_end_offset_advances_with_each_produce(tmp_path: Path) -> None:
    partition = Partition(tmp_path / "p0.log", partition_id=0, fsync_every_write=False)
    start = partition.end_offset()
    partition.produce(b"k", b"v")
    end = partition.end_offset()
    assert end > start


def test_next_offset_matches_following_entry_offset(tmp_path: Path) -> None:
    partition = Partition(tmp_path / "p0.log", partition_id=0, fsync_every_write=False)
    partition.produce(b"a", b"1")
    partition.produce(b"b", b"2")

    records = list(partition.read_from(0))
    assert records[0].next_offset == records[1].offset
