import pytest

from storage_core.log.entry import HEADER_SIZE, CorruptEntryError, EntryType, LogEntry


def test_encode_decode_round_trip() -> None:
    entry = LogEntry.create(EntryType.SET, key=b"hello", value=b"world")
    encoded = entry.encode()

    header, key, value = (
        encoded[:HEADER_SIZE],
        encoded[HEADER_SIZE : HEADER_SIZE + 5],
        encoded[HEADER_SIZE + 5 :],
    )
    decoded = LogEntry.decode(header, key, value)

    assert decoded.entry_type is EntryType.SET
    assert decoded.key == b"hello"
    assert decoded.value == b"world"
    assert decoded.timestamp_ms == entry.timestamp_ms


def test_delete_entry_has_empty_value() -> None:
    entry = LogEntry.create(EntryType.DELETE, key=b"gone")
    assert entry.value == b""
    assert entry.total_size == HEADER_SIZE + len(b"gone")


def test_total_size_matches_encoded_length() -> None:
    entry = LogEntry.create(EntryType.SET, key=b"k", value=b"v" * 100)
    assert entry.total_size == len(entry.encode())


def test_corrupt_checksum_is_detected() -> None:
    entry = LogEntry.create(EntryType.SET, key=b"k", value=b"v")
    encoded = bytearray(entry.encode())
    # Flip a byte in the value payload without touching lengths.
    encoded[-1] ^= 0xFF

    header = bytes(encoded[:HEADER_SIZE])
    key = bytes(encoded[HEADER_SIZE : HEADER_SIZE + 1])
    value = bytes(encoded[HEADER_SIZE + 1 :])

    with pytest.raises(CorruptEntryError, match="crc32 mismatch"):
        LogEntry.decode(header, key, value)


def test_length_mismatch_is_detected() -> None:
    entry = LogEntry.create(EntryType.SET, key=b"k", value=b"v")
    header = entry.encode()[:HEADER_SIZE]

    with pytest.raises(CorruptEntryError, match="length mismatch"):
        LogEntry.decode(header, key=b"wrong-length-key", value=b"v")


def test_short_header_is_rejected() -> None:
    with pytest.raises(CorruptEntryError, match="expected"):
        LogEntry.decode_header(b"too-short")


def test_unknown_entry_type_is_rejected() -> None:
    import struct

    from storage_core.log.entry import HEADER_FORMAT
    from storage_core.log.checksum import compute_crc32

    bogus_type = 99
    crc = compute_crc32(bogus_type, b"k", b"v")
    header = struct.pack(HEADER_FORMAT, 1, bogus_type, 1, 1, 0, crc)

    with pytest.raises(CorruptEntryError, match="unknown entry_type"):
        LogEntry.decode(header, b"k", b"v")