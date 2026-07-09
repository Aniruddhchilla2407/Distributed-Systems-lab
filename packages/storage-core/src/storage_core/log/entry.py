"""Fixed-header log entry format.

See docs/log-format.md for the full byte-layout spec. This module is the
single source of truth for encoding/decoding — every other package
(kv-server, broker-core, bench-cli) goes through this, never hand-rolls
struct packing itself.
"""

from __future__ import annotations

import struct
import time
from dataclasses import dataclass
from enum import IntEnum

from storage_core.log.checksum import compute_crc32, verify_crc32

# "<BBIIQI"
#   B  version      (1 byte,  uint8)
#   B  entry_type   (1 byte,  uint8)
#   I  key_len      (4 bytes, uint32)
#   I  value_len    (4 bytes, uint32)
#   Q  timestamp_ms (8 bytes, uint64)
#   I  crc32        (4 bytes, uint32)
HEADER_FORMAT = "<BBIIQI"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)  # 22 bytes

FORMAT_VERSION = 1


class EntryType(IntEnum):
    SET = 1
    DELETE = 2
    SNAPSHOT_MARKER = 3
    PRODUCE = 4


class CorruptEntryError(Exception):
    """Raised when a header or checksum fails validation during decode."""


@dataclass(frozen=True, slots=True)
class LogEntry:
    entry_type: EntryType
    key: bytes
    value: bytes
    timestamp_ms: int
    version: int = FORMAT_VERSION

    @property
    def total_size(self) -> int:
        """Total bytes this entry occupies on disk, header + payload."""
        return HEADER_SIZE + len(self.key) + len(self.value)

    @classmethod
    def create(cls, entry_type: EntryType, key: bytes, value: bytes = b"") -> LogEntry:
        """Build a new entry stamped with the current time."""
        return cls(
            entry_type=entry_type,
            key=key,
            value=value,
            timestamp_ms=int(time.time() * 1000),
        )

    def encode(self) -> bytes:
        """Serialize this entry to its on-disk byte representation."""
        crc = compute_crc32(int(self.entry_type), self.key, self.value)
        header = struct.pack(
            HEADER_FORMAT,
            self.version,
            int(self.entry_type),
            len(self.key),
            len(self.value),
            self.timestamp_ms,
            crc,
        )
        return header + self.key + self.value

    @staticmethod
    def decode_header(raw_header: bytes) -> tuple[int, int, int, int, int, int]:
        """Parse a raw 22-byte header into its fields.

        Returns (version, entry_type, key_len, value_len, timestamp_ms, crc32).
        Raises CorruptEntryError if raw_header isn't exactly HEADER_SIZE bytes.
        """
        if len(raw_header) != HEADER_SIZE:
            raise CorruptEntryError(
                f"expected {HEADER_SIZE}-byte header, got {len(raw_header)} bytes"
            )
        return struct.unpack(HEADER_FORMAT, raw_header)

    @classmethod
    def decode(cls, raw_header: bytes, key: bytes, value: bytes) -> LogEntry:
        """Parse a full entry from its header + payload bytes.

        Raises CorruptEntryError if lengths or checksum don't match.
        """
        version, entry_type_raw, key_len, value_len, timestamp_ms, crc = cls.decode_header(
            raw_header
        )

        if len(key) != key_len or len(value) != value_len:
            raise CorruptEntryError(
                f"payload length mismatch: header says key={key_len} value={value_len}, "
                f"got key={len(key)} value={len(value)}"
            )

        try:
            entry_type = EntryType(entry_type_raw)
        except ValueError as exc:
            raise CorruptEntryError(f"unknown entry_type byte: {entry_type_raw}") from exc

        if not verify_crc32(crc, entry_type_raw, key, value):
            raise CorruptEntryError("crc32 mismatch: entry payload is corrupt")

        return cls(
            entry_type=entry_type,
            key=key,
            value=value,
            timestamp_ms=timestamp_ms,
            version=version,
        )
