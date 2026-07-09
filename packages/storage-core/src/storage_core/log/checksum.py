"""CRC32 checksum helper for log entries.

Kept as its own module so the algorithm can be swapped later (e.g. to
xxhash for speed) without touching entry.py's framing logic.
"""

from __future__ import annotations

import zlib


def compute_crc32(entry_type: int, key: bytes, value: bytes) -> int:
    """Compute the CRC32 checksum over entry_type + key + value.

    entry_type is included so that a bit-flip changing e.g. SET -> DELETE
    is also caught, not just corruption of the key/value bytes.
    """
    checksum = zlib.crc32(bytes([entry_type]))
    checksum = zlib.crc32(key, checksum)
    checksum = zlib.crc32(value, checksum)
    return checksum & 0xFFFFFFFF


def verify_crc32(expected: int, entry_type: int, key: bytes, value: bytes) -> bool:
    """Return True if the recomputed checksum matches `expected`."""
    return compute_crc32(entry_type, key, value) == expected