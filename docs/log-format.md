# Log Entry Format

This is the on-disk binary format for a single entry in the append-only log.
`storage-core` implements it once; `broker-core` reuses it unchanged for
partitions. Every reader (KV recovery, broker consumer, benchmarking tools)
depends on this being stable, so the header is versioned from day one.

## Design goals

- **Fixed-size header** so any reader can seek to a byte offset and know
  exactly how many bytes to read before it even looks at the payload.
- **Self-describing entries** so a partial/corrupt tail entry (from a crash
  mid-write) can be detected and truncated on recovery, instead of silently
  corrupting the whole log.
- **One format, two use cases**: `entry_type` is what lets the same struct
  serve both "SET/DELETE a key" (KV store) and "produce a message" (broker).

## Header layout (22 bytes, little-endian)

| Field          | Type     | Size (bytes) | Offset | Description                                   |
|----------------|----------|---------------|--------|------------------------------------------------|
| `version`      | uint8    | 1             | 0      | Format version. Currently `1`.                  |
| `entry_type`   | uint8    | 1             | 1      | See Entry Types below.                          |
| `key_len`      | uint32   | 4             | 2      | Length of the key payload in bytes.             |
| `value_len`    | uint32   | 4             | 6      | Length of the value payload in bytes.           |
| `timestamp_ms` | uint64   | 8             | 10     | Wall-clock write time, ms since Unix epoch.     |
| `crc32`        | uint32   | 4             | 18     | CRC32 over `entry_type + key + value` bytes.    |

Total header size: **22 bytes**. Python `struct` format string: `"<BBIIQI"`.

> Note: `crc32` is deliberately placed *after* `timestamp_ms` in the struct
> even though it's conceptually about the payload — this keeps all
> fixed-width integer fields contiguous, which keeps `struct.calcsize`
> simple and avoids padding surprises.

## Entry types

| Value | Name              | Used by         | Meaning                                      |
|-------|-------------------|-----------------|-----------------------------------------------|
| `1`   | `SET`              | KV store        | `key` → `value` write.                        |
| `2`   | `DELETE`           | KV store        | Tombstone for `key`. `value_len` is `0`.      |
| `3`   | `SNAPSHOT_MARKER`  | KV store        | Marks a compaction snapshot boundary.         |
| `4`   | `PRODUCE`          | Broker          | Append a message to a partition.              |

## Full entry layout on disk