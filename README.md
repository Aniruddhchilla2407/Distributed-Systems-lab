# distributed-systems-lab

Three small systems built from one shared primitive: a durable, crash-safe
append-only log.

1. **`storage-core`** — a fixed-header write-ahead log, and a WAL-backed
   key-value store built on top of it.
2. **`broker-core`** — a Kafka-style topic/partition message broker that
   reuses `storage-core`'s log format directly: a partition *is* a log,
   just addressed by byte offset instead of replayed into a dict.
3. **`bench-cli`** — an async load-testing CLI that benchmarks both servers
   over HTTP and reports p50/p95/p99 latency and throughput.

Each core package (`storage-core`, `broker-core`) has zero I/O/networking
concerns and is fully unit tested in isolation. Each is exposed over HTTP
by a thin FastAPI server (`kv-server`, `broker-server`).

## Why one shared log format

See [`docs/log-format.md`](docs/log-format.md) for the full byte-layout
spec. The short version: every entry is a 22-byte fixed header (version,
entry type, key/value lengths, timestamp, CRC32) followed by the key and
value bytes. Because the header is fixed-size and self-describing, any
reader can:

- seek to an arbitrary byte offset and know exactly how many bytes to read
  before even looking at the payload (this is literally what a broker
  "offset" is here — a byte position),
- detect a torn write left over from a crash mid-append (short header,
  length mismatch, or CRC32 mismatch) and safely truncate back to the last
  valid entry, rather than corrupting the whole log.

`broker-core`'s `Partition` class is a ~40-line wrapper around
`storage-core`'s `LogWriter`/`LogReader` — the durability and recovery
logic is written exactly once.

## Project layout
packages/
├── storage-core/    # append-only log + WAL-backed KV store (core logic, no I/O)
├── kv-server/        # FastAPI HTTP server wrapping storage-core's KVStore
├── broker-core/      # topic/partition broker built on storage-core's log
├── broker-server/    # FastAPI HTTP server wrapping broker-core
└── bench-cli/         # async load generator + latency percentile reporting
## Running it locally

```bash
python -m venv .venv
.venv\Scripts\activate        # Windows; use `source .venv/bin/activate` on macOS/Linux

pip install -e "packages/storage-core[dev]"
pip install -e "packages/broker-core[dev]"
pip install -e "packages/kv-server[dev]"
pip install -e "packages/broker-server[dev]"
pip install -e "packages/bench-cli[dev]"
```

Run all tests:
```bash
pytest packages/ -v
```

Start the servers (separate terminals):
```bash
python -m kv_server.cli start --port 8000 --data-path data/kv_server.log
python -m broker_server.cli start --port 8001 --data-dir data/broker
```

Benchmark them:
```bash
bench-cli kv --url http://127.0.0.1:8000 --concurrency 20 --duration 15
bench-cli broker --url http://127.0.0.1:8001 --concurrency 20 --duration 15
```

## Benchmark results

20 concurrent workers, 15-second run, 50/50 read/write split, single
uvicorn process, tested on localhost.

| Configuration | Throughput (ops/sec) | p50 (ms) | p95 (ms) | p99 (ms) |
|---|---|---|---|---|
| kv-server, fsync off | 137.20 | 79.29 | 438.82 | 692.96 |
| kv-server, fsync on | 134.53 | 83.87 | 454.75 | 759.76 |
| kv-server, fsync on + batched (10ms window) | 128.98 | 77.74 | 483.09 | 741.17 |
| broker-server, fsync off | 95.51 | 107.88 | 630.65 | 1170.45 |
| broker-server, fsync on | 83.09 | 129.16 | 715.77 | 1107.05 |

### What the data actually shows

The obvious hypothesis going in was "disabling fsync should meaningfully
speed things up, since we're skipping a disk round-trip." The data doesn't
support that: fsync off vs on differs by only 2–13% across every metric.

That ruled out disk sync as the dominant cost, so the next hypothesis was
batching multiple writers' fsyncs into one disk trip (see
`KVStore.batch_window_ms` in `storage-core`). That also didn't move the
numbers — which, combined with the first result, points to the real
bottleneck: `KVStore` serializes every write through a single
`threading.Lock`, so under concurrent load, requests are mostly queueing
for lock acquisition, not waiting on disk I/O. Batching only reduces *how
often* fsync is called at the end of an already-serialized path — it
can't help a problem it isn't the cause of.

**This is a real, deliberate design tradeoff, not a bug.** A single
writer per log is what makes the crash-recovery story simple and correct
(see `docs/log-format.md`) — there's one unambiguous order of events to
replay. Removing that lock without another form of ordering guarantee
would break recovery correctness.

**What would actually help**, in rough order of effort:
- **Sharding**: split the keyspace across N independent logs (each with
  its own lock), the same pattern `broker-core`'s `Topic.partition_for_key`
  already uses for message partitioning. This raises the throughput
  ceiling by giving concurrent writers independent locks — at the cost of
  losing a single global write-order across the whole store (ordering is
  preserved only within a shard). Not implemented here yet; a natural
  next step.
- **Separating read and write locking**: `get()` currently takes the same
  lock as `set()`/`delete()`, even though reads never touch the WAL. A
  reader-writer lock would let reads proceed without waiting on writers.
- **Reducing work done while holding the lock**: the lock is currently
  held across the full encode + write + dict update; trimming that
  critical section would shorten queue time per writer even without
  architectural changes.

## Known limitations

- `broker-core`'s consumer group rebalancing is on-demand (triggered when
  a new member first calls `consume()`) and uses static round-robin
  assignment recomputed on membership change — not a real rebalance
  protocol with heartbeats or proactive partition revocation. A member
  that joins after another has already consumed everything may see fewer
  messages than a perfectly even split.
- Offsets are raw byte positions in the log file, not logical sequence
  numbers with a separate index (see `docs/log-format.md`) — simplest
  correct thing that works; a production system would add an index layer.
- No replication — this is a single-node log, not a distributed one.
  `storage-core` and `broker-core` are designed so that adding replication
  later (e.g., an external process that ships the WAL to replicas) would
  not require changing the log format.

## License

MIT — see [LICENSE](LICENSE).