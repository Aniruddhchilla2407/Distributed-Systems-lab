"""A topic: a named collection of partitions.

Partition assignment for a keyed message uses CRC32(key) % num_partitions.
This gives every message with the same key a stable, deterministic home
in the same partition, which is what preserves per-key ordering.
"""

from __future__ import annotations

import zlib
from pathlib import Path

from broker_core.partition import Partition


class Topic:
    def __init__(
        self,
        data_dir: str | Path,
        name: str,
        num_partitions: int,
        *,
        fsync_every_write: bool = True,
    ) -> None:
        if num_partitions < 1:
            raise ValueError("a topic must have at least one partition")

        self.name = name
        self.num_partitions = num_partitions
        self.topic_dir = Path(data_dir) / name
        self.topic_dir.mkdir(parents=True, exist_ok=True)

        self._partitions: list[Partition] = [
            Partition(
                self.topic_dir / f"partition-{i}.log",
                partition_id=i,
                fsync_every_write=fsync_every_write,
            )
            for i in range(num_partitions)
        ]

    def partition_for_key(self, key: bytes) -> int:
        """Deterministic partition assignment: same key always lands in the
        same partition, so per-key message ordering is preserved."""
        return zlib.crc32(key) % self.num_partitions

    def get_partition(self, partition_id: int) -> Partition:
        return self._partitions[partition_id]

    def partitions(self) -> list[Partition]:
        return list(self._partitions)

    def close(self) -> None:
        for p in self._partitions:
            p.close()

    def __enter__(self) -> Topic:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()
