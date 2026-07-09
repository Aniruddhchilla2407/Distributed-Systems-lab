"""Consumer group coordination: partition assignment + offset commits.

A consumer group lets multiple consumer processes cooperatively read a
topic, with each partition handled by exactly one group member at a time.
Assignment here is a simple static round-robin over partition IDs --
real Kafka does dynamic rebalancing on membership changes; we keep this
fixed for simplicity and call it out as a known simplification.

Committed offsets are persisted to a JSON file per group, so a consumer
that restarts resumes from where it left off instead of from the start.
"""

from __future__ import annotations

import json
from pathlib import Path

from broker_core.consumer import Consumer
from broker_core.topic import Topic


class OffsetStore:
    """Persists committed offsets for one consumer group, one file per group."""

    def __init__(self, topic: Topic, group_id: str) -> None:
        self.group_id = group_id
        offsets_dir = topic.topic_dir / "__consumer_offsets__"
        offsets_dir.mkdir(parents=True, exist_ok=True)
        self._path = offsets_dir / f"{group_id}.json"
        self._offsets: dict[str, int] = self._load()

    def _load(self) -> dict[str, int]:
        if not self._path.exists():
            return {}
        with open(self._path, encoding="utf-8") as f:
            return {str(k): int(v) for k, v in json.load(f).items()}

    def _save(self) -> None:
        with open(self._path, "w", encoding="utf-8") as f:
            json.dump(self._offsets, f)

    def get(self, partition_id: int) -> int:
        return self._offsets.get(str(partition_id), 0)

    def commit(self, partition_id: int, offset: int) -> None:
        self._offsets[str(partition_id)] = offset
        self._save()


class ConsumerGroup:
    def __init__(self, topic: Topic, group_id: str, member_ids: list[str]) -> None:
        if not member_ids:
            raise ValueError("a consumer group needs at least one member")

        self.topic = topic
        self.group_id = group_id
        self.member_ids = member_ids
        self.offsets = OffsetStore(topic, group_id)
        self._assignment = self._assign_partitions()

    def _assign_partitions(self) -> dict[str, list[int]]:
        """Round-robin assignment of partitions across members."""
        assignment: dict[str, list[int]] = {member_id: [] for member_id in self.member_ids}
        for partition_id in range(self.topic.num_partitions):
            member_id = self.member_ids[partition_id % len(self.member_ids)]
            assignment[member_id].append(partition_id)
        return assignment

    def assignment_for(self, member_id: str) -> list[int]:
        return list(self._assignment.get(member_id, []))

    def consumers_for(self, member_id: str) -> list[Consumer]:
        """Build a Consumer, seeded from the last committed offset, for
        each partition assigned to this member."""
        return [
            Consumer(self.topic.get_partition(pid), start_offset=self.offsets.get(pid))
            for pid in self.assignment_for(member_id)
        ]

    def commit(self, partition_id: int, offset: int) -> None:
        self.offsets.commit(partition_id, offset)