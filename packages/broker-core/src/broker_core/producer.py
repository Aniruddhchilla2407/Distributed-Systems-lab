"""Producer: sends messages to a topic, choosing a partition per message.

Keyed messages: partition chosen deterministically by key (see
Topic.partition_for_key), so ordering is preserved per key.
Unkeyed messages: partition chosen round-robin, to spread load evenly
across partitions when there's no ordering requirement.
"""

from __future__ import annotations

from dataclasses import dataclass
from itertools import cycle

from broker_core.topic import Topic


@dataclass(frozen=True, slots=True)
class ProduceResult:
    partition: int
    offset: int


class Producer:
    def __init__(self, topic: Topic) -> None:
        self.topic = topic
        self._round_robin = cycle(range(topic.num_partitions))

    def send(self, value: bytes, key: bytes | None = None) -> ProduceResult:
        partition_id = (
            self.topic.partition_for_key(key) if key is not None else next(self._round_robin)
        )
        partition = self.topic.get_partition(partition_id)
        offset = partition.produce(key, value)
        return ProduceResult(partition=partition_id, offset=offset)