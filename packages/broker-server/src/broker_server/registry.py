"""In-process registry tying together broker-core's Topic/ConsumerGroup
objects with the HTTP layer's notion of "which topics/groups exist".

Topics are created explicitly (POST /topics) rather than auto-created on
first use, so a typo in a topic name in a produce/consume call fails loudly
instead of silently creating a stray topic.

Consumer group membership is tracked per (topic, group_id): the first time
an unseen member_id calls consume(), it's added to the group and partitions
are (re)assigned across the now-larger membership. This is a deliberately
simple stand-in for real rebalancing -- assignment is static round-robin,
recomputed on membership change, with no rebalance protocol or heartbeats.
"""

from __future__ import annotations

from pathlib import Path

from broker_core.consumer_group import ConsumerGroup
from broker_core.partition import ConsumerRecord
from broker_core.producer import Producer, ProduceResult
from broker_core.topic import Topic


class TopicAlreadyExistsError(Exception):
    pass


class TopicNotFoundError(Exception):
    pass


class BrokerRegistry:
    def __init__(self, data_dir: str | Path, *, fsync_every_write: bool = True) -> None:
        self.data_dir = Path(data_dir)
        self.fsync_every_write = fsync_every_write
        self._topics: dict[str, Topic] = {}
        self._producers: dict[str, Producer] = {}
        self._groups: dict[tuple[str, str], ConsumerGroup] = {}
        self._group_members: dict[tuple[str, str], list[str]] = {}

    def create_topic(self, name: str, num_partitions: int) -> Topic:
        if name in self._topics:
            raise TopicAlreadyExistsError(f"topic '{name}' already exists")
        topic = Topic(self.data_dir, name, num_partitions, fsync_every_write=self.fsync_every_write)
        self._topics[name] = topic
        self._producers[name] = Producer(topic)
        return topic

    def get_topic(self, name: str) -> Topic:
        topic = self._topics.get(name)
        if topic is None:
            raise TopicNotFoundError(f"topic '{name}' not found")
        return topic

    def list_topics(self) -> list[Topic]:
        return list(self._topics.values())

    def produce(self, topic_name: str, value: bytes, key: bytes | None) -> ProduceResult:
        self.get_topic(topic_name)  # raises TopicNotFoundError if missing
        producer = self._producers[topic_name]
        return producer.send(value, key=key)

    def _group_key(self, topic_name: str, group_id: str) -> tuple[str, str]:
        return (topic_name, group_id)

    def _ensure_group(self, topic_name: str, group_id: str, member_id: str) -> ConsumerGroup:
        topic = self.get_topic(topic_name)
        key = self._group_key(topic_name, group_id)
        members = self._group_members.setdefault(key, [])

        if member_id not in members:
            members.append(member_id)

        # Recreate the group whenever membership changes, so partition
        # assignment is recomputed across the current member list. This
        # also naturally re-reads the last committed offsets from disk.
        if key not in self._groups or set(self._groups[key].member_ids) != set(members):
            self._groups[key] = ConsumerGroup(topic, group_id, list(members))

        return self._groups[key]

    def consume(
        self, topic_name: str, group_id: str, member_id: str, max_records: int
    ) -> list[ConsumerRecord]:
        group = self._ensure_group(topic_name, group_id, member_id)
        consumers = group.consumers_for(member_id)

        records: list[ConsumerRecord] = []
        remaining = max_records
        for consumer in consumers:
            if remaining <= 0:
                break
            batch = consumer.poll(max_records=remaining)
            records.extend(batch)
            remaining -= len(batch)
            if batch:
                group.commit(consumer.partition.partition_id, consumer.current_offset)

        return records

    def close(self) -> None:
        for topic in self._topics.values():
            topic.close()
