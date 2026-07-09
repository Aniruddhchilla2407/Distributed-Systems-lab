from pathlib import Path

from broker_core.consumer_group import ConsumerGroup
from broker_core.producer import Producer
from broker_core.topic import Topic


def test_partitions_are_assigned_round_robin(tmp_path: Path) -> None:
    topic = Topic(tmp_path, "topic", num_partitions=4, fsync_every_write=False)
    group = ConsumerGroup(topic, group_id="g1", member_ids=["m1", "m2"])

    assert group.assignment_for("m1") == [0, 2]
    assert group.assignment_for("m2") == [1, 3]


def test_single_member_gets_all_partitions(tmp_path: Path) -> None:
    topic = Topic(tmp_path, "topic", num_partitions=3, fsync_every_write=False)
    group = ConsumerGroup(topic, group_id="g1", member_ids=["solo"])

    assert group.assignment_for("solo") == [0, 1, 2]


def test_consumers_for_member_start_from_zero_when_no_commits(tmp_path: Path) -> None:
    topic = Topic(tmp_path, "topic", num_partitions=2, fsync_every_write=False)
    producer = Producer(topic)
    producer.send(b"v1", key=b"a")

    group = ConsumerGroup(topic, group_id="g1", member_ids=["m1"])
    consumers = group.consumers_for("m1")

    assert all(c.current_offset == 0 for c in consumers)


def test_commit_persists_across_group_recreation(tmp_path: Path) -> None:
    topic = Topic(tmp_path, "topic", num_partitions=1, fsync_every_write=False)

    group = ConsumerGroup(topic, group_id="g1", member_ids=["m1"])
    group.commit(partition_id=0, offset=42)

    # Recreate the group (simulating a restart) and check offset was persisted.
    group2 = ConsumerGroup(topic, group_id="g1", member_ids=["m1"])
    assert group2.offsets.get(0) == 42


def test_committed_offset_lets_consumer_resume(tmp_path: Path) -> None:
    topic = Topic(tmp_path, "topic", num_partitions=1, fsync_every_write=False)
    producer = Producer(topic)
    producer.send(b"first", key=b"k")
    producer.send(b"second", key=b"k")

    group = ConsumerGroup(topic, group_id="g1", member_ids=["m1"])
    consumer = group.consumers_for("m1")[0]
    consumer.poll(max_records=1)
    group.commit(partition_id=0, offset=consumer.current_offset)

    # New group instance (simulating a consumer restart) should resume after "first"
    group2 = ConsumerGroup(topic, group_id="g1", member_ids=["m1"])
    consumer2 = group2.consumers_for("m1")[0]
    remaining = consumer2.poll()

    assert len(remaining) == 1
    assert remaining[0].value == b"second"
