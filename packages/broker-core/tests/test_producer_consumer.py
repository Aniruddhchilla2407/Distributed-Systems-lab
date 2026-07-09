from pathlib import Path

from broker_core.consumer import Consumer
from broker_core.producer import Producer
from broker_core.topic import Topic


def test_keyed_messages_are_partitioned_deterministically(tmp_path: Path) -> None:
    topic = Topic(tmp_path, "orders", num_partitions=4, fsync_every_write=False)
    producer = Producer(topic)

    r1 = producer.send(b"payload-1", key=b"user-42")
    r2 = producer.send(b"payload-2", key=b"user-42")

    # same key -> always the same partition
    assert r1.partition == r2.partition


def test_unkeyed_messages_round_robin_across_partitions(tmp_path: Path) -> None:
    topic = Topic(tmp_path, "events", num_partitions=3, fsync_every_write=False)
    producer = Producer(topic)

    results = [producer.send(f"msg-{i}".encode()) for i in range(6)]
    partitions_hit = {r.partition for r in results}

    assert partitions_hit == {0, 1, 2}


def test_consumer_reads_produced_messages_in_order(tmp_path: Path) -> None:
    topic = Topic(tmp_path, "log", num_partitions=1, fsync_every_write=False)
    producer = Producer(topic)

    for i in range(5):
        producer.send(f"value-{i}".encode(), key=b"same-key")

    consumer = Consumer(topic.get_partition(0), start_offset=0)
    records = consumer.poll(max_records=100)

    assert [r.value for r in records] == [f"value-{i}".encode() for i in range(5)]


def test_consumer_poll_respects_max_records(tmp_path: Path) -> None:
    topic = Topic(tmp_path, "log", num_partitions=1, fsync_every_write=False)
    producer = Producer(topic)
    for i in range(10):
        producer.send(f"value-{i}".encode(), key=b"k")

    consumer = Consumer(topic.get_partition(0), start_offset=0)
    first_batch = consumer.poll(max_records=4)
    assert len(first_batch) == 4

    second_batch = consumer.poll(max_records=100)
    assert len(second_batch) == 6


def test_consumer_seek_repositions_read(tmp_path: Path) -> None:
    topic = Topic(tmp_path, "log", num_partitions=1, fsync_every_write=False)
    producer = Producer(topic)
    producer.send(b"first", key=b"k")
    second = producer.send(b"second", key=b"k")

    consumer = Consumer(topic.get_partition(0), start_offset=0)
    consumer.seek(second.offset)
    records = consumer.poll()

    assert len(records) == 1
    assert records[0].value == b"second"


def test_has_more_reflects_unread_messages(tmp_path: Path) -> None:
    topic = Topic(tmp_path, "log", num_partitions=1, fsync_every_write=False)
    producer = Producer(topic)
    producer.send(b"only-message", key=b"k")

    consumer = Consumer(topic.get_partition(0), start_offset=0)
    assert consumer.has_more() is True

    consumer.poll()
    assert consumer.has_more() is False
