from collections.abc import Iterator
from pathlib import Path

import pytest
from broker_server.app import create_app
from fastapi.testclient import TestClient


@pytest.fixture
def client(tmp_path: Path) -> Iterator[TestClient]:
    app = create_app(data_dir=tmp_path / "broker", fsync_every_write=False)
    with TestClient(app) as c:
        yield c


def test_health_check(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200


def test_create_topic(client: TestClient) -> None:
    response = client.post("/topics", json={"name": "orders", "num_partitions": 3})
    assert response.status_code == 201
    assert response.json() == {"name": "orders", "num_partitions": 3}


def test_create_duplicate_topic_returns_409(client: TestClient) -> None:
    client.post("/topics", json={"name": "orders", "num_partitions": 1})
    response = client.post("/topics", json={"name": "orders", "num_partitions": 1})
    assert response.status_code == 409


def test_list_topics(client: TestClient) -> None:
    client.post("/topics", json={"name": "a", "num_partitions": 1})
    client.post("/topics", json={"name": "b", "num_partitions": 2})

    response = client.get("/topics")
    names = {t["name"] for t in response.json()}
    assert names == {"a", "b"}


def test_produce_to_missing_topic_returns_404(client: TestClient) -> None:
    response = client.post("/topics/nope/produce", json={"value": "x"})
    assert response.status_code == 404


def test_produce_and_consume_round_trip(client: TestClient) -> None:
    client.post("/topics", json={"name": "events", "num_partitions": 1})
    produce_response = client.post("/topics/events/produce", json={"value": "hello", "key": "k1"})
    assert produce_response.status_code == 200
    body = produce_response.json()
    assert body["partition"] == 0
    assert body["offset"] == 0

    consume_response = client.post(
        "/topics/events/groups/g1/consume", json={"member_id": "m1", "max_records": 10}
    )
    assert consume_response.status_code == 200
    consumed = consume_response.json()
    assert consumed["count"] == 1
    assert consumed["records"][0]["value"] == "hello"
    assert consumed["records"][0]["key"] == "k1"


def test_consume_advances_past_previously_read_messages(client: TestClient) -> None:
    client.post("/topics", json={"name": "events", "num_partitions": 1})
    client.post("/topics/events/produce", json={"value": "first", "key": "k"})
    client.post("/topics/events/produce", json={"value": "second", "key": "k"})

    first_poll = client.post(
        "/topics/events/groups/g1/consume", json={"member_id": "m1", "max_records": 1}
    )
    assert first_poll.json()["records"][0]["value"] == "first"

    second_poll = client.post(
        "/topics/events/groups/g1/consume", json={"member_id": "m1", "max_records": 10}
    )
    assert second_poll.json()["records"][0]["value"] == "second"


def test_consume_from_missing_topic_returns_404(client: TestClient) -> None:
    response = client.post("/topics/nope/groups/g1/consume", json={"member_id": "m1"})
    assert response.status_code == 404


def test_multiple_members_share_total_message_count(client: TestClient) -> None:
    client.post("/topics", json={"name": "events", "num_partitions": 2})
    for i in range(4):
        client.post("/topics/events/produce", json={"value": f"msg-{i}"})

    r1 = client.post(
        "/topics/events/groups/g1/consume", json={"member_id": "m1", "max_records": 10}
    )
    r2 = client.post(
        "/topics/events/groups/g1/consume", json={"member_id": "m2", "max_records": 10}
    )

    total = r1.json()["count"] + r2.json()["count"]
    assert total == 4


def test_data_persists_across_restarts(tmp_path: Path) -> None:
    data_dir = tmp_path / "broker"

    app1 = create_app(data_dir=data_dir, fsync_every_write=False)
    with TestClient(app1) as client1:
        client1.post("/topics", json={"name": "durable", "num_partitions": 1})
        client1.post("/topics/durable/produce", json={"value": "persisted", "key": "k"})

    app2 = create_app(data_dir=data_dir, fsync_every_write=False)
    with TestClient(app2) as client2:
        client2.post("/topics", json={"name": "durable", "num_partitions": 1})
        consume_response = client2.post(
            "/topics/durable/groups/g1/consume", json={"member_id": "m1", "max_records": 10}
        )
        assert consume_response.json()["records"][0]["value"] == "persisted"
