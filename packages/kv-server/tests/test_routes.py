from pathlib import Path
from typing import Iterator

import pytest
from fastapi.testclient import TestClient

from kv_server.app import create_app


@pytest.fixture
def client(tmp_path: Path) -> Iterator[TestClient]:
    app = create_app(data_path=tmp_path / "test.log", fsync_every_write=False)
    with TestClient(app) as c:
        yield c


def test_health_check(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_get_missing_key_returns_404(client: TestClient) -> None:
    response = client.get("/keys/nope")
    assert response.status_code == 404


def test_set_and_get_round_trip(client: TestClient) -> None:
    put_response = client.put("/keys/foo", json={"value": "bar"})
    assert put_response.status_code == 200
    assert put_response.json() == {"key": "foo", "value": "bar"}

    get_response = client.get("/keys/foo")
    assert get_response.status_code == 200
    assert get_response.json() == {"key": "foo", "value": "bar"}


def test_set_overwrites_existing_key(client: TestClient) -> None:
    client.put("/keys/foo", json={"value": "v1"})
    client.put("/keys/foo", json={"value": "v2"})

    response = client.get("/keys/foo")
    assert response.json()["value"] == "v2"


def test_delete_removes_key(client: TestClient) -> None:
    client.put("/keys/foo", json={"value": "bar"})

    delete_response = client.delete("/keys/foo")
    assert delete_response.status_code == 204

    get_response = client.get("/keys/foo")
    assert get_response.status_code == 404


def test_delete_missing_key_returns_404(client: TestClient) -> None:
    response = client.delete("/keys/nope")
    assert response.status_code == 404


def test_list_keys(client: TestClient) -> None:
    client.put("/keys/a", json={"value": "1"})
    client.put("/keys/b", json={"value": "2"})

    response = client.get("/keys")
    assert response.status_code == 200
    body = response.json()
    assert body["count"] == 2
    assert set(body["keys"]) == {"a", "b"}


def test_compact_endpoint_preserves_data(client: TestClient) -> None:
    client.put("/keys/a", json={"value": "1"})
    client.put("/keys/a", json={"value": "2"})
    client.delete("/keys/a")
    client.put("/keys/b", json={"value": "3"})

    compact_response = client.post("/compact")
    assert compact_response.status_code == 200

    assert client.get("/keys/a").status_code == 404
    assert client.get("/keys/b").json()["value"] == "3"


def test_data_persists_across_app_restarts(tmp_path: Path) -> None:
    log_path = tmp_path / "persist.log"

    app1 = create_app(data_path=log_path, fsync_every_write=False)
    with TestClient(app1) as client1:
        client1.put("/keys/x", json={"value": "persisted"})

    app2 = create_app(data_path=log_path, fsync_every_write=False)
    with TestClient(app2) as client2:
        response = client2.get("/keys/x")
        assert response.status_code == 200
        assert response.json()["value"] == "persisted"
def test_batched_writes_work_through_http(tmp_path: Path) -> None:
    from kv_server.app import create_app as create_batched_app

    app = create_batched_app(
        data_path=tmp_path / "batched.log",
        fsync_every_write=True,
        batch_window_ms=10,
    )
    with TestClient(app) as client:
        for i in range(10):
            response = client.put(f"/keys/k{i}", json={"value": f"v{i}"})
            assert response.status_code == 200

        for i in range(10):
            response = client.get(f"/keys/k{i}")
            assert response.json()["value"] == f"v{i}"