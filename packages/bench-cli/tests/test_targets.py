import httpx
import pytest

from bench_cli.targets.broker_target import BrokerTarget
from bench_cli.targets.kv_target import KVTarget
from bench_cli.workload import WorkloadConfig


def _kv_mock_handler(request: httpx.Request) -> httpx.Response:
    if request.method == "PUT":
        return httpx.Response(200, json={"key": "k", "value": "v"})
    if request.method == "GET":
        return httpx.Response(200, json={"key": "k", "value": "v"})
    return httpx.Response(404)


@pytest.mark.asyncio
async def test_kv_target_write_and_read() -> None:
    config = WorkloadConfig(key_space_size=5, value_size_bytes=10)
    target = KVTarget("http://testserver", config)

    transport = httpx.MockTransport(_kv_mock_handler)
    target._client = httpx.AsyncClient(base_url="http://testserver", transport=transport)

    await target.write()
    await target.read()
    await target.teardown()


def _broker_mock_handler(request: httpx.Request) -> httpx.Response:
    if request.url.path == "/topics":
        return httpx.Response(201, json={"name": "bench-topic", "num_partitions": 4})
    if request.url.path.endswith("/produce"):
        return httpx.Response(200, json={"partition": 0, "offset": 0})
    if request.url.path.endswith("/consume"):
        return httpx.Response(200, json={"records": [], "count": 0})
    return httpx.Response(404)


@pytest.mark.asyncio
async def test_broker_target_write_and_read() -> None:
    config = WorkloadConfig(key_space_size=5, value_size_bytes=10)
    target = BrokerTarget("http://testserver", config)

    transport = httpx.MockTransport(_broker_mock_handler)
    target._client = httpx.AsyncClient(base_url="http://testserver", transport=transport)

    await target.write()
    await target.read()
    await target.teardown()