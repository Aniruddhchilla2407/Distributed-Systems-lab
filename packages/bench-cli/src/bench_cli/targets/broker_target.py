"""Benchmark target adapter for broker-server.

"write" = produce a message. "read" = consume a batch of messages for a
dedicated benchmark consumer group, so reads don't compete with other
consumers' offsets. Topic is created in setup() if it doesn't exist yet.
"""

from __future__ import annotations

import httpx

from bench_cli.workload import WorkloadConfig, random_key, random_value


class BrokerTarget:
    def __init__(
        self,
        base_url: str,
        config: WorkloadConfig,
        topic_name: str = "bench-topic",
        num_partitions: int = 4,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.config = config
        self.topic_name = topic_name
        self.num_partitions = num_partitions
        self._client: httpx.AsyncClient | None = None

    async def setup(self) -> None:
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=10.0)
        response = await self._client.post(
            "/topics", json={"name": self.topic_name, "num_partitions": self.num_partitions}
        )
        # 409 means the topic already exists from a previous run -- fine.
        if response.status_code not in (201, 409):
            response.raise_for_status()

    async def teardown(self) -> None:
        if self._client is not None:
            await self._client.aclose()

    async def write(self) -> None:
        assert self._client is not None
        key = random_key(self.config.key_space_size)
        value = random_value(self.config.value_size_bytes)
        response = await self._client.post(
            f"/topics/{self.topic_name}/produce", json={"value": value, "key": key}
        )
        response.raise_for_status()

    async def read(self) -> None:
        assert self._client is not None
        response = await self._client.post(
            f"/topics/{self.topic_name}/groups/bench-group/consume",
            json={"member_id": "bench-member", "max_records": 10},
        )
        response.raise_for_status()