"""Benchmark target adapter for kv-server.

"write" = PUT /keys/{key}, "read" = GET /keys/{key}. Keys/values are drawn
from the shared WorkloadConfig (key space size, value size) so the same
config object can drive both this and the broker target comparably.
"""

from __future__ import annotations

import httpx

from bench_cli.workload import WorkloadConfig, random_key, random_value


class KVTarget:
    def __init__(self, base_url: str, config: WorkloadConfig) -> None:
        self.base_url = base_url.rstrip("/")
        self.config = config
        self._client: httpx.AsyncClient | None = None

    async def setup(self) -> None:
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=10.0)
        # Seed the key space so reads have something to hit from the start.
        for i in range(min(self.config.key_space_size, 50)):
            await self._client.put(
                f"/keys/key-{i}", json={"value": random_value(self.config.value_size_bytes)}
            )

    async def teardown(self) -> None:
        if self._client is not None:
            await self._client.aclose()

    async def write(self) -> None:
        assert self._client is not None
        key = random_key(self.config.key_space_size)
        value = random_value(self.config.value_size_bytes)
        response = await self._client.put(f"/keys/{key}", json={"value": value})
        response.raise_for_status()

    async def read(self) -> None:
        assert self._client is not None
        key = random_key(self.config.key_space_size)
        response = await self._client.get(f"/keys/{key}")
        if response.status_code not in (200, 404):
            response.raise_for_status()
