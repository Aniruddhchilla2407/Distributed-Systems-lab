"""Concurrent load runner.

Spins up `concurrency` asyncio workers, each looping "pick an operation,
time it, record the latency" until `duration_seconds` elapses. Uses
asyncio rather than threads since targets are async HTTP clients
(httpx.AsyncClient) -- this generates high concurrency without the
overhead of OS threads.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Protocol

from bench_cli.workload import WorkloadConfig, is_write


class Target(Protocol):
    """A benchmark target: knows how to fire one read or one write
    against whatever system it wraps (kv-server, broker-server, ...)."""

    async def setup(self) -> None: ...
    async def teardown(self) -> None: ...
    async def read(self) -> None: ...
    async def write(self) -> None: ...


@dataclass(slots=True)
class RunResult:
    latencies_ms: list[float] = field(default_factory=list)
    errors: int = 0
    duration_seconds: float = 0.0


async def _worker(
    target: Target, write_ratio: float, deadline: float, result: RunResult, lock: asyncio.Lock
) -> None:
    while time.monotonic() < deadline:
        start = time.perf_counter()
        try:
            if is_write(write_ratio):
                await target.write()
            else:
                await target.read()
            elapsed_ms = (time.perf_counter() - start) * 1000
            async with lock:
                result.latencies_ms.append(elapsed_ms)
        except Exception:
            async with lock:
                result.errors += 1


async def run_benchmark(target: Target, config: WorkloadConfig) -> RunResult:
    await target.setup()
    result = RunResult()
    lock = asyncio.Lock()
    start = time.monotonic()
    deadline = start + config.duration_seconds

    workers = [
        _worker(target, config.write_ratio, deadline, result, lock)
        for _ in range(config.concurrency)
    ]
    await asyncio.gather(*workers)

    result.duration_seconds = time.monotonic() - start
    await target.teardown()
    return result
