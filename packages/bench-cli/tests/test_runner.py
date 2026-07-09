import asyncio

from bench_cli.runner import run_benchmark
from bench_cli.workload import WorkloadConfig


class FakeTarget:
    def __init__(self) -> None:
        self.setup_called = False
        self.teardown_called = False
        self.reads = 0
        self.writes = 0

    async def setup(self) -> None:
        self.setup_called = True

    async def teardown(self) -> None:
        self.teardown_called = True

    async def read(self) -> None:
        self.reads += 1
        await asyncio.sleep(0)

    async def write(self) -> None:
        self.writes += 1
        await asyncio.sleep(0)


class FailingTarget(FakeTarget):
    async def write(self) -> None:
        raise RuntimeError("simulated failure")


def test_run_benchmark_calls_setup_and_teardown() -> None:
    target = FakeTarget()
    config = WorkloadConfig(concurrency=2, duration_seconds=0.2, write_ratio=0.5)

    asyncio.run(run_benchmark(target, config))

    assert target.setup_called
    assert target.teardown_called


def test_run_benchmark_records_latencies() -> None:
    target = FakeTarget()
    config = WorkloadConfig(concurrency=3, duration_seconds=0.2, write_ratio=0.5)

    result = asyncio.run(run_benchmark(target, config))

    assert result.errors == 0
    assert len(result.latencies_ms) == target.reads + target.writes
    assert len(result.latencies_ms) > 0


def test_run_benchmark_all_reads_when_write_ratio_zero() -> None:
    target = FakeTarget()
    config = WorkloadConfig(concurrency=2, duration_seconds=0.2, write_ratio=0.0)

    asyncio.run(run_benchmark(target, config))

    assert target.writes == 0
    assert target.reads > 0


def test_run_benchmark_counts_errors_without_crashing() -> None:
    target = FailingTarget()
    config = WorkloadConfig(concurrency=2, duration_seconds=0.2, write_ratio=1.0)

    result = asyncio.run(run_benchmark(target, config))

    assert result.errors > 0
    assert len(result.latencies_ms) == 0
