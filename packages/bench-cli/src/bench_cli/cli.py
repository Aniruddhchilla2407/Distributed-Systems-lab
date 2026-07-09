"""CLI entrypoint:
bench-cli kv --url http://127.0.0.1:8000 --concurrency 20 --duration 10
bench-cli broker --url http://127.0.0.1:8001 --concurrency 20 --duration 10
"""

from __future__ import annotations

import asyncio

import typer

from bench_cli.report import print_report, save_report_json
from bench_cli.runner import run_benchmark
from bench_cli.stats import compute_stats
from bench_cli.targets.broker_target import BrokerTarget
from bench_cli.targets.kv_target import KVTarget
from bench_cli.workload import WorkloadConfig

app = typer.Typer(name="bench-cli", help="Load-test kv-server and broker-server")


@app.command()
def kv(
    url: str = typer.Option("http://127.0.0.1:8000", help="Base URL of kv-server"),
    concurrency: int = typer.Option(10, help="Number of concurrent workers"),
    duration: float = typer.Option(10.0, help="Benchmark duration in seconds"),
    write_ratio: float = typer.Option(0.5, help="Fraction of operations that are writes"),
    key_space: int = typer.Option(1000, help="Number of distinct keys to read/write across"),
    value_size: int = typer.Option(100, help="Value size in bytes"),
    output_json: str | None = typer.Option(None, help="Optional path to save results as JSON"),
) -> None:
    config = WorkloadConfig(
        concurrency=concurrency,
        duration_seconds=duration,
        write_ratio=write_ratio,
        key_space_size=key_space,
        value_size_bytes=value_size,
    )
    target = KVTarget(url, config)

    result = asyncio.run(run_benchmark(target, config))
    stats = compute_stats(result.latencies_ms, result.errors, result.duration_seconds)

    title = f"kv-server benchmark ({url})"
    print_report(title, stats)
    if output_json:
        save_report_json(output_json, title, stats)


@app.command()
def broker(
    url: str = typer.Option("http://127.0.0.1:8001", help="Base URL of broker-server"),
    concurrency: int = typer.Option(10, help="Number of concurrent workers"),
    duration: float = typer.Option(10.0, help="Benchmark duration in seconds"),
    write_ratio: float = typer.Option(0.5, help="Fraction of operations that are writes (produce)"),
    key_space: int = typer.Option(1000, help="Number of distinct message keys"),
    value_size: int = typer.Option(100, help="Message value size in bytes"),
    topic: str = typer.Option("bench-topic", help="Topic name to benchmark against"),
    partitions: int = typer.Option(4, help="Number of partitions if the topic needs creating"),
    output_json: str | None = typer.Option(None, help="Optional path to save results as JSON"),
) -> None:
    config = WorkloadConfig(
        concurrency=concurrency,
        duration_seconds=duration,
        write_ratio=write_ratio,
        key_space_size=key_space,
        value_size_bytes=value_size,
    )
    target = BrokerTarget(url, config, topic_name=topic, num_partitions=partitions)

    result = asyncio.run(run_benchmark(target, config))
    stats = compute_stats(result.latencies_ms, result.errors, result.duration_seconds)

    title = f"broker-server benchmark ({url})"
    print_report(title, stats)
    if output_json:
        save_report_json(output_json, title, stats)


if __name__ == "__main__":
    app()
