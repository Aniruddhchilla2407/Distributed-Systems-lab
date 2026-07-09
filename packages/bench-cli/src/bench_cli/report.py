"""Terminal reporting for benchmark results."""

from __future__ import annotations

import json
from dataclasses import asdict

from rich.console import Console
from rich.table import Table

from bench_cli.stats import BenchmarkStats

console = Console()


def print_report(title: str, stats: BenchmarkStats) -> None:
    table = Table(title=title)
    table.add_column("Metric", style="cyan")
    table.add_column("Value", style="magenta")

    table.add_row("Total requests", str(stats.count))
    table.add_row("Errors", str(stats.errors))
    table.add_row("Duration (s)", f"{stats.duration_seconds:.2f}")
    table.add_row("Throughput (ops/sec)", f"{stats.throughput_ops_sec:.2f}")
    table.add_row("Min latency (ms)", f"{stats.min_ms:.3f}")
    table.add_row("Mean latency (ms)", f"{stats.mean_ms:.3f}")
    table.add_row("p50 latency (ms)", f"{stats.p50_ms:.3f}")
    table.add_row("p95 latency (ms)", f"{stats.p95_ms:.3f}")
    table.add_row("p99 latency (ms)", f"{stats.p99_ms:.3f}")
    table.add_row("Max latency (ms)", f"{stats.max_ms:.3f}")

    console.print(table)


def save_report_json(path: str, title: str, stats: BenchmarkStats) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump({"title": title, **asdict(stats)}, f, indent=2)