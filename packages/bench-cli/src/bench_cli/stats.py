"""Latency percentile and throughput computation."""

from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class BenchmarkStats:
    count: int
    errors: int
    duration_seconds: float
    throughput_ops_sec: float
    min_ms: float
    mean_ms: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float


def _percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    k = (len(sorted_values) - 1) * pct
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_values[int(k)]
    return sorted_values[f] + (sorted_values[c] - sorted_values[f]) * (k - f)


def compute_stats(
    latencies_ms: list[float], errors: int, duration_seconds: float
) -> BenchmarkStats:
    if not latencies_ms:
        return BenchmarkStats(
            count=0,
            errors=errors,
            duration_seconds=duration_seconds,
            throughput_ops_sec=0.0,
            min_ms=0.0,
            mean_ms=0.0,
            p50_ms=0.0,
            p95_ms=0.0,
            p99_ms=0.0,
            max_ms=0.0,
        )

    sorted_latencies = sorted(latencies_ms)
    count = len(sorted_latencies)
    throughput = count / duration_seconds if duration_seconds > 0 else 0.0

    return BenchmarkStats(
        count=count,
        errors=errors,
        duration_seconds=duration_seconds,
        throughput_ops_sec=throughput,
        min_ms=sorted_latencies[0],
        mean_ms=sum(sorted_latencies) / count,
        p50_ms=_percentile(sorted_latencies, 0.50),
        p95_ms=_percentile(sorted_latencies, 0.95),
        p99_ms=_percentile(sorted_latencies, 0.99),
        max_ms=sorted_latencies[-1],
    )