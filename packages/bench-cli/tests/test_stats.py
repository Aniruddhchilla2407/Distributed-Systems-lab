from bench_cli.stats import compute_stats


def test_empty_latencies_returns_zeroed_stats() -> None:
    stats = compute_stats([], errors=0, duration_seconds=5.0)
    assert stats.count == 0
    assert stats.throughput_ops_sec == 0.0


def test_percentiles_on_known_distribution() -> None:
    latencies = [float(i) for i in range(1, 101)]  # 1..100 ms
    stats = compute_stats(latencies, errors=0, duration_seconds=1.0)

    assert stats.count == 100
    assert stats.min_ms == 1.0
    assert stats.max_ms == 100.0
    assert 49 <= stats.p50_ms <= 51
    assert 94 <= stats.p95_ms <= 96
    assert 98 <= stats.p99_ms <= 100


def test_throughput_calculation() -> None:
    latencies = [1.0] * 50
    stats = compute_stats(latencies, errors=0, duration_seconds=5.0)
    assert stats.throughput_ops_sec == 10.0


def test_errors_are_passed_through() -> None:
    stats = compute_stats([1.0, 2.0], errors=3, duration_seconds=1.0)
    assert stats.errors == 3
    assert stats.count == 2
