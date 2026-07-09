from bench_cli.workload import WorkloadConfig, is_write, random_key, random_value


def test_random_key_within_key_space() -> None:
    for _ in range(100):
        key = random_key(key_space_size=10)
        assert key.startswith("key-")
        index = int(key.removeprefix("key-"))
        assert 0 <= index < 10


def test_random_value_has_requested_length() -> None:
    value = random_value(size_bytes=50)
    assert len(value) == 50


def test_is_write_respects_ratio_bounds() -> None:
    assert all(not is_write(0.0) for _ in range(50))
    assert all(is_write(1.0) for _ in range(50))


def test_workload_config_defaults() -> None:
    config = WorkloadConfig()
    assert config.concurrency == 10
    assert 0.0 <= config.write_ratio <= 1.0