"""Workload generation: defines what request pattern to fire at a target.

Kept generic (write ratio, key space size, value size) so the same
WorkloadConfig drives both kv-server (get/set) and broker-server
(produce/consume) benchmarks -- each target adapter decides what "read"
and "write" concretely mean for its own API.
"""

from __future__ import annotations

import random
import string
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class WorkloadConfig:
    concurrency: int = 10
    duration_seconds: float = 10.0
    write_ratio: float = 0.5  # fraction of operations that are writes, 0.0-1.0
    key_space_size: int = 1000  # number of distinct keys to read/write across
    value_size_bytes: int = 100


def random_value(size_bytes: int) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=size_bytes))


def random_key(key_space_size: int) -> str:
    return f"key-{random.randint(0, key_space_size - 1)}"


def is_write(write_ratio: float) -> bool:
    return random.random() < write_ratio
