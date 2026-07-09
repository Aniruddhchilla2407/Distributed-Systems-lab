"""Consumer: sequentially reads messages from a single partition.

A Consumer tracks its own read position (current_offset) in memory. It
does not persist offsets itself -- that's ConsumerGroup's job (offsets
need to survive process restarts; a single Consumer object does not).
"""

from __future__ import annotations

from broker_core.partition import ConsumerRecord, Partition


class Consumer:
    def __init__(self, partition: Partition, start_offset: int = 0) -> None:
        self.partition = partition
        self.current_offset = start_offset

    def poll(self, max_records: int = 100) -> list[ConsumerRecord]:
        """Read up to max_records messages starting at current_offset,
        advancing current_offset past whatever was read."""
        records: list[ConsumerRecord] = []
        for record in self.partition.read_from(self.current_offset):
            records.append(record)
            self.current_offset = record.next_offset
            if len(records) >= max_records:
                break
        return records

    def seek(self, offset: int) -> None:
        self.current_offset = offset

    def has_more(self) -> bool:
        return self.current_offset < self.partition.end_offset()