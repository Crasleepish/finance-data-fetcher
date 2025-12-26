from __future__ import annotations

import queue
from dataclasses import dataclass, field

from infra.queue.base import TaskItem, TaskQueue


@dataclass
class InMemoryTaskQueue(TaskQueue):
    """In-process FIFO queue implementation."""

    _queue: queue.Queue[TaskItem] = field(default_factory=queue.Queue)

    def enqueue(self, item: TaskItem) -> None:
        """Put a task item into the queue."""
        self._queue.put(item)

    def dequeue(self, timeout: float | None = None) -> TaskItem | None:
        """Get a task item from the queue; return None on timeout."""
        try:
            return self._queue.get(timeout=timeout)
        except queue.Empty:
            return None
