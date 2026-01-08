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

    def remove(self, task_id: int) -> bool:
        """Remove a pending task by id if it is still in the queue."""
        removed = False
        with self._queue.mutex:
            items = list(self._queue.queue)
            remaining = [item for item in items if item.task_id != task_id]
            removed = len(remaining) != len(items)
            if removed:
                self._queue.queue.clear()
                self._queue.queue.extend(remaining)
        return removed
