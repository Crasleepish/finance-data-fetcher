from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from models.task_spec import TaskSpec


@dataclass(frozen=True)
class TaskItem:
    """Queue payload for task execution."""

    task_id: int
    spec: TaskSpec


class TaskQueue(Protocol):
    """Abstract queue interface for task execution."""

    def enqueue(self, item: TaskItem) -> None:
        """Put a task item into the queue."""

    def dequeue(self, timeout: float | None = None) -> TaskItem | None:
        """Get a task item from the queue; return None on timeout."""
