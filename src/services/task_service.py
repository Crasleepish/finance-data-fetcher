from __future__ import annotations

from dataclasses import dataclass

from infra.queue.base import TaskItem, TaskQueue
from infra.task_state.store import TaskStatusStore
from models.task_spec import TaskSpec
from models.task_status import TaskStatusRecord


@dataclass(frozen=True)
class TaskService:
    """Service for creating tasks and enqueuing them for execution."""

    store: TaskStatusStore
    queue: TaskQueue

    def start_task(self, spec: TaskSpec, idempotency_key: str) -> TaskStatusRecord:
        """Create a task record and enqueue it for execution."""
        task = self.store.create_task(spec=spec, idempotency_key=idempotency_key, attempt=1)
        self.queue.enqueue(TaskItem(task_id=task.task_id, spec=spec))
        return task
