from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy.exc import IntegrityError

from infra.idempotency.guard import IdempotencyGuard
from infra.queue.base import TaskItem, TaskQueue
from infra.task_state.store import TaskStatusStore
from models.task_payload import PipelineTask
from models.task_status import TaskStatusRecord


@dataclass(frozen=True)
class TaskService:
    """Service for creating tasks and enqueuing them for execution."""

    store: TaskStatusStore
    queue: TaskQueue
    guard: IdempotencyGuard

    def start_task(self, task: PipelineTask) -> TaskStatusRecord:
        """Create a task record and enqueue it for execution."""
        decision = self.guard.check_or_prepare(task)
        if decision.existing is not None:
            return decision.existing

        attempt = decision.attempt or 1
        try:
            record = self.store.create_task(
                spec=task.spec,
                idempotency_key=decision.idempotency_key,
                attempt=attempt,
                task_payload=task.model_dump(mode="json"),
            )
        except IntegrityError:
            decision = self.guard.check_or_prepare(task)
            if decision.existing is None:
                raise
            return decision.existing

        self.queue.enqueue(TaskItem(task_id=record.task_id, task=task))
        return record
