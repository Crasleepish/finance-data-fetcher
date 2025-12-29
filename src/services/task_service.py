from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from sqlalchemy.exc import IntegrityError

from core.pipeline.validation import ensure_hashable
from infra.idempotency.guard import IdempotencyGuard
from infra.queue.base import TaskItem, TaskQueue
from infra.task_state.store import TaskStatusStore
from models.task_payload import PipelineTask
from models.task_status import TaskStatusRecord

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TaskService:
    """Service for creating tasks and enqueuing them for execution."""

    store: TaskStatusStore
    queue: TaskQueue
    guard: IdempotencyGuard

    def start_task(self, task: PipelineTask) -> TaskStatusRecord:
        """Create a task record and enqueue it for execution."""
        arguments_digest = ensure_hashable(task.arguments)
        options_digest = ensure_hashable(task.options)
        pipeline_label = task.pipeline_id or "selector"
        decision = self.guard.check_or_prepare(task)
        if decision.existing is not None:
            logger.info(
                "idempotency decision",
                extra={
                    "action": "dedupe_active",
                    "task_id": decision.existing.task_id,
                    "pipeline_id": pipeline_label,
                    "arguments_digest": arguments_digest,
                    "options_digest": options_digest,
                },
            )
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
            logger.info(
                "idempotency decision",
                extra={
                    "action": "dedupe_active",
                    "task_id": decision.existing.task_id,
                    "pipeline_id": pipeline_label,
                    "arguments_digest": arguments_digest,
                    "options_digest": options_digest,
                },
            )
            return decision.existing

        logger.info(
            "idempotency decision",
            extra={
                "action": "create_run",
                "task_id": record.task_id,
                "pipeline_id": pipeline_label,
                "arguments_digest": arguments_digest,
                "options_digest": options_digest,
            },
        )

        enqueue_start = time.monotonic()
        self.queue.enqueue(TaskItem(task_id=record.task_id, task=task))
        enqueue_latency_ms = int((time.monotonic() - enqueue_start) * 1000)
        logger.info(
            "enqueue task",
            extra={
                "queue_name": type(self.queue).__name__,
                "enqueue_latency_ms": enqueue_latency_ms,
                "task_id": record.task_id,
            },
        )
        return record
