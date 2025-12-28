from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from typing import Protocol

from infra.queue.base import TaskItem, TaskQueue
from infra.task_state.store import TaskStatusStore
from models.task_payload import PipelineTask
from models.task_status import TaskState

logger = logging.getLogger(__name__)


class TaskHandler(Protocol):
    """Handler interface for executing a task spec."""

    def handle(self, task_id: int, task: PipelineTask) -> None: ...


@dataclass
class WorkerRuntime:
    """Background worker that pulls tasks from a queue and executes them."""

    queue: TaskQueue
    store: TaskStatusStore
    handler: TaskHandler
    poll_interval: float = 0.5
    manage_state: bool = False
    _stop_event: threading.Event = field(default_factory=threading.Event, init=False)
    _thread: threading.Thread | None = field(default=None, init=False)

    def start(self) -> None:
        """Start the worker thread."""
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the worker thread."""
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            item = self.queue.dequeue(timeout=self.poll_interval)
            if item is None:
                continue
            self._execute_item(item)

    def _execute_item(self, item: TaskItem) -> None:
        worker_id = threading.current_thread().name
        queue_lag_ms = int((time.monotonic() - item.enqueued_at) * 1000)
        logger.info(
            "dequeued task",
            extra={"worker_id": worker_id, "queue_lag_ms": queue_lag_ms, "task_id": item.task_id},
        )
        try:
            try:
                record = self.store.get_by_id(item.task_id)
                if record.state in {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED}:
                    logger.warning(
                        "task already terminal on dequeue",
                        extra={"task_id": item.task_id, "state": record.state.value},
                    )
                    return
            except Exception:
                logger.warning(
                    "failed to load task status on dequeue",
                    extra={"task_id": item.task_id},
                )

            if self.manage_state:
                self.store.update_state(item.task_id, TaskState.RUNNING)
            self.store.update_heartbeat(item.task_id)
            self.handler.handle(item.task_id, item.task)
            if self.manage_state:
                self.store.update_state(item.task_id, TaskState.SUCCEEDED)
        except Exception as exc:
            logger.exception("worker execution failed", extra={"task_id": item.task_id})
            if self.manage_state:
                self.store.update_state(item.task_id, TaskState.FAILED, error=str(exc))
            time.sleep(0)
