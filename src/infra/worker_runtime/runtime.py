from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Protocol

from infra.queue.base import TaskItem, TaskQueue
from infra.task_state.store import TaskStatusStore
from models.task_payload import PipelineTask
from models.task_status import TaskState


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
        try:
            self.store.update_state(item.task_id, TaskState.RUNNING)
            self.store.update_heartbeat(item.task_id)
            self.handler.handle(item.task_id, item.task)
            self.store.update_state(item.task_id, TaskState.SUCCEEDED)
        except Exception as exc:
            self.store.update_state(item.task_id, TaskState.FAILED, error=str(exc))
            time.sleep(0)
