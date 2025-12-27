from __future__ import annotations

import time

from sqlalchemy.engine import Engine

from infra.idempotency.guard import IdempotencyGuard
from infra.queue.in_memory import InMemoryTaskQueue
from infra.task_state.store import TaskStatusStore
from infra.worker_runtime.runtime import WorkerRuntime
from models.task_payload import PipelineTask
from models.task_spec import TaskSpec
from models.task_status import TaskState
from services.task_service import TaskService


class SleepHandler:
    def handle(self, task_id: int, task: PipelineTask) -> None:
        if task.spec == TaskSpec.NOOP_SLEEP:
            time.sleep(5)
            return
        raise ValueError("unsupported spec")


def test_worker_executes_sleep_task(postgres_engine: Engine) -> None:
    store = TaskStatusStore(engine=postgres_engine)
    queue = InMemoryTaskQueue()
    service = TaskService(store=store, queue=queue, guard=IdempotencyGuard(engine=postgres_engine))
    runtime = WorkerRuntime(queue=queue, store=store, handler=SleepHandler())
    runtime.start()

    task = service.start_task(
        PipelineTask(
            spec=TaskSpec.NOOP_SLEEP,
            pipeline_id="demo",
            source="unit-test",
            task_type="noop",
            arguments={},
            options={},
        )
    )

    deadline = time.time() + 20
    observed_running = False
    while time.time() < deadline:
        record = store.get_by_id(task.task_id)
        if record.state == TaskState.RUNNING:
            observed_running = True
        if record.state == TaskState.SUCCEEDED:
            runtime.stop()
            assert observed_running
            return
        time.sleep(0.25)

    runtime.stop()
    raise AssertionError("task did not complete in time")
