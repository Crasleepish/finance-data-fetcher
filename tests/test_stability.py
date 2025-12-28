from __future__ import annotations

import threading
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.engine import Engine

from infra.db.tables import task_table
from infra.idempotency.guard import IdempotencyGuard, generate_idempotency_key
from infra.queue.in_memory import InMemoryTaskQueue
from infra.task_state.store import TaskStatusStore
from models.task_payload import PipelineTask
from models.task_spec import TaskSpec
from models.task_status import TaskState
from services.task_service import TaskService


def _task_payload() -> PipelineTask:
    return PipelineTask(
        spec=TaskSpec.NOOP_SLEEP,
        pipeline_id="demo",
        source="unit-test",
        task_type="noop",
        arguments={},
        options={},
    )


def test_concurrent_start_idempotent(postgres_engine: Engine) -> None:
    store = TaskStatusStore(engine=postgres_engine)
    queue = InMemoryTaskQueue()
    guard = IdempotencyGuard(engine=postgres_engine)
    service = TaskService(store=store, queue=queue, guard=guard)

    payload = _task_payload()
    key = generate_idempotency_key(payload)

    def worker() -> None:
        service.start_task(payload)

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    with postgres_engine.begin() as connection:
        count = connection.execute(
            select(task_table.c.task_id).where(task_table.c.idempotency_key == key)
        ).all()
    assert len(count) == 1


def test_progress_monotonic(postgres_engine: Engine) -> None:
    store = TaskStatusStore(engine=postgres_engine)
    payload = _task_payload()
    task = store.create_task(
        spec=payload.spec,
        idempotency_key="key-progress",
        attempt=1,
        task_payload=payload.model_dump(mode="json"),
    )

    record = store.update_progress(task.task_id, Decimal("10"))
    assert record.progress == Decimal("10.00")

    record = store.update_progress(task.task_id, Decimal("5"))
    assert record.progress == Decimal("10.00")


def test_terminal_state_immutable(postgres_engine: Engine) -> None:
    store = TaskStatusStore(engine=postgres_engine)
    payload = _task_payload()
    task = store.create_task(
        spec=payload.spec,
        idempotency_key="key-terminal",
        attempt=1,
        task_payload=payload.model_dump(mode="json"),
    )
    store.update_state(task.task_id, TaskState.RUNNING)
    store.update_state(task.task_id, TaskState.SUCCEEDED)

    try:
        store.update_state(task.task_id, TaskState.RUNNING)
        raised = False
    except ValueError:
        raised = True

    assert raised is True
