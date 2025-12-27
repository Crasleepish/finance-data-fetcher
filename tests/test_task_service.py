from __future__ import annotations

from sqlalchemy.engine import Engine

from infra.idempotency.guard import IdempotencyGuard
from infra.queue.in_memory import InMemoryTaskQueue
from infra.task_state.store import TaskStatusStore
from models.task_payload import PipelineTask
from models.task_spec import TaskSpec
from services.task_service import TaskService


def test_repeated_start_returns_existing_task(postgres_engine: Engine) -> None:
    store = TaskStatusStore(engine=postgres_engine)
    queue = InMemoryTaskQueue()
    guard = IdempotencyGuard(engine=postgres_engine)
    service = TaskService(store=store, queue=queue, guard=guard)

    task_payload = PipelineTask(
        spec=TaskSpec.PIPELINE,
        pipeline_id="dummy",
        source="unit-test",
        task_type="noop",
        arguments={"params": {"start_date": "2024-01-01"}},
        options={},
    )

    first = service.start_task(task_payload)
    second = service.start_task(task_payload)

    assert first.task_id == second.task_id
    assert first.attempt == second.attempt == 1
    assert first.state == second.state
