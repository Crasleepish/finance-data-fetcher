from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy.engine import Engine

from infra.idempotency.guard import IdempotencyGuard
from infra.task_state.store import TaskStatusStore
from models.task_payload import PipelineTask
from models.task_spec import TaskSpec
from models.task_status import TaskState


def test_task_status_lifecycle(postgres_engine: Engine) -> None:
    store = TaskStatusStore(engine=postgres_engine)
    task = store.create_task(TaskSpec.NOOP_SLEEP, "key-1", attempt=1)

    assert task.state == TaskState.PENDING
    assert task.progress == Decimal("0.00")

    task = store.update_state(task.task_id, TaskState.RUNNING)
    assert task.started_at is not None

    task = store.update_progress(task.task_id, Decimal("42.5"))
    assert task.progress == Decimal("42.50")

    running = store.list_running()
    assert {record.task_id for record in running} == {task.task_id}

    task = store.update_state(task.task_id, TaskState.SUCCEEDED)
    assert task.finished_at is not None

    running = store.list_running()
    assert running == []


def test_invalid_transition(postgres_engine: Engine) -> None:
    store = TaskStatusStore(engine=postgres_engine)
    task = store.create_task(TaskSpec.NOOP_SLEEP, "key-2", attempt=1)

    with pytest.raises(ValueError, match="Invalid transition"):
        store.update_state(task.task_id, TaskState.SUCCEEDED)


def test_idempotency_guard_reuse_and_retry(postgres_engine: Engine) -> None:
    guard = IdempotencyGuard(engine=postgres_engine)
    store = TaskStatusStore(engine=postgres_engine)

    payload = PipelineTask(
        spec=TaskSpec.NOOP_SLEEP,
        pipeline_id="demo",
        source="sina",
        task_type="noop",
        arguments={"params": {"market": "cn"}},
        options={},
    )

    decision = guard.check_or_prepare(payload)
    assert decision.existing is None
    task = store.create_task(
        spec=payload.spec,
        idempotency_key=decision.idempotency_key,
        attempt=decision.attempt or 1,
    )

    again = guard.check_or_prepare(payload)
    assert again.existing is not None
    assert again.existing.task_id == task.task_id

    store.update_state(task.task_id, TaskState.RUNNING)
    store.update_state(task.task_id, TaskState.SUCCEEDED)

    rerun_decision = guard.check_or_prepare(payload)
    assert rerun_decision.existing is None
    assert rerun_decision.attempt == 2
