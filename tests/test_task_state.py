from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy.engine import Engine

from infra.idempotency.guard import IdempotencyGuard, IdempotencyInput
from infra.task_state.store import TaskStatusStore
from models.task_status import TaskState


def test_task_status_lifecycle(postgres_engine: Engine) -> None:
    store = TaskStatusStore(engine=postgres_engine)
    task = store.create_task("fetch_stock_info", "key-1", attempt=1)

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
    task = store.create_task("fetch_stock_info", "key-2", attempt=1)

    with pytest.raises(ValueError, match="Invalid transition"):
        store.update_state(task.task_id, TaskState.SUCCEEDED)


def test_idempotency_guard_reuse_and_retry(postgres_engine: Engine) -> None:
    guard = IdempotencyGuard(engine=postgres_engine)
    store = TaskStatusStore(engine=postgres_engine)

    payload = IdempotencyInput(
        spec="fetch_stock_info",
        source="sina",
        start_date=None,
        end_date=None,
        params={"market": "cn"},
    )

    first = guard.start_or_get_task(payload)
    again = guard.start_or_get_task(payload)
    assert first.task_id == again.task_id
    assert first.attempt == 1

    store.update_state(first.task_id, TaskState.RUNNING)
    store.update_state(first.task_id, TaskState.SUCCEEDED)

    rerun = guard.start_or_get_task(payload)
    assert rerun.task_id != first.task_id
    assert rerun.attempt == 2

    row = store.get_by_id(rerun.task_id)
    assert row.state == TaskState.PENDING
