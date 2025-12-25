from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import Engine, Table, insert, select, update
from sqlalchemy.engine import RowMapping

from infra.db.engine import transaction
from infra.db.tables import task_table
from infra.task_state.state_machine import TaskStateMachine
from models.task_status import TaskState, TaskStatusRecord

_ACTIVE_STATES = (TaskState.PENDING, TaskState.RUNNING)


@dataclass(frozen=True)
class TaskStatusStore:
    engine: Engine
    table: Table = task_table
    state_machine: TaskStateMachine = TaskStateMachine()

    def create_task(self, spec: str, idempotency_key: str, attempt: int) -> TaskStatusRecord:
        now = _utc_now()
        stmt = (
            insert(self.table)
            .values(
                idempotency_key=idempotency_key,
                spec=spec,
                state=TaskState.PENDING.value,
                attempt=attempt,
                progress=Decimal("0"),
                created_at=now,
            )
            .returning(self.table)
        )
        with transaction(self.engine) as connection:
            row = connection.execute(stmt).mappings().one()
        return _row_to_task_status(row)

    def update_progress(self, task_id: int, progress: Decimal) -> TaskStatusRecord:
        normalized = _normalize_progress(progress)
        stmt = (
            update(self.table)
            .where(self.table.c.task_id == task_id)
            .values(progress=normalized)
            .returning(self.table)
        )
        with transaction(self.engine) as connection:
            row = connection.execute(stmt).mappings().one()
        return _row_to_task_status(row)

    def update_state(
        self, task_id: int, new_state: TaskState, error: str | None = None
    ) -> TaskStatusRecord:
        with transaction(self.engine) as connection:
            current_row = (
                connection.execute(
                    select(self.table).where(self.table.c.task_id == task_id).with_for_update()
                )
                .mappings()
                .one()
            )
            current_state = TaskState(current_row["state"])
            self.state_machine.ensure_transition(current_state, new_state)

            updates = _state_updates(new_state, error, current_row)
            row = (
                connection.execute(
                    update(self.table)
                    .where(self.table.c.task_id == task_id)
                    .values(**updates)
                    .returning(self.table)
                )
                .mappings()
                .one()
            )
        return _row_to_task_status(row)

    def list_running(self) -> list[TaskStatusRecord]:
        stmt = select(self.table).where(self.table.c.state.in_([s.value for s in _ACTIVE_STATES]))
        with transaction(self.engine) as connection:
            rows = connection.execute(stmt).mappings().all()
        return [_row_to_task_status(row) for row in rows]

    def get_by_id(self, task_id: int) -> TaskStatusRecord:
        stmt = select(self.table).where(self.table.c.task_id == task_id)
        with transaction(self.engine) as connection:
            row = connection.execute(stmt).mappings().one()
        return _row_to_task_status(row)


def _state_updates(
    new_state: TaskState, error: str | None, current_row: RowMapping
) -> dict[str, Any]:
    updates: dict[str, Any] = {"state": new_state.value}
    now = _utc_now()

    if new_state == TaskState.RUNNING and current_row["started_at"] is None:
        updates["started_at"] = now

    if new_state in {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED}:
        updates["finished_at"] = now

    if error is not None:
        updates["error"] = error
    elif new_state == TaskState.SUCCEEDED:
        updates["error"] = None

    return updates


def _normalize_progress(progress: Decimal | float | int) -> Decimal:
    if not isinstance(progress, Decimal):
        progress = Decimal(str(progress))
    if progress < 0 or progress > 100:
        raise ValueError("progress must be between 0 and 100")
    return progress.quantize(Decimal("0.01"))


def _row_to_task_status(row: RowMapping) -> TaskStatusRecord:
    return TaskStatusRecord(
        task_id=row["task_id"],
        idempotency_key=row["idempotency_key"],
        spec=row["spec"],
        state=TaskState(row["state"]),
        attempt=row["attempt"],
        progress=Decimal(row["progress"]),
        error=row["error"],
        created_at=row["created_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
    )


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
