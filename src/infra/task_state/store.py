from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from sqlalchemy import Engine, Table, func, insert, select, update
from sqlalchemy.engine import RowMapping

from infra.db.engine import transaction
from infra.db.tables import task_table
from infra.task_state.state_machine import TaskStateMachine
from models.task_payload import PipelineTask
from models.task_spec import TaskSpec
from models.task_status import TaskState, TaskStatusRecord

_ACTIVE_STATES = (TaskState.PENDING, TaskState.RUNNING)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TaskStatusStore:
    """Persistence helper for task status records."""

    engine: Engine
    table: Table = task_table
    state_machine: TaskStateMachine = TaskStateMachine()

    def create_task(
        self,
        spec: TaskSpec,
        idempotency_key: str,
        attempt: int,
        task_payload: dict[str, Any],
    ) -> TaskStatusRecord:
        """Create a new task in PENDING state."""
        now = _utc_now()
        stmt = (
            insert(self.table)
            .values(
                idempotency_key=idempotency_key,
                spec=spec.value,
                state=TaskState.PENDING.value,
                attempt=attempt,
                progress=Decimal("0"),
                created_at=now,
                task_payload=task_payload,
            )
            .returning(self.table)
        )
        try:
            with transaction(self.engine) as connection:
                row = connection.execute(stmt).mappings().one()
        except Exception:
            logger.exception("task status write failed", extra={"operation": "create_task"})
            raise
        return _row_to_task_status(row)

    def update_progress(self, task_id: int, progress: Decimal) -> TaskStatusRecord:
        """Update task progress (0-100)."""
        normalized = _normalize_progress(progress)
        try:
            with transaction(self.engine) as connection:
                current_row = (
                    connection.execute(
                        select(self.table).where(self.table.c.task_id == task_id).with_for_update()
                    )
                    .mappings()
                    .one()
                )
                current_progress = Decimal(current_row["progress"])
                if normalized < current_progress:
                    return _row_to_task_status(current_row)
                row = (
                    connection.execute(
                        update(self.table)
                        .where(self.table.c.task_id == task_id)
                        .values(progress=normalized)
                        .returning(self.table)
                    )
                    .mappings()
                    .one()
                )
        except Exception:
            logger.exception(
                "task status write failed",
                extra={"operation": "update_progress", "task_id": task_id},
            )
            raise
        return _row_to_task_status(row)

    def update_state(
        self, task_id: int, new_state: TaskState, error: str | None = None
    ) -> TaskStatusRecord:
        """Transition task state with FSM validation and timestamp updates."""
        try:
            with transaction(self.engine) as connection:
                current_row = (
                    connection.execute(
                        select(self.table).where(self.table.c.task_id == task_id).with_for_update()
                    )
                    .mappings()
                    .one()
                )
                current_state = TaskState(current_row["state"])
                try:
                    self.state_machine.ensure_transition(current_state, new_state)
                except ValueError:
                    logger.warning(
                        "invalid state transition",
                        extra={
                            "task_id": task_id,
                            "from_state": current_state.value,
                            "to_state": new_state.value,
                        },
                    )
                    raise

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
        except ValueError:
            raise
        except Exception:
            logger.exception(
                "task status write failed",
                extra={"operation": "update_state", "task_id": task_id},
            )
            raise
        return _row_to_task_status(row)

    def update_heartbeat(self, task_id: int) -> TaskStatusRecord:
        """Update task heartbeat timestamp."""
        stmt = (
            update(self.table)
            .where(self.table.c.task_id == task_id)
            .values(last_heartbeat_at=_utc_now())
            .returning(self.table)
        )
        try:
            with transaction(self.engine) as connection:
                row = connection.execute(stmt).mappings().one()
        except Exception:
            logger.exception(
                "task status write failed",
                extra={"operation": "update_heartbeat", "task_id": task_id},
            )
            raise
        return _row_to_task_status(row)

    def list_running(self) -> list[TaskStatusRecord]:
        """List tasks in active states (PENDING/RUNNING)."""
        stmt = select(self.table).where(self.table.c.state.in_([s.value for s in _ACTIVE_STATES]))
        with transaction(self.engine) as connection:
            rows = connection.execute(stmt).mappings().all()
        return [_row_to_task_status(row) for row in rows]

    def get_by_id(self, task_id: int) -> TaskStatusRecord:
        """Fetch a task status record by id."""
        stmt = select(self.table).where(self.table.c.task_id == task_id)
        with transaction(self.engine) as connection:
            row = connection.execute(stmt).mappings().one()
        return _row_to_task_status(row)

    def get_task_payload(self, task_id: int) -> PipelineTask:
        """Fetch task payload for a given id."""
        stmt = select(self.table.c.task_payload).where(self.table.c.task_id == task_id)
        with transaction(self.engine) as connection:
            payload = connection.execute(stmt).scalar_one()
        return PipelineTask.model_validate(payload)

    def list_tasks(
        self,
        *,
        task_id: int | None,
        spec: str | None,
        states: list[TaskState],
        created_at_range: tuple[datetime | None, datetime | None],
        started_at_range: tuple[datetime | None, datetime | None],
        finished_at_range: tuple[datetime | None, datetime | None],
        last_heartbeat_at_range: tuple[datetime | None, datetime | None],
        page: int,
        page_size: int,
    ) -> tuple[list[RowMapping], int]:
        """List tasks with filters and pagination."""
        created_start, created_end = created_at_range
        started_start, started_end = started_at_range
        finished_start, finished_end = finished_at_range
        heartbeat_start, heartbeat_end = last_heartbeat_at_range
        state_values = [state.value for state in states]

        conditions = (
            self.table.c.task_id == task_id if task_id is not None else None,
            self.table.c.spec == spec if spec is not None else None,
            self.table.c.state.in_(state_values) if state_values else None,
            self.table.c.created_at >= created_start if created_start is not None else None,
            self.table.c.created_at <= created_end if created_end is not None else None,
            self.table.c.started_at >= started_start if started_start is not None else None,
            self.table.c.started_at <= started_end if started_end is not None else None,
            self.table.c.finished_at >= finished_start if finished_start is not None else None,
            self.table.c.finished_at <= finished_end if finished_end is not None else None,
            (
                self.table.c.last_heartbeat_at >= heartbeat_start
                if heartbeat_start is not None
                else None
            ),
            (
                self.table.c.last_heartbeat_at <= heartbeat_end
                if heartbeat_end is not None
                else None
            ),
        )
        if heartbeat_start is not None or heartbeat_end is not None:
            conditions = (
                *conditions,
                self.table.c.last_heartbeat_at.is_not(None),
            )
        filters = [condition for condition in conditions if condition is not None]

        stmt = select(self.table)
        count_stmt = select(func.count()).select_from(self.table)
        if filters:
            stmt = stmt.where(*filters)
            count_stmt = count_stmt.where(*filters)

        stmt = (
            stmt.order_by(self.table.c.created_at.desc(), self.table.c.task_id.desc())
            .offset((page - 1) * page_size)
            .limit(page_size)
        )

        with transaction(self.engine) as connection:
            total = int(connection.execute(count_stmt).scalar_one())
            rows = connection.execute(stmt).mappings().all()
        return rows, total


def _state_updates(
    new_state: TaskState, error: str | None, current_row: RowMapping
) -> dict[str, Any]:
    """Compute column updates for a state transition."""
    updates: dict[str, Any] = {"state": new_state.value}
    now = _utc_now()

    if new_state == TaskState.RUNNING and current_row["started_at"] is None:
        updates["started_at"] = now
        updates["last_heartbeat_at"] = now

    if new_state in {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED}:
        updates["finished_at"] = now

    if error is not None:
        updates["error"] = error
    elif new_state == TaskState.SUCCEEDED:
        updates["error"] = None

    return updates


def _normalize_progress(progress: Decimal | float | int) -> Decimal:
    """Normalize and validate progress within 0-100."""
    if not isinstance(progress, Decimal):
        progress = Decimal(str(progress))
    if progress < 0 or progress > 100:
        raise ValueError("progress must be between 0 and 100")
    return progress.quantize(Decimal("0.01"))


def _row_to_task_status(row: RowMapping) -> TaskStatusRecord:
    """Map a DB row to TaskStatusRecord."""
    return TaskStatusRecord(
        task_id=row["task_id"],
        idempotency_key=row["idempotency_key"],
        spec=TaskSpec(row["spec"]),
        state=TaskState(row["state"]),
        attempt=row["attempt"],
        progress=Decimal(row["progress"]),
        error=row["error"],
        created_at=row["created_at"],
        started_at=row["started_at"],
        finished_at=row["finished_at"],
        last_heartbeat_at=row["last_heartbeat_at"],
    )


def _utc_now() -> datetime:
    """UTC timestamp helper."""
    return datetime.now(timezone.utc)
