from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from hashlib import sha256
from typing import Any, Mapping

from pydantic import BaseModel
from sqlalchemy import Connection, Engine, Table, func, insert, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql import Select

from infra.db.engine import transaction
from infra.db.tables import task_table
from infra.task_state.store import _row_to_task_status
from models.task_status import TaskState, TaskStatusRecord


class IdempotencyInput(BaseModel):
    """Inputs used to build a deterministic idempotency key."""
    spec: str
    source: str
    start_date: date | None = None
    end_date: date | None = None
    params: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class IdempotencyGuard:
    """Guard to prevent concurrent duplicate task runs for the same logic."""
    engine: Engine
    table: Table = task_table

    def start_or_get_task(self, payload: IdempotencyInput) -> TaskStatusRecord:
        """Return active task for key or create a new PENDING run."""
        idempotency_key = generate_idempotency_key(payload)
        with transaction(self.engine) as connection:
            existing = _select_active_by_key(self.table, idempotency_key)
            row = connection.execute(existing).mappings().first()
            if row:
                return _row_to_task_status(row)

            attempt = _next_attempt(connection, self.table, idempotency_key)
            stmt = (
                insert(self.table)
                .values(
                    idempotency_key=idempotency_key,
                    spec=payload.spec,
                    state=TaskState.PENDING.value,
                    attempt=attempt,
                    progress=0,
                    created_at=_utc_now(),
                )
                .returning(self.table)
            )

            try:
                row = connection.execute(stmt).mappings().one()
            except IntegrityError:
                row = connection.execute(existing).mappings().one()

        return _row_to_task_status(row)


def generate_idempotency_key(payload: IdempotencyInput) -> str:
    """Generate a SHA256 hex key from normalized input payload."""
    raw = payload.model_dump(mode="json")
    packed = json.dumps(raw, sort_keys=True, separators=(",", ":"))
    return sha256(packed.encode("utf-8")).hexdigest()


def _select_active_by_key(table: Table, idempotency_key: str) -> Select:
    """Query for active runs (PENDING/RUNNING) by idempotency key."""
    return select(table).where(
        table.c.idempotency_key == idempotency_key,
        table.c.state.in_([TaskState.PENDING.value, TaskState.RUNNING.value]),
    )


def _next_attempt(connection: Connection, table: Table, idempotency_key: str) -> int:
    """Compute next attempt number for an idempotency key."""
    stmt = select(func.max(table.c.attempt)).where(table.c.idempotency_key == idempotency_key)
    current = connection.execute(stmt).scalar()
    return (current or 0) + 1


def _utc_now() -> datetime:
    """UTC timestamp helper."""
    return datetime.now(timezone.utc)
