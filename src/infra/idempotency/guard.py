from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha256

from sqlalchemy import Connection, Engine, Table, func, select
from sqlalchemy.sql import Select

from infra.db.engine import transaction
from infra.db.tables import task_table
from infra.task_state.store import _row_to_task_status
from models.task_payload import IdempotencyInput
from models.task_status import TaskState, TaskStatusRecord


@dataclass(frozen=True)
class IdempotencyGuard:
    """Guard to prevent concurrent duplicate task runs for the same logic."""

    engine: Engine
    table: Table = task_table

    def check_or_prepare(self, payload: IdempotencyInput) -> IdempotencyDecision:
        """Check for active run and prepare key/attempt for new task if needed."""
        idempotency_key = generate_idempotency_key(payload)
        with transaction(self.engine) as connection:
            existing = _select_active_by_key(self.table, idempotency_key)
            row = connection.execute(existing).mappings().first()
            if row:
                return IdempotencyDecision(
                    idempotency_key=idempotency_key,
                    existing=_row_to_task_status(row),
                    attempt=None,
                )

            attempt = _next_attempt(connection, self.table, idempotency_key)
            return IdempotencyDecision(
                idempotency_key=idempotency_key,
                existing=None,
                attempt=attempt,
            )


@dataclass(frozen=True)
class IdempotencyDecision:
    """Decision result for idempotency checks."""

    idempotency_key: str
    existing: TaskStatusRecord | None
    attempt: int | None


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
