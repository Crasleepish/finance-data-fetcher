from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field

from models.task_spec import TaskSpec


class TaskState(StrEnum):
    """Lifecycle states for background tasks."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class TaskStatusRecord(BaseModel):
    """Immutable task status record returned from persistence layer."""

    model_config = ConfigDict(frozen=True)

    task_id: int = Field(..., ge=1)
    idempotency_key: str
    spec: TaskSpec
    state: TaskState
    attempt: int = Field(..., ge=1)
    progress: Decimal = Field(..., ge=Decimal("0"), le=Decimal("100"))
    error: str | None = None
    created_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    last_heartbeat_at: datetime | None = None
