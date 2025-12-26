from __future__ import annotations

from typing import cast

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel

from infra.task_state.store import TaskStatusStore
from models.task_status import TaskState, TaskStatusRecord

router = APIRouter(prefix="/tasks", tags=["tasks"])


class TaskStatusResponse(BaseModel):
    """Response payload for task status lookups."""

    task_id: int
    idempotency_key: str
    spec: str
    state: TaskState
    attempt: int
    progress: str
    error: str | None
    created_at: str
    started_at: str | None
    finished_at: str | None
    last_heartbeat_at: str | None


def get_task_store(request: Request) -> TaskStatusStore:
    """Provide TaskStatusStore from app state."""
    return cast(TaskStatusStore, request.app.state.task_store)


@router.get("/{task_id}", response_model=TaskStatusResponse)
def get_task_status(
    task_id: int,
    store: TaskStatusStore = Depends(get_task_store),
) -> TaskStatusResponse:
    """Get task status by id."""
    record = store.get_by_id(task_id)
    return _to_response(record)


def _to_response(record: TaskStatusRecord) -> TaskStatusResponse:
    return TaskStatusResponse(
        task_id=record.task_id,
        idempotency_key=record.idempotency_key,
        spec=record.spec.value,
        state=record.state,
        attempt=record.attempt,
        progress=str(record.progress),
        error=record.error,
        created_at=record.created_at.isoformat(),
        started_at=record.started_at.isoformat() if record.started_at else None,
        finished_at=record.finished_at.isoformat() if record.finished_at else None,
        last_heartbeat_at=(
            record.last_heartbeat_at.isoformat() if record.last_heartbeat_at else None
        ),
    )
