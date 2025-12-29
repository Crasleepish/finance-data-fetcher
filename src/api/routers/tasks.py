from __future__ import annotations

import logging
from typing import cast

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from core.pipeline.validation import ensure_hashable
from infra.task_state.store import TaskStatusStore
from models.task_payload import PipelineTask
from models.task_status import TaskState, TaskStatusRecord
from services.task_service import TaskService

router = APIRouter(prefix="/tasks", tags=["tasks"])
logger = logging.getLogger(__name__)


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


class TaskStartResponse(BaseModel):
    """Response payload for task start requests."""

    task_id: int
    state: TaskState
    idempotency_key: str


class TaskRunningResponse(BaseModel):
    """Response payload for running tasks."""

    task_id: int
    state: TaskState
    progress: str
    started_at: str | None
    error: str | None


def get_task_store(request: Request) -> TaskStatusStore:
    """Provide TaskStatusStore from app state."""
    return cast(TaskStatusStore, request.app.state.task_store)


def get_task_service(request: Request) -> TaskService:
    """Provide TaskService from app state."""
    return cast(TaskService, request.app.state.task_service)


@router.post("/start", response_model=TaskStartResponse)
def start_task(
    payload: PipelineTask,
    request: Request,
    service: TaskService = Depends(get_task_service),
) -> TaskStartResponse:
    """Start a task asynchronously and return its id."""
    arguments_digest = ensure_hashable(payload.arguments)
    options_digest = ensure_hashable(payload.options)
    caller = request.headers.get("x-caller")
    logger.info(
        "task start request",
        extra={
            "pipeline_id": payload.pipeline_id or "selector",
            "arguments_digest": arguments_digest,
            "options_digest": options_digest,
            "caller": caller,
        },
    )
    try:
        record = service.start_task(payload)
    except Exception:
        logger.exception(
            "task start failed",
            extra={"pipeline_id": payload.pipeline_id},
        )
        raise HTTPException(status_code=500, detail="task start failed")
    return TaskStartResponse(
        task_id=record.task_id,
        state=record.state,
        idempotency_key=record.idempotency_key,
    )


@router.get("/running", response_model=list[TaskRunningResponse])
def list_running(store: TaskStatusStore = Depends(get_task_store)) -> list[TaskRunningResponse]:
    """List active task runs."""
    records = store.list_running()
    return [
        TaskRunningResponse(
            task_id=record.task_id,
            state=record.state,
            progress=str(record.progress),
            started_at=record.started_at.isoformat() if record.started_at else None,
            error=record.error,
        )
        for record in records
    ]


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
