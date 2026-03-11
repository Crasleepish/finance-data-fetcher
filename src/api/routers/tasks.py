from __future__ import annotations

import logging
from typing import cast

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from core.pipeline.validation import ensure_hashable
from core.task_query.validation import (
    normalize_page,
    normalize_page_size,
    parse_datetime_range,
)
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


class TaskListQuery(BaseModel):
    """Query parameters for task list endpoint."""

    task_id: int | None = None
    spec: str | None = None
    state: list[TaskState] | None = None
    created_at_from: str | None = None
    created_at_to: str | None = None
    started_at_from: str | None = None
    started_at_to: str | None = None
    finished_at_from: str | None = None
    finished_at_to: str | None = None
    last_heartbeat_at_from: str | None = None
    last_heartbeat_at_to: str | None = None
    page: int | None = None
    page_size: int | None = None


class TaskListItem(BaseModel):
    """Task list item including task payload."""

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
    task_payload: dict


class TaskListMeta(BaseModel):
    """Metadata for task list response."""

    page: int
    page_size: int
    total: int


class TaskListResponse(BaseModel):
    """Response payload for task list endpoint."""

    items: list[TaskListItem]
    meta: TaskListMeta


def get_task_store(request: Request) -> TaskStatusStore:
    """Provide TaskStatusStore from app state."""
    return cast(TaskStatusStore, request.app.state.task_store)


def get_task_service(request: Request) -> TaskService:
    """Provide TaskService from app state."""
    return cast(TaskService, request.app.state.task_service)


@router.post(
    "/start",
    response_model=TaskStartResponse,
    operation_id="start_task",
    summary="Start a task",
    responses={
        400: {
            "description": "Request validation failed",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": {"detail": {"type": "string"}},
                    },
                    "example": {"detail": "request validation failed"},
                }
            },
        },
        500: {
            "description": "Task start failed",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": {"detail": {"type": "string"}},
                    },
                    "example": {"detail": "task start failed"},
                }
            },
        },
    },
)
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


@router.get(
    "/running",
    response_model=list[TaskRunningResponse],
    operation_id="list_running_tasks",
    summary="List running tasks",
    responses={
        400: {
            "description": "Request validation failed",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": {"detail": {"type": "string"}},
                    },
                    "example": {"detail": "request validation failed"},
                }
            },
        },
        500: {
            "description": "Internal server error",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": {"detail": {"type": "string"}},
                    },
                    "example": {"detail": "internal server error"},
                }
            },
        },
    },
)
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


@router.get(
    "/list",
    response_model=TaskListResponse,
    operation_id="list_tasks",
    summary="List tasks",
    responses={
        400: {
            "description": "Request validation failed",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": {"detail": {"type": "string"}},
                    },
                    "example": {"detail": "request validation failed"},
                }
            },
        },
        500: {
            "description": "Internal server error",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": {"detail": {"type": "string"}},
                    },
                    "example": {"detail": "internal server error"},
                }
            },
        },
    },
)
def list_tasks(
    query: TaskListQuery = Depends(),
    store: TaskStatusStore = Depends(get_task_store),
) -> TaskListResponse:
    """List tasks with filters and pagination."""
    try:
        page = normalize_page(query.page)
        page_size = normalize_page_size(query.page_size)
        created_at_range = parse_datetime_range(query.created_at_from, query.created_at_to)
        started_at_range = parse_datetime_range(query.started_at_from, query.started_at_to)
        finished_at_range = parse_datetime_range(query.finished_at_from, query.finished_at_to)
        heartbeat_range = parse_datetime_range(
            query.last_heartbeat_at_from, query.last_heartbeat_at_to
        )
        states = list(query.state or [])

        rows, total = store.list_tasks(
            task_id=query.task_id,
            spec=query.spec,
            states=states,
            created_at_range=created_at_range,
            started_at_range=started_at_range,
            finished_at_range=finished_at_range,
            last_heartbeat_at_range=heartbeat_range,
            page=page,
            page_size=page_size,
        )
    except ValueError as exc:
        logger.info("task list validation failed", extra={"error": str(exc)})
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception:
        logger.exception("task list failed")
        raise HTTPException(status_code=500, detail="internal server error")

    items = [_row_to_list_item(row) for row in rows]
    return TaskListResponse(
        items=items,
        meta=TaskListMeta(page=page, page_size=page_size, total=total),
    )


@router.get(
    "/{task_id}",
    response_model=TaskStatusResponse,
    operation_id="get_task_status",
    summary="Get task status",
    responses={
        400: {
            "description": "Request validation failed",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": {"detail": {"type": "string"}},
                    },
                    "example": {"detail": "request validation failed"},
                }
            },
        },
        500: {
            "description": "Internal server error",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": {"detail": {"type": "string"}},
                    },
                    "example": {"detail": "internal server error"},
                }
            },
        },
    },
)
def get_task_status(
    task_id: int,
    store: TaskStatusStore = Depends(get_task_store),
) -> TaskStatusResponse:
    """Get task status by id."""
    record = store.get_by_id(task_id)
    return _to_response(record)


@router.post(
    "/cancel/{task_id}",
    response_model=TaskStatusResponse,
    operation_id="cancel_task",
    summary="Cancel a task",
    responses={
        400: {
            "description": "Request validation failed",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": {"detail": {"type": "string"}},
                    },
                    "example": {"detail": "request validation failed"},
                }
            },
        },
        500: {
            "description": "Task cancel failed",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": {"detail": {"type": "string"}},
                    },
                    "example": {"detail": "task cancel failed"},
                }
            },
        },
    },
)
def cancel_task(
    task_id: int,
    service: TaskService = Depends(get_task_service),
) -> TaskStatusResponse:
    """Cancel a pending or running task by id."""
    try:
        record = service.cancel_task(task_id)
    except Exception:
        logger.exception("task cancel failed", extra={"task_id": task_id})
        raise HTTPException(status_code=500, detail="task cancel failed")
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


def _row_to_list_item(row: dict) -> TaskListItem:
    return TaskListItem(
        task_id=row["task_id"],
        idempotency_key=row["idempotency_key"],
        spec=row["spec"],
        state=TaskState(row["state"]),
        attempt=row["attempt"],
        progress=str(row["progress"]),
        error=row["error"],
        created_at=row["created_at"].isoformat(),
        started_at=row["started_at"].isoformat() if row["started_at"] else None,
        finished_at=row["finished_at"].isoformat() if row["finished_at"] else None,
        last_heartbeat_at=(
            row["last_heartbeat_at"].isoformat() if row["last_heartbeat_at"] else None
        ),
        task_payload=dict(row["task_payload"]),
    )
