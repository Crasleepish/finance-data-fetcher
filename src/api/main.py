from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from api.routers.calendar import router as calendar_router
from api.routers.tasks import router as tasks_router
from config.loader import load_config
from core.pipeline.registry import PipelineRegistry
from infra.db.engine import create_engine_from_config
from infra.idempotency.guard import IdempotencyGuard
from infra.logging import setup_logging
from infra.queue.in_memory import InMemoryTaskQueue
from infra.task_state.store import TaskStatusStore
from infra.worker_runtime.runtime import WorkerRuntime
from services.calendar_service import build_calendar_service
from services.task_service import TaskService
from services.worker_handler import PipelineTaskHandler

logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    """Create FastAPI app with configured dependencies and routes."""

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        config = load_config()
        setup_logging(config.logging)
        logger.info("application startup")
        app.state.calendar_service = build_calendar_service(config)
        engine = create_engine_from_config(config.database)
        task_store = TaskStatusStore(engine=engine)
        task_queue = InMemoryTaskQueue()
        guard = IdempotencyGuard(engine=engine)
        registry = PipelineRegistry()
        app.state.task_store = task_store
        app.state.task_service = TaskService(store=task_store, queue=task_queue, guard=guard)
        app.state.worker_runtime = WorkerRuntime(
            queue=task_queue,
            store=task_store,
            handler=PipelineTaskHandler(registry=registry),
        )
        app.state.pipeline_registry = registry
        app.state.worker_runtime.start()
        yield
        app.state.worker_runtime.stop()

    app = FastAPI(lifespan=lifespan)
    app.include_router(calendar_router)
    app.include_router(tasks_router)
    return app


app = create_app()
