from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from api.routers.calendar import router as calendar_router
from api.routers.tasks import router as tasks_router
from config.loader import load_config
from infra.db.engine import create_engine_from_config
from infra.logging import setup_logging
from infra.queue.in_memory import InMemoryTaskQueue
from infra.task_state.store import TaskStatusStore
from infra.worker_runtime.runtime import WorkerRuntime
from models.task_spec import TaskSpec
from services.calendar_service import build_calendar_service
from services.task_service import TaskService

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
        app.state.task_store = task_store
        app.state.task_service = TaskService(store=task_store, queue=task_queue)
        app.state.worker_runtime = WorkerRuntime(
            queue=task_queue,
            store=task_store,
            handler=_TaskHandler(),
        )
        app.state.worker_runtime.start()
        yield
        app.state.worker_runtime.stop()

    app = FastAPI(lifespan=lifespan)
    app.include_router(calendar_router)
    app.include_router(tasks_router)
    return app


class _TaskHandler:
    """Internal task handler used by the worker runtime."""

    def handle(self, task_id: int, spec: TaskSpec) -> None:
        if spec == TaskSpec.NOOP_SLEEP:
            import time

            time.sleep(5)
            return
        raise ValueError(f"Unknown task spec: {spec}")


app = create_app()
