from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from api.routers.calendar import router as calendar_router
from api.routers.tasks import router as tasks_router
from config.loader import load_config
from core.fetch.retry import RetryPolicy
from core.pipeline.registry import PipelineRegistry
from infra.db.engine import create_engine_from_config
from infra.db.repository import Repository
from infra.db.tables import stock_info, test_messages
from infra.idempotency.guard import IdempotencyGuard
from infra.logging import setup_logging
from infra.queue.in_memory import InMemoryTaskQueue
from infra.task_state.store import TaskStatusStore
from infra.tushare.client import TushareProClient
from infra.worker_runtime.runtime import WorkerRuntime
from services.calendar_service import build_calendar_service
from services.pipeline_selector import PipelineSelector, load_pipeline_mapping
from services.pipelines.stock_info_pipeline import StockInfoPipeline
from services.task_service import TaskService
from services.workflow_engine import WorkflowEngine

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
        registry.register(
            "stock_info",
            StockInfoPipeline(
                client=TushareProClient(config.tushare.token),
                retry_policy=RetryPolicy(),
            ),
        )
        selector = PipelineSelector(mapping=load_pipeline_mapping(config.pipeline_mapping_path))
        repo = Repository(engine=engine, table=test_messages)
        repo_by_pipeline = {"stock_info": Repository(engine=engine, table=stock_info)}
        workflow = WorkflowEngine(
            store=task_store,
            registry=registry,
            selector=selector,
            repo=repo,
            repo_by_pipeline=repo_by_pipeline,
            upsert_keys_by_pipeline={"stock_info": ["stock_code"]},
        )
        app.state.task_store = task_store
        app.state.task_service = TaskService(store=task_store, queue=task_queue, guard=guard)
        app.state.worker_runtime = WorkerRuntime(
            queue=task_queue,
            store=task_store,
            handler=workflow,
        )
        app.state.pipeline_registry = registry
        app.state.pipeline_selector = selector
        app.state.workflow_engine = workflow
        app.state.worker_runtime.start()
        yield
        app.state.worker_runtime.stop()

    app = FastAPI(lifespan=lifespan)

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        error_fields = []
        for err in exc.errors():
            loc = err.get("loc", [])
            if len(loc) >= 2:
                error_fields.append(str(loc[1]))
        logger.warning(
            "request validation failed",
            extra={
                "path": request.url.path,
                "error_count": len(exc.errors()),
                "error_fields": sorted(set(error_fields)),
            },
        )
        return JSONResponse(status_code=400, content={"detail": "request validation failed"})

    app.include_router(calendar_router)
    app.include_router(tasks_router)
    return app


app = create_app()
