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
from infra.db.tables import (
    adj_factor,
    etf_info,
    fund_hist,
    fund_info,
    fundamental_data,
    index_hist,
    index_info,
    stock_hist_unadj,
    stock_info,
    test_messages,
)
from infra.idempotency.guard import IdempotencyGuard
from infra.logging import setup_logging
from infra.queue.in_memory import InMemoryTaskQueue
from infra.task_state.store import TaskStatusStore
from infra.tushare.client import TushareProClient
from infra.worker_runtime.runtime import WorkerRuntime
from services.calendar_service import build_calendar_service
from services.pipeline_selector import PipelineSelector, load_pipeline_mapping
from services.pipelines.adj_factor_pipeline import AdjFactorPipeline
from services.pipelines.etf_info_pipeline import EtfInfoPipeline
from services.pipelines.fund_hist_index_pipeline import FundHistIndexPipeline
from services.pipelines.fund_hist_money_pipeline import FundHistMoneyPipeline
from services.pipelines.fund_info_pipeline import FundInfoPipeline
from services.pipelines.fundamental_data_pipeline import FundamentalDataPipeline
from services.pipelines.fundamental_data_single_pipeline import FundamentalDataSinglePipeline
from services.pipelines.index_hist_bond_pipeline import IndexHistBondPipeline
from services.pipelines.index_hist_gold_pipeline import IndexHistGoldPipeline
from services.pipelines.index_hist_stock_pipeline import IndexHistStockPipeline
from services.pipelines.index_info_pipeline import IndexInfoPipeline
from services.pipelines.stock_hist_unadj_pipeline import StockHistUnadjPipeline
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
        retry_policy = RetryPolicy()
        tushare_client = TushareProClient(config.tushare.token_private)
        tushare_public_client = TushareProClient(config.tushare.token_public)
        registry.register(
            "stock_info",
            StockInfoPipeline(
                client=tushare_client,
                retry_policy=retry_policy,
            ),
        )
        registry.register(
            "stock_hist_unadj",
            StockHistUnadjPipeline(
                calendar=app.state.calendar_service.calendar,
                client=tushare_client,
                retry_policy=retry_policy,
            ),
        )
        registry.register(
            "adj_factor",
            AdjFactorPipeline(
                calendar=app.state.calendar_service.calendar,
                client=tushare_client,
                retry_policy=retry_policy,
            ),
        )
        registry.register(
            "fund_info",
            FundInfoPipeline(
                client=tushare_client,
                retry_policy=retry_policy,
            ),
        )
        registry.register(
            "etf_info",
            EtfInfoPipeline(
                client=tushare_client,
                retry_policy=retry_policy,
            ),
        )
        registry.register(
            "fund_hist_index",
            FundHistIndexPipeline(
                calendar=app.state.calendar_service.calendar,
                client=tushare_client,
                retry_policy=retry_policy,
                engine=engine,
                fund_info_table=fund_info,
            ),
        )
        registry.register(
            "fund_hist_money",
            FundHistMoneyPipeline(
                calendar=app.state.calendar_service.calendar,
                client=tushare_client,
                retry_policy=retry_policy,
                engine=engine,
                fund_info_table=fund_info,
                codes_raw=config.data.fund.money,
            ),
        )
        registry.register(
            "index_info",
            IndexInfoPipeline(
                client=tushare_client,
                retry_policy=retry_policy,
            ),
        )
        registry.register(
            "index_hist_stock",
            IndexHistStockPipeline(
                calendar=app.state.calendar_service.calendar,
                client=tushare_client,
                retry_policy=retry_policy,
                engine=engine,
                index_info_table=index_info,
                codes_raw=config.data.index.stock,
            ),
        )
        registry.register(
            "index_hist_bond",
            IndexHistBondPipeline(
                calendar=app.state.calendar_service.calendar,
                retry_policy=retry_policy,
                engine=engine,
                index_info_table=index_info,
                codes_raw=config.data.index.bond,
            ),
        )
        registry.register(
            "index_hist_gold",
            IndexHistGoldPipeline(
                calendar=app.state.calendar_service.calendar,
                client=tushare_client,
                retry_policy=retry_policy,
                engine=engine,
                index_info_table=index_info,
                codes_raw=config.data.index.gold,
            ),
        )
        registry.register(
            "fundamental_data",
            FundamentalDataPipeline(
                client=tushare_public_client,
                retry_policy=retry_policy,
                engine=engine,
                table=fundamental_data,
            ),
        )
        registry.register(
            "fundamental_data_single",
            FundamentalDataSinglePipeline(
                client=tushare_client,
                retry_policy=retry_policy,
                engine=engine,
                stock_table=stock_info,
                fundamental_table=fundamental_data,
            ),
        )
        selector = PipelineSelector(mapping=load_pipeline_mapping(config.pipeline_mapping_path))
        repo = Repository(engine=engine, table=test_messages)
        repo_by_pipeline = {
            "stock_info": Repository(engine=engine, table=stock_info),
            "stock_hist_unadj": Repository(engine=engine, table=stock_hist_unadj),
            "adj_factor": Repository(engine=engine, table=adj_factor),
            "index_info": Repository(engine=engine, table=index_info),
            "index_hist_stock": Repository(engine=engine, table=index_hist),
            "index_hist_bond": Repository(engine=engine, table=index_hist),
            "index_hist_gold": Repository(engine=engine, table=index_hist),
            "fund_info": Repository(engine=engine, table=fund_info),
            "etf_info": Repository(engine=engine, table=etf_info),
            "fund_hist_index": Repository(engine=engine, table=fund_hist),
            "fund_hist_money": Repository(engine=engine, table=fund_hist),
            "fundamental_data": Repository(engine=engine, table=fundamental_data),
            "fundamental_data_single": Repository(engine=engine, table=fundamental_data),
        }
        workflow = WorkflowEngine(
            store=task_store,
            registry=registry,
            selector=selector,
            repo=repo,
            repo_by_pipeline=repo_by_pipeline,
            upsert_keys_by_pipeline={
                "stock_info": ["stock_code"],
                "stock_hist_unadj": ["stock_code", "date"],
                "adj_factor": ["stock_code", "date"],
                "index_info": ["index_code"],
                "index_hist_stock": ["index_code", "date"],
                "index_hist_bond": ["index_code", "date"],
                "index_hist_gold": ["index_code", "date"],
                "fund_info": ["fund_code"],
                "etf_info": ["etf_code"],
                "fund_hist_index": ["fund_code", "date"],
                "fund_hist_money": ["fund_code", "date"],
                "fundamental_data": ["stock_code", "report_date"],
                "fundamental_data_single": ["stock_code", "report_date"],
            },
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
