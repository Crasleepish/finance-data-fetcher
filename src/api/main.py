from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from api.routers.calendar import router as calendar_router
from config.loader import load_config
from infra.logging import setup_logging
from services.calendar_service import build_calendar_service

logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    """Create FastAPI app with configured dependencies and routes."""
    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        config = load_config()
        setup_logging(config.logging)
        logger.info("application startup")
        app.state.calendar_service = build_calendar_service(config)
        yield

    app = FastAPI(lifespan=lifespan)
    app.include_router(calendar_router)
    return app


app = create_app()
