from __future__ import annotations

import logging
from typing import cast

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from models.data_query import DataListResponse, DataResultsResponse, ErrorResponse
from services.data_query_service import DataQueryService

router = APIRouter(prefix="/data", tags=["data"])
logger = logging.getLogger(__name__)


class ResultsQuery(BaseModel):
    """Query parameters for results endpoint."""

    data_type: str
    asset_code: str
    start_date: str
    end_date: str
    limit: int | None = None
    order: str | None = None


class ListQuery(BaseModel):
    """Query parameters for list endpoint."""

    data_type: str
    keyword: str | None = None
    page: int | None = None
    page_size: int | None = None
    order: str | None = None


def get_data_query_service(request: Request) -> DataQueryService:
    """Provide DataQueryService from app state."""
    return cast(DataQueryService, request.app.state.data_query_service)


def _error_response(message: str) -> JSONResponse:
    payload = ErrorResponse(success=False, data=None, error=message, meta=None)
    return JSONResponse(status_code=400, content=payload.model_dump(mode="json"))


@router.get(
    "/results",
    response_model=DataResultsResponse,
    operation_id="get_data_results",
    summary="Get historical data points",
    responses={
        400: {
            "description": "Request validation failed",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "success": {"type": "boolean"},
                            "data": {"type": "object", "nullable": True},
                            "error": {"type": "string"},
                            "meta": {"type": "object", "nullable": True},
                        },
                    },
                    "example": {
                        "success": False,
                        "data": None,
                        "error": "request validation failed",
                        "meta": None,
                    },
                }
            },
        }
    },
)
def get_results(
    query: ResultsQuery = Depends(),
    service: DataQueryService = Depends(get_data_query_service),
) -> DataResultsResponse | JSONResponse:
    """Get historical OHLC results for an asset."""
    try:
        return service.get_results(
            data_type=query.data_type,
            asset_code=query.asset_code,
            start_date=query.start_date,
            end_date=query.end_date,
            limit=query.limit,
            order=query.order,
        )
    except ValueError as exc:
        logger.info("data results validation failed", extra={"error": str(exc)})
        return _error_response(str(exc))


@router.get(
    "/list",
    response_model=DataListResponse,
    operation_id="list_data_assets",
    summary="List available assets",
    responses={
        400: {
            "description": "Request validation failed",
            "content": {
                "application/json": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "success": {"type": "boolean"},
                            "data": {"type": "object", "nullable": True},
                            "error": {"type": "string"},
                            "meta": {"type": "object", "nullable": True},
                        },
                    },
                    "example": {
                        "success": False,
                        "data": None,
                        "error": "request validation failed",
                        "meta": None,
                    },
                }
            },
        }
    },
)
def list_assets(
    query: ListQuery = Depends(),
    service: DataQueryService = Depends(get_data_query_service),
) -> DataListResponse | JSONResponse:
    """List assets for a data type with optional keyword search."""
    try:
        return service.list_assets(
            data_type=query.data_type,
            keyword=query.keyword,
            page=query.page,
            page_size=query.page_size,
            order=query.order,
        )
    except ValueError as exc:
        logger.info("data list validation failed", extra={"error": str(exc)})
        return _error_response(str(exc))
