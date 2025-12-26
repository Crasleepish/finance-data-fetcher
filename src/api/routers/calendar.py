from __future__ import annotations

from datetime import date
from typing import cast

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field

from services.calendar_service import CalendarService

router = APIRouter(prefix="/calendar", tags=["calendar"])


class CalendarSyncRequest(BaseModel):
    """Request body for calendar sync endpoint."""

    start_date: date
    end_date: date
    exchange: str = Field(default="SSE")


class CalendarSyncResponse(BaseModel):
    """Response payload for calendar sync endpoint."""

    inserted: int
    start_date: date
    end_date: date
    exchange: str


def get_calendar_service(request: Request) -> CalendarService:
    """Provide CalendarService from app state."""
    return cast(CalendarService, request.app.state.calendar_service)


@router.post("/sync", response_model=CalendarSyncResponse)
def sync_calendar(
    payload: CalendarSyncRequest,
    service: CalendarService = Depends(get_calendar_service),
) -> CalendarSyncResponse:
    """Manually sync trade calendar for a date range."""
    inserted = service.sync(payload.start_date, payload.end_date, payload.exchange)
    return CalendarSyncResponse(
        inserted=inserted,
        start_date=payload.start_date,
        end_date=payload.end_date,
        exchange=payload.exchange,
    )
