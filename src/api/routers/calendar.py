from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, Field

from services.calendar_service import CalendarService

router = APIRouter(prefix="/calendar", tags=["calendar"])


class CalendarSyncRequest(BaseModel):
    start_date: date
    end_date: date
    exchange: str = Field(default="SSE")


class CalendarSyncResponse(BaseModel):
    inserted: int
    start_date: date
    end_date: date
    exchange: str


def get_calendar_service(request: Request) -> CalendarService:
    return request.app.state.calendar_service


@router.post("/sync", response_model=CalendarSyncResponse)
def sync_calendar(
    payload: CalendarSyncRequest,
    service: CalendarService = Depends(get_calendar_service),
) -> CalendarSyncResponse:
    inserted = service.sync(payload.start_date, payload.end_date, payload.exchange)
    return CalendarSyncResponse(
        inserted=inserted,
        start_date=payload.start_date,
        end_date=payload.end_date,
        exchange=payload.exchange,
    )
