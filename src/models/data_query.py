from __future__ import annotations

from datetime import date
from typing import Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

DataT = TypeVar("DataT")
MetaT = TypeVar("MetaT")


class ResponseEnvelope(BaseModel, Generic[DataT, MetaT]):
    """Standard response envelope for data query endpoints."""

    success: bool
    data: DataT | None
    error: str | None
    meta: MetaT | None


class DataPoint(BaseModel):
    """OHLC point for chart/table output."""

    date: date
    open: float | None
    close: float | None
    high: float | None
    low: float | None
    volume: int | None
    amount: float | None
    pct_change: float | None


class DataResultsPayload(BaseModel):
    """Payload for results endpoint."""

    data_type: str
    asset_code: str
    start_date: date
    end_date: date
    currency: str
    unit: str
    points: list[DataPoint] = Field(default_factory=list)


class ResultsMeta(BaseModel):
    """Metadata for results endpoint."""

    count: int = Field(ge=0)


class DataResultsResponse(ResponseEnvelope[DataResultsPayload, ResultsMeta]):
    """Response envelope for results endpoint."""


class AssetItem(BaseModel):
    """Asset list item."""

    code: str
    name: str
    market: str | None = None
    currency: str | None = None


class DataListPayload(BaseModel):
    """Payload for list endpoint."""

    data_type: str
    items: list[AssetItem] = Field(default_factory=list)


class ListMeta(BaseModel):
    """Metadata for list endpoint."""

    page: int = Field(ge=1)
    page_size: int = Field(ge=1)
    total: int = Field(ge=0)


class DataListResponse(ResponseEnvelope[DataListPayload, ListMeta]):
    """Response envelope for list endpoint."""


class ErrorResponse(ResponseEnvelope[None, None]):
    """Error envelope for data endpoints."""

    model_config = ConfigDict(frozen=True)
