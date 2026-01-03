from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

from sqlalchemy.engine import Engine

from config.settings import GoldDataConfig
from core.clean.gold_cftc_report_cleaner import GoldCftcReportCleaner
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.gold_derivatives_fetcher import GoldDerivativesFetcher


@dataclass(frozen=True)
class GoldCftcReportPipeline(IngestionPipeline):
    """Pipeline for fetching and persisting CFTC gold reports."""

    engine: Engine
    config: GoldDataConfig
    _fetcher: GoldDerivativesFetcher = field(init=False, repr=False)
    _cleaner: GoldCftcReportCleaner = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "_fetcher",
            GoldDerivativesFetcher(engine=self.engine, config=self.config),
        )
        object.__setattr__(self, "_cleaner", GoldCftcReportCleaner())

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        """Plan a single chunk for the requested as-of date."""
        params = dict(arguments.get("params", {}))
        as_of_date = _parse_date(_require_param(params, "as_of_date"))
        return [{"params": {"as_of_date": as_of_date.isoformat()}}]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch CFTC report records for the provided as-of date."""
        params = chunk_args.get("params") or {}
        as_of_date = _parse_date(_require_param(params, "as_of_date"))
        return self._fetcher.ensure_cftc_reports(as_of_date)

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize CFTC report records for persistence."""
        return self._cleaner.clean(raw_batch)


def _require_param(params: dict[str, object], key: str) -> str:
    value = params.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing required param: {key}")
    return value


def _parse_date(value: str) -> date:
    if len(value) == 10:
        return datetime.strptime(value, "%Y-%m-%d").date()
    if len(value) == 8 and value.isdigit():
        return datetime.strptime(value, "%Y%m%d").date()
    raise ValueError("expected YYYY-MM-DD or YYYYMMDD date string")
