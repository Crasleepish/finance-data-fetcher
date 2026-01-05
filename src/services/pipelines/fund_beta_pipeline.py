from __future__ import annotations

from dataclasses import dataclass, field

from sqlalchemy.engine import Engine

from core.calendar.service import TradingCalendarService
from core.clean.fund_beta_cleaner import FundBetaCleaner
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.fund_beta_data_fetcher import FundBetaDataFetcher
from services.fund_beta_estimator import FundBetaEstimator


@dataclass(frozen=True)
class FundBetaPipeline(IngestionPipeline):
    """Pipeline for computing fund beta exposures."""

    engine: Engine
    calendar: TradingCalendarService
    _fetcher: FundBetaDataFetcher = field(init=False, repr=False)
    _estimator: FundBetaEstimator = field(init=False, repr=False)
    _cleaner: FundBetaCleaner = field(init=False, repr=False)

    def __post_init__(self) -> None:
        fetcher = FundBetaDataFetcher(engine=self.engine, calendar=self.calendar)
        object.__setattr__(self, "_fetcher", fetcher)
        object.__setattr__(self, "_estimator", FundBetaEstimator(data_fetcher=fetcher))
        object.__setattr__(self, "_cleaner", FundBetaCleaner())

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        """Plan one chunk per fund code."""
        params = dict(arguments.get("params", {}))
        start_date = _require_param(params, "start_date")
        end_date = _require_param(params, "end_date")
        mode = params.get("mode", "realtime")
        if mode not in {"historical", "realtime"}:
            raise ValueError("mode must be historical or realtime")
        codes = self._fetcher.load_fund_codes(["被动指数型", "增强指数型"])
        return [
            {
                "params": {
                    "fund_code": code,
                    "start_date": start_date,
                    "end_date": end_date,
                    "mode": mode,
                }
            }
            for code in codes
        ]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Fetch beta records for a single fund."""
        params = chunk_args.get("params") or {}
        fund_code = _require_param(params, "fund_code")
        start_date = _require_param(params, "start_date")
        end_date = _require_param(params, "end_date")
        mode = params.get("mode", "realtime")
        if mode not in {"historical", "realtime"}:
            raise ValueError("mode must be historical or realtime")
        if mode == "historical":
            return self._estimator.run_historical_beta(fund_code, start_date, end_date)
        return self._estimator.run_realtime_update(fund_code, start_date, end_date)

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize beta records for persistence."""
        return self._cleaner.clean(raw_batch)


def _require_param(params: dict[str, object], key: str) -> str:
    value = params.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing required param: {key}")
    return value
