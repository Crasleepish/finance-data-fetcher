from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime

from sqlalchemy.engine import Engine

from core.calendar.service import TradingCalendarService
from core.clean.fund_beta_cleaner import FundBetaCleaner
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch
from infra.fund_beta_data_fetcher import FundBetaDataFetcher
from services.fund_beta_estimator import WINDOW_SIZE, FundBetaEstimator

logger = logging.getLogger(__name__)


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
        requested_codes = _optional_code_list(params.get("fund_codes"))
        if mode not in {"historical", "realtime"}:
            raise ValueError("mode must be historical or realtime")
        codes_with_start = self._fetcher.load_fund_codes_with_data(
            ["被动指数型", "增强指数型"], start_date, end_date
        )
        if not codes_with_start:
            return []
        fund_start = {code: fund_start for code, fund_start in codes_with_start}
        if requested_codes is not None:
            allowed = set(fund_start)
            fund_start = {code: fund_start[code] for code in requested_codes if code in allowed}
        codes = list(fund_start.keys())
        if not codes:
            return []
        min_start = min(_parse_date(value) for value in fund_start.values()).strftime("%Y-%m-%d")
        prefetch_start = start_date
        if mode == "realtime":
            bootstrap_range = self._fetcher.get_bootstrap_range(min_start, WINDOW_SIZE)
            if bootstrap_range is not None:
                prefetch_start = bootstrap_range[0]
        else:
            prefetch_start = min_start
        self._fetcher.prime_fund_net_values(codes, prefetch_start, end_date)
        return [
            {
                "params": {
                    "fund_code": code,
                    "start_date": fund_start[code],
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
        try:
            if mode == "historical":
                return self._estimator.run_historical_beta(fund_code, start_date, end_date)
            return self._estimator.run_realtime_update(fund_code, start_date, end_date)
        except Exception as exc:
            logger.warning(
                "fund beta fetch skipped due to missing data",
                extra={
                    "fund_code": fund_code,
                    "start_date": start_date,
                    "end_date": end_date,
                    "mode": mode,
                    "error": str(exc),
                },
            )
            return []

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize beta records for persistence."""
        return self._cleaner.clean(raw_batch)


def _require_param(params: dict[str, object], key: str) -> str:
    value = params.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"missing required param: {key}")
    return value


def _optional_code_list(value: object) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise ValueError("fund_codes must be a list of strings")
    codes = [item for item in value if isinstance(item, str) and item]
    if len(codes) != len(value):
        raise ValueError("fund_codes must be a list of non-empty strings")
    return codes


def _parse_date(value: str) -> datetime:
    if len(value) == 10:
        return datetime.strptime(value, "%Y-%m-%d")
    if len(value) == 8 and value.isdigit():
        return datetime.strptime(value, "%Y%m%d")
    raise ValueError("expected YYYY-MM-DD or YYYYMMDD date string")
