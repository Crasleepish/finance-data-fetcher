from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from core.calendar.service import TradingCalendarService
from core.clean.internal_index_cleaner import InternalIndexCleaner
from core.pipeline.pipeline import IngestionPipeline
from core.pipeline.types import Arguments, ChunkArgs, NormalizedBatch, RawBatch

logger = logging.getLogger(__name__)

BASE_DATE = date(2006, 3, 31)
BASE_VALUE = 1000.0

INDEX_COMPONENTS: dict[str, list[str]] = {
    "NYBIG.IN": ["bm_BL", "bm_BM", "bm_BH"],
    "NYSML.IN": ["bm_SL", "bm_SM", "bm_SH"],
    "NYVAL.IN": ["bm_SH", "bm_BH"],
    "NYGRO.IN": ["bm_SL", "bm_BL"],
    "NYBV.IN": ["bm_BH"],
    "NYBG.IN": ["bm_BL"],
    "NYSV.IN": ["bm_SH"],
    "NYSG.IN": ["bm_SL"],
}

INDEX_INFO: dict[str, str] = {
    "NYBIG.IN": "沪深全市场大盘",
    "NYSML.IN": "沪深全市场小盘",
    "NYVAL.IN": "沪深全市场价值",
    "NYGRO.IN": "沪深全市场成长",
    "NYBV.IN": "大盘价值",
    "NYBG.IN": "大盘成长",
    "NYSV.IN": "小盘价值",
    "NYSG.IN": "小盘成长",
}


@dataclass(frozen=True)
class InternalIndexPipeline(IngestionPipeline):
    """Pipeline for internal research indices derived from bm portfolios."""

    engine: Engine
    calendar: TradingCalendarService
    output_dir: str = "./bt_result"
    _cleaner: InternalIndexCleaner = InternalIndexCleaner()

    def plan_chunks(self, arguments: Arguments) -> list[ChunkArgs]:
        """Plan chunks by natural year."""
        params = dict(arguments.get("params", {}))
        start = _parse_date(_require_param(params, "start_date"))
        end = _parse_date(_require_param(params, "end_date"))
        return [
            {"params": {"start_date": chunk_start.isoformat(), "end_date": chunk_end.isoformat()}}
            for chunk_start, chunk_end in _plan_year_chunks(start, end)
        ]

    def fetch(self, chunk_args: ChunkArgs) -> RawBatch:
        """Compute internal index series for a date range."""
        params = chunk_args.get("params") or {}
        start_date = _parse_date(_require_param(params, "start_date"))
        end_date = _parse_date(_require_param(params, "end_date"))

        self._ensure_index_info()
        base_next = self.calendar.next_trade_day(BASE_DATE)
        if base_next is None:
            raise ValueError("missing next trade day for base date")

        prev_closes = self._load_prev_closes(INDEX_COMPONENTS.keys(), start_date)
        need_base = any(value is None for value in prev_closes.values())
        calc_start = base_next if need_base and start_date > base_next else start_date

        if need_base and start_date > base_next:
            logger.warning(
                "missing previous closes; recomputing from base date",
                extra={"start_date": start_date.isoformat(), "base_date": base_next.isoformat()},
            )

        returns_df = self._load_bm_returns(calc_start, end_date)
        if returns_df.empty:
            raise RuntimeError("bm daily returns missing for range")

        weights_by_component = self._load_bm_weights(end_date)
        dates = [ts.date() for ts in returns_df.index]
        component_codes = _resolve_component_codes(weights_by_component, dates)
        component_turnover = self._load_component_turnover(component_codes)

        rows: list[dict[str, object]] = []
        for index_code, components in INDEX_COMPONENTS.items():
            comp_source = returns_df[components]
            missing_mask = comp_source.isna().any(axis=1)
            if missing_mask.any():
                missing_dates = comp_source.index[missing_mask]
                sample = [ts.date().isoformat() for ts in missing_dates[:3]]
                logger.warning(
                    "component returns missing; skipping dates",
                    extra={
                        "index_code": index_code,
                        "missing_count": int(missing_mask.sum()),
                        "missing_sample": sample,
                    },
                )
            comp_df = comp_source.loc[~missing_mask].dropna(how="any")
            if comp_df.empty:
                logger.warning(
                    "missing component returns; skipping index",
                    extra={"index_code": index_code, "start_date": start_date.isoformat()},
                )
                continue

            start_value = _resolve_start_value(prev_closes.get(index_code))
            series = (1.0 + comp_df.mean(axis=1)).cumprod() * start_value
            series = series.loc[(series.index.date >= start_date) & (series.index.date <= end_date)]
            if series.empty:
                continue

            prev_series = series.shift(1)
            prev_series.iloc[0] = start_value
            change = series - prev_series
            change_percent = change / prev_series * 100.0

            for idx, value in series.items():
                date_key = idx.date()
                volume, amount = _sum_component_turnover(components, component_turnover, date_key)
                close_value = _round_price(value)
                prev_close = _round_price(float(prev_series.loc[idx]))
                change_value = close_value - prev_close
                change_percent = change_value / prev_close * 100.0 if prev_close != 0 else None
                rows.append(
                    {
                        "index_code": index_code,
                        "date": date_key,
                        "open": close_value,
                        "close": close_value,
                        "high": close_value,
                        "low": close_value,
                        "volume": volume,
                        "amount": amount,
                        "change_percent": change_percent,
                        "change": change_value,
                    }
                )

        if not rows:
            logger.warning(
                "no internal index rows produced for chunk",
                extra={"start_date": start_date.isoformat(), "end_date": end_date.isoformat()},
            )
        return rows

    def clean(self, raw_batch: RawBatch) -> NormalizedBatch:
        """Normalize internal index rows into index_hist records."""
        return self._cleaner.clean(raw_batch)

    def _load_bm_returns(self, start: date, end: date) -> pd.DataFrame:
        paths = {
            name: Path(self.output_dir) / f"{name}_daily_returns.csv" for name in _all_components()
        }
        for name, path in paths.items():
            if not path.exists():
                raise FileNotFoundError(f"missing return file: {path}")

        frames = []
        for name, path in paths.items():
            df = pd.read_csv(path, parse_dates=["date"])
            df = df.rename(columns={"value": name})
            frames.append(df[["date", name]])

        merged = frames[0]
        for frame in frames[1:]:
            merged = merged.merge(frame, on="date", how="outer")

        merged = merged.sort_values("date")
        merged = merged[
            (merged["date"].dt.date >= start) & (merged["date"].dt.date <= end)
        ].set_index("date")
        return merged

    def _load_bm_weights(self, end: date) -> dict[str, pd.DataFrame]:
        weights: dict[str, pd.DataFrame] = {}
        for name in _all_components():
            path = Path(self.output_dir) / f"{name}_weights.csv"
            if not path.exists():
                raise FileNotFoundError(f"missing weights file: {path}")
            df = pd.read_csv(path, parse_dates=["date"])
            df = df.sort_values("date")
            df = df[df["date"].dt.date <= end].set_index("date")
            df = df.fillna(0.0)
            weights[name] = df
        return weights

    def _load_prev_closes(
        self, codes: Iterable[str], before_date: date
    ) -> dict[str, tuple[date, float] | None]:
        codes_list = list(codes)
        result: dict[str, tuple[date, float] | None] = {code: None for code in codes_list}
        stmt = text(
            """
            SELECT DISTINCT ON (index_code) index_code, date, close
            FROM index_hist
            WHERE index_code = ANY(:codes) AND date < :before
            ORDER BY index_code, date DESC
            """
        )
        with self.engine.begin() as connection:
            rows = connection.execute(stmt, {"codes": codes_list, "before": before_date}).mappings()
            for row in rows:
                index_code = row["index_code"]
                close = row["close"]
                if index_code in result and close is not None:
                    result[index_code] = (row["date"], float(close))
        return result

    def _load_component_turnover(
        self, component_codes: dict[str, dict[date, set[str]]]
    ) -> dict[str, dict[date, tuple[int | None, float | None]]]:
        turnover: dict[str, dict[date, tuple[int | None, float | None]]] = {
            name: {} for name in component_codes
        }
        all_dates = sorted({d for items in component_codes.values() for d in items})
        for trade_date in all_dates:
            codes = {
                code for items in component_codes.values() for code in items.get(trade_date, set())
            }
            if not codes:
                logger.warning(
                    "no component codes for turnover",
                    extra={"date": trade_date.isoformat()},
                )
                for name in component_codes:
                    turnover[name][trade_date] = (None, None)
                continue
            code_stats = self._load_turnover_map(trade_date, codes)
            if not code_stats:
                logger.warning(
                    "missing turnover data",
                    extra={"date": trade_date.isoformat(), "code_count": len(codes)},
                )
            for name, dates in component_codes.items():
                codes_for_date = dates.get(trade_date, set())
                volume_sum = 0
                amount_sum = 0.0
                has_data = False
                for code in codes_for_date:
                    stats = code_stats.get(code)
                    if stats is None:
                        continue
                    volume, amount = stats
                    if volume is not None:
                        volume_sum += volume
                        has_data = True
                    if amount is not None:
                        amount_sum += amount
                        has_data = True
                turnover[name][trade_date] = (
                    volume_sum if has_data else None,
                    amount_sum if has_data else None,
                )
        return turnover

    def _load_turnover_map(
        self, trade_date: date, codes: set[str]
    ) -> dict[str, tuple[int | None, float | None]]:
        stmt = text(
            """
            SELECT stock_code, volume, amount
            FROM stock_hist_unadj
            WHERE date = :date AND stock_code = ANY(:codes)
            """
        )
        rows: dict[str, tuple[int | None, float | None]] = {}
        with self.engine.begin() as connection:
            result = connection.execute(stmt, {"date": trade_date, "codes": list(codes)}).mappings()
            for row in result:
                volume = row["volume"]
                amount = row["amount"]
                rows[row["stock_code"]] = (
                    int(volume) if volume is not None else None,
                    float(amount) if amount is not None else None,
                )
        return rows

    def _ensure_index_info(self) -> None:
        stmt = text(
            """
            INSERT INTO index_info (index_code, index_name, market)
            VALUES (:index_code, :index_name, :market)
            ON CONFLICT (index_code)
            DO UPDATE SET index_name = EXCLUDED.index_name, market = EXCLUDED.market
            """
        )
        with self.engine.begin() as connection:
            connection.execute(
                stmt,
                [
                    {"index_code": code, "index_name": name, "market": "IN"}
                    for code, name in INDEX_INFO.items()
                ],
            )


def _resolve_start_value(prev_close: tuple[date, float] | None) -> float:
    if prev_close is not None:
        return prev_close[1]
    return BASE_VALUE


def _round_price(value: float) -> float:
    return round(float(value), 2)


def _resolve_component_codes(
    weights_by_component: dict[str, pd.DataFrame],
    dates: list[date],
) -> dict[str, dict[date, set[str]]]:
    resolved: dict[str, dict[date, set[str]]] = {}
    for name, weights in weights_by_component.items():
        rebalance_dates = [ts.date() for ts in weights.index]
        rebalance_dates.sort()
        code_cache: dict[date, set[str]] = {}
        for trade_date in dates:
            rebalance_date = _latest_rebalance(rebalance_dates, trade_date)
            if rebalance_date is None:
                codes = set()
            else:
                if rebalance_date not in code_cache:
                    row = weights.loc[weights.index.date == rebalance_date].iloc[0]
                    codes = set(row.index[row > 0].tolist())
                    code_cache[rebalance_date] = codes
                codes = code_cache[rebalance_date]
            resolved.setdefault(name, {})[trade_date] = codes
    return resolved


def _latest_rebalance(rebalance_dates: list[date], trade_date: date) -> date | None:
    for rebalance_date in reversed(rebalance_dates):
        if rebalance_date <= trade_date:
            return rebalance_date
    return None


def _sum_component_turnover(
    components: list[str],
    component_turnover: dict[str, dict[date, tuple[int | None, float | None]]],
    trade_date: date,
) -> tuple[int | None, float | None]:
    volume_sum = 0
    amount_sum = 0.0
    has_data = False
    for component in components:
        volume, amount = component_turnover.get(component, {}).get(trade_date, (None, None))
        if volume is not None:
            volume_sum += volume
            has_data = True
        if amount is not None:
            amount_sum += amount
            has_data = True
    if not has_data:
        return None, None
    return volume_sum, amount_sum


def _all_components() -> list[str]:
    return sorted({item for items in INDEX_COMPONENTS.values() for item in items})


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


def _plan_year_chunks(start: date, end: date) -> list[tuple[date, date]]:
    chunks: list[tuple[date, date]] = []
    current = start
    while current <= end:
        year_end = date(current.year, 12, 31)
        chunk_end = min(end, year_end)
        chunks.append((current, chunk_end))
        current = chunk_end + timedelta(days=1)
    return chunks
