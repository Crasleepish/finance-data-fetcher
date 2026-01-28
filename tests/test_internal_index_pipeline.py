from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest
from sqlalchemy.engine import Engine

from infra.db.tables import index_info, stock_hist_unadj
from services.pipelines.internal_index_pipeline import (
    BASE_VALUE,
    INDEX_COMPONENTS,
    InternalIndexPipeline,
)


def _write_returns(
    output_dir: Path,
    name: str,
    dates: list[date],
    values: list[float | None],
) -> None:
    df = pd.DataFrame({"date": dates, "value": values})
    df.to_csv(output_dir / f"{name}_daily_returns.csv", index=False)


def _create_returns_files(output_dir: Path, values_by_name: dict[str, list[float | None]]) -> None:
    dates = [date(2006, 4, 3), date(2006, 4, 4)]
    for name, values in values_by_name.items():
        _write_returns(output_dir, name, dates, values)


def _write_weights(output_dir: Path, name: str, codes: list[str], weights: list[float]) -> None:
    df = pd.DataFrame(
        {
            "date": [date(2006, 3, 31)],
            **{code: [weight] for code, weight in zip(codes, weights, strict=True)},
        }
    )
    df.to_csv(output_dir / f"{name}_weights.csv", index=False)


def _create_weights_files(output_dir: Path, codes_by_component: dict[str, list[str]]) -> None:
    for name, codes in codes_by_component.items():
        weights = [1.0 for _ in codes]
        _write_weights(output_dir, name, codes, weights)


def test_internal_index_plan_chunks_year_split() -> None:
    engine = Mock(spec=Engine)
    calendar = Mock()
    pipeline = InternalIndexPipeline(engine=engine, calendar=calendar)
    chunks = pipeline.plan_chunks(
        {"params": {"start_date": "2024-06-01", "end_date": "2026-02-01"}}
    )
    assert chunks == [
        {"params": {"start_date": "2024-06-01", "end_date": "2024-12-31"}},
        {"params": {"start_date": "2025-01-01", "end_date": "2025-12-31"}},
        {"params": {"start_date": "2026-01-01", "end_date": "2026-02-01"}},
    ]


def test_internal_index_fetch_uses_base_value(postgres_engine: Engine, tmp_path: Path) -> None:
    values = {
        "bm_BL": [0.01, 0.02],
        "bm_BM": [0.01, 0.02],
        "bm_BH": [0.01, 0.02],
        "bm_SL": [0.03, 0.04],
        "bm_SM": [0.03, 0.04],
        "bm_SH": [0.03, 0.04],
    }
    _create_returns_files(tmp_path, values)
    codes_by_component = {
        "bm_BL": ["000001.SZ"],
        "bm_BM": ["000002.SZ"],
        "bm_BH": ["000003.SZ"],
        "bm_SL": ["000004.SZ"],
        "bm_SM": ["000005.SZ"],
        "bm_SH": ["000006.SZ"],
    }
    _create_weights_files(tmp_path, codes_by_component)
    with postgres_engine.begin() as connection:
        for code in ["000001.SZ", "000002.SZ", "000003.SZ", "000004.SZ", "000005.SZ", "000006.SZ"]:
            connection.execute(
                stock_hist_unadj.insert(),
                [
                    {
                        "stock_code": code,
                        "date": date(2006, 4, 3),
                        "open": 10.0,
                        "close": 10.0,
                        "high": 10.0,
                        "low": 10.0,
                        "volume": 100,
                        "amount": 1000.0,
                        "pre_close": 10.0,
                        "change_percent": 0.0,
                        "change": 0.0,
                        "turnover_rate": None,
                        "turnover_rate_f": None,
                        "volume_ratio": None,
                        "pe": None,
                        "pe_ttm": None,
                        "pb": None,
                        "ps": None,
                        "ps_ttm": None,
                        "dv_ratio": None,
                        "dv_ttm": None,
                        "total_share": None,
                        "float_share": None,
                        "free_share": None,
                        "mkt_cap": None,
                        "circ_mv": None,
                        "is_suspend": "N",
                    },
                    {
                        "stock_code": code,
                        "date": date(2006, 4, 4),
                        "open": 10.0,
                        "close": 10.0,
                        "high": 10.0,
                        "low": 10.0,
                        "volume": 200,
                        "amount": 2000.0,
                        "pre_close": 10.0,
                        "change_percent": 0.0,
                        "change": 0.0,
                        "turnover_rate": None,
                        "turnover_rate_f": None,
                        "volume_ratio": None,
                        "pe": None,
                        "pe_ttm": None,
                        "pb": None,
                        "ps": None,
                        "ps_ttm": None,
                        "dv_ratio": None,
                        "dv_ttm": None,
                        "total_share": None,
                        "float_share": None,
                        "free_share": None,
                        "mkt_cap": None,
                        "circ_mv": None,
                        "is_suspend": "N",
                    },
                ],
            )
    calendar = Mock()
    calendar.next_trade_day.return_value = date(2006, 4, 3)

    pipeline = InternalIndexPipeline(
        engine=postgres_engine, calendar=calendar, output_dir=str(tmp_path)
    )
    raw = pipeline.fetch({"params": {"start_date": "2006-04-03", "end_date": "2006-04-04"}})
    by_key = {(row["index_code"], row["date"]): row for row in raw}
    assert len(by_key) == len(INDEX_COMPONENTS) * 2

    nybig_day1 = by_key[("NYBIG.IN", date(2006, 4, 3))]
    nybig_day2 = by_key[("NYBIG.IN", date(2006, 4, 4))]
    assert nybig_day1["close"] == pytest.approx(BASE_VALUE * 1.01, rel=1e-6)
    assert nybig_day2["close"] == pytest.approx(BASE_VALUE * 1.01 * 1.02, rel=1e-6)
    assert nybig_day1["open"] == pytest.approx(round(BASE_VALUE * 1.01, 2), rel=1e-6)
    assert nybig_day1["volume"] == 300
    assert nybig_day1["amount"] == pytest.approx(3000.0, rel=1e-6)
    with postgres_engine.begin() as connection:
        info_rows = connection.execute(
            index_info.select().where(index_info.c.index_code == "NYBIG.IN")
        ).mappings()
        info = list(info_rows)
    assert info and info[0]["market"] == "IN"


def test_internal_index_skips_days_with_missing_components(
    postgres_engine: Engine, tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    values = {
        "bm_BL": [0.01, 0.02],
        "bm_BM": [0.01, 0.02],
        "bm_BH": [0.01, None],
        "bm_SL": [0.03, 0.04],
        "bm_SM": [0.03, 0.04],
        "bm_SH": [0.03, 0.04],
    }
    _create_returns_files(tmp_path, values)
    codes_by_component = {
        "bm_BL": ["000001.SZ"],
        "bm_BM": ["000002.SZ"],
        "bm_BH": ["000003.SZ"],
        "bm_SL": ["000004.SZ"],
        "bm_SM": ["000005.SZ"],
        "bm_SH": ["000006.SZ"],
    }
    _create_weights_files(tmp_path, codes_by_component)
    calendar = Mock()
    calendar.next_trade_day.return_value = date(2006, 4, 3)
    pipeline = InternalIndexPipeline(
        engine=postgres_engine, calendar=calendar, output_dir=str(tmp_path)
    )
    caplog.set_level(logging.WARNING)
    raw = pipeline.fetch({"params": {"start_date": "2006-04-03", "end_date": "2006-04-04"}})
    bv_rows = [row for row in raw if row["index_code"] == "NYBV.IN"]
    assert [row["date"] for row in bv_rows] == [date(2006, 4, 3)]
    assert any("component returns missing" in record.message for record in caplog.records)
