from __future__ import annotations

from datetime import date

from core.clean.index_hist_bond_cleaner import IndexHistBondCleaner
from core.clean.index_hist_global_cleaner import IndexHistGlobalCleaner
from core.clean.index_hist_gold_cleaner import IndexHistGoldCleaner
from core.clean.index_hist_stock_cleaner import IndexHistStockCleaner


def test_index_hist_stock_cleaner_units_and_fill() -> None:
    cleaner = IndexHistStockCleaner()
    raw = [
        {
            "index_code": "000001.SH",
            "trade_date": "20240102",
            "close": 100.0,
            "open": None,
            "high": None,
            "low": None,
            "vol": 2.0,
            "amount": 3.5,
            "pct_chg": 1.2,
            "change": 1.0,
        }
    ]
    cleaned = list(cleaner.clean(raw))
    assert cleaned == [
        {
            "index_code": "000001.SH",
            "date": date(2024, 1, 2),
            "open": 100.0,
            "close": 100.0,
            "high": 100.0,
            "low": 100.0,
            "volume": 200,
            "amount": 3500.0,
            "change_percent": 1.2,
            "change": 1.0,
        }
    ]


def test_index_hist_bond_cleaner_units_and_fill() -> None:
    cleaner = IndexHistBondCleaner()
    raw = [
        {
            "index_code": "H11001.CSI",
            "日期": "2024-01-02",
            "收盘": 100.0,
            "开盘": None,
            "最高": None,
            "最低": None,
            "成交量": 1.5,
            "成交金额": 2.0,
            "涨跌幅": 0.5,
            "涨跌": 0.3,
        }
    ]
    cleaned = list(cleaner.clean(raw))
    assert cleaned == [
        {
            "index_code": "H11001.CSI",
            "date": date(2024, 1, 2),
            "open": 100.0,
            "close": 100.0,
            "high": 100.0,
            "low": 100.0,
            "volume": 1500000,
            "amount": 200000000.0,
            "change_percent": 0.5,
            "change": 0.3,
        }
    ]


def test_index_hist_gold_cleaner_units_and_fill() -> None:
    cleaner = IndexHistGoldCleaner()
    raw = [
        {
            "index_code": "Au99.99.SGE",
            "trade_date": "20240102",
            "close": 420.0,
            "open": None,
            "high": None,
            "low": None,
            "vol": 2.5,
            "amount": None,
            "change": 0.2,
        }
    ]
    cleaned = list(cleaner.clean(raw))
    expected_percent = 0.2 / (420.0 - 0.2) * 100
    assert cleaned == [
        {
            "index_code": "Au99.99.SGE",
            "date": date(2024, 1, 2),
            "open": 420.0,
            "close": 420.0,
            "high": 420.0,
            "low": 420.0,
            "volume": 2500,
            "amount": None,
            "change_percent": expected_percent,
            "change": 0.2,
        }
    ]


def test_index_hist_global_cleaner_maps_fields() -> None:
    cleaner = IndexHistGlobalCleaner()
    # Values sourced from production index_hist row (Au99.95.SGE, 2025-12-31).
    raw = [
        {
            "index_code": "XIN9.GLB",
            "date": "2025-12-31",
            "open": 952.0,
            "close": 970.8,
            "high": 980.0,
            "low": 952.0,
            "volume": 54000,
            "amount": 52233200.0,
        }
    ]
    cleaned = list(cleaner.clean(raw))
    assert cleaned == [
        {
            "index_code": "XIN9.GLB",
            "date": date(2025, 12, 31),
            "open": 952.0,
            "close": 970.8,
            "high": 980.0,
            "low": 952.0,
            "volume": 54000,
            "amount": 52233200.0,
            "change_percent": None,
            "change": None,
        }
    ]
