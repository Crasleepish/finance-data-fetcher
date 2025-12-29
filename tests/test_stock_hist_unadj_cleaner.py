from __future__ import annotations

from datetime import date

from core.clean.stock_hist_unadj_cleaner import StockHistUnadjCleaner


def test_stock_hist_unadj_cleaner_merges_and_converts() -> None:
    cleaner = StockHistUnadjCleaner()
    raw = [
        {
            "trade_date": "20240102",
            "daily": [
                {
                    "ts_code": "000001.SZ",
                    "open": 10.0,
                    "high": 11.0,
                    "low": 9.0,
                    "close": 10.5,
                    "pre_close": 10.2,
                    "change": 0.3,
                    "pct_chg": 2.94,
                    "vol": 100.0,
                    "amount": 50.0,
                }
            ],
            "daily_basic": [
                {
                    "ts_code": "000001.SZ",
                    "turnover_rate": 1.2,
                    "turnover_rate_f": 1.0,
                    "volume_ratio": 0.9,
                    "pe": 10.5,
                    "pe_ttm": 11.0,
                    "pb": 1.5,
                    "ps": 2.0,
                    "ps_ttm": 2.1,
                    "dv_ratio": 0.5,
                    "dv_ttm": 0.6,
                    "total_share": 10.0,
                    "float_share": 8.0,
                    "free_share": 7.0,
                    "total_mv": 100.0,
                    "circ_mv": 80.0,
                }
            ],
            "stock_st": [{"ts_code": "000001.SZ"}],
            "suspend": [{"ts_code": "000001.SZ"}],
        }
    ]

    records = list(cleaner.clean(raw))
    assert records == [
        {
            "stock_code": "000001.SZ",
            "date": date(2024, 1, 2),
            "open": 10.0,
            "close": 10.5,
            "high": 11.0,
            "low": 9.0,
            "volume": 10000,
            "amount": 50000.0,
            "pre_close": 10.2,
            "change": 0.3,
            "change_percent": 2.94,
            "turnover_rate": 1.2,
            "turnover_rate_f": 1.0,
            "volume_ratio": 0.9,
            "pe": 10.5,
            "pe_ttm": 11.0,
            "pb": 1.5,
            "ps": 2.0,
            "ps_ttm": 2.1,
            "dv_ratio": 0.5,
            "dv_ttm": 0.6,
            "total_share": 100000,
            "float_share": 80000,
            "free_share": 70000,
            "mkt_cap": 1000000,
            "circ_mv": 800000,
            "is_st": "Y",
            "is_suspend": "Y",
        }
    ]
