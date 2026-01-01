from __future__ import annotations

from datetime import date

from core.clean.etf_info_cleaner import EtfInfoCleaner


def test_etf_info_cleaner_maps_fields() -> None:
    cleaner = EtfInfoCleaner()
    raw = [
        {
            "ts_code": "510300.SH",
            "name": "沪深300ETF",
            "fund_type": "ETF",
            "invest_type": "被动指数型",
            "found_date": "20120528",
        },
        {
            "ts_code": "510500.SH",
            "name": "中证500ETF",
            "fund_type": "ETF",
            "invest_type": None,
            "found_date": None,
        },
    ]

    cleaned = list(cleaner.clean(raw))

    assert cleaned == [
        {
            "etf_code": "510300.SH",
            "etf_name": "沪深300ETF",
            "fund_type": "ETF",
            "invest_type": "被动指数型",
            "found_date": date(2012, 5, 28),
        },
        {
            "etf_code": "510500.SH",
            "etf_name": "中证500ETF",
            "fund_type": "ETF",
            "invest_type": None,
            "found_date": None,
        },
    ]
