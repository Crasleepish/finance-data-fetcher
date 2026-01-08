from __future__ import annotations

from datetime import date

from core.clean.fundamental_data_cleaner import FundamentalDataCleaner


def test_fundamental_data_cleaner_maps_fields() -> None:
    cleaner = FundamentalDataCleaner(overwrite=False)
    raw = [
        {
            "income": [
                {
                    "ts_code": "000001.SZ",
                    "end_date": "20240331",
                    "n_income_attr_p": 10.0,
                    "operate_profit": 20.0,
                    "total_revenue": 30.0,
                    "total_cogs": 40.0,
                }
            ],
            "balance": [
                {
                    "ts_code": "000001.SZ",
                    "end_date": "20240331",
                    "total_hldr_eqy_exc_min_int": 100.0,
                    "total_assets": 200.0,
                    "total_cur_liab": 50.0,
                    "total_ncl": 25.0,
                    "total_liab": None,
                }
            ],
            "cashflow": [
                {
                    "ts_code": "000001.SZ",
                    "end_date": "20240331",
                    "n_cashflow_act": 5.0,
                    "c_pay_acq_const_fiolta": 6.0,
                }
            ],
        }
    ]

    records = list(cleaner.clean(raw))
    assert records == [
        {
            "stock_code": "000001.SZ",
            "report_date": date(2024, 3, 31),
            "total_equity": 100.0,
            "total_assets": 200.0,
            "current_liabilities": 50.0,
            "noncurrent_liabilities": 25.0,
            "net_profit": 10.0,
            "operating_profit": 20.0,
            "total_revenue": 30.0,
            "total_cost": 40.0,
            "net_cash_from_operating": 5.0,
            "cash_for_fixed_assets": 6.0,
            "operating_profit_ttm": None,
            "total_liabilities": 75.0,
        }
    ]


def test_fundamental_data_cleaner_net_profit_fallback() -> None:
    cleaner = FundamentalDataCleaner(overwrite=False)
    raw = [
        {
            "income": [
                {
                    "ts_code": "000002.SZ",
                    "end_date": "20241231",
                    "n_income_attr_p": None,
                    "n_income": None,
                    "continued_net_profit": 3.0,
                    "end_net_profit": 7.0,
                }
            ],
            "balance": [{"ts_code": "000002.SZ", "end_date": "20241231"}],
            "cashflow": [{"ts_code": "000002.SZ", "end_date": "20241231"}],
        }
    ]

    records = list(cleaner.clean(raw))
    assert records[0]["net_profit"] == 10.0


def test_fundamental_data_cleaner_net_profit_partial_fallback() -> None:
    cleaner = FundamentalDataCleaner(overwrite=False)
    raw = [
        {
            "income": [
                {
                    "ts_code": "000005.SZ",
                    "end_date": "20241231",
                    "n_income_attr_p": None,
                    "n_income": None,
                    "continued_net_profit": 3.0,
                    "end_net_profit": None,
                }
            ],
            "balance": [{"ts_code": "000005.SZ", "end_date": "20241231"}],
            "cashflow": [{"ts_code": "000005.SZ", "end_date": "20241231"}],
        }
    ]

    records = list(cleaner.clean(raw))
    assert records[0]["net_profit"] is None


def test_fundamental_data_cleaner_net_profit_secondary() -> None:
    cleaner = FundamentalDataCleaner(overwrite=False)
    raw = [
        {
            "income": [
                {
                    "ts_code": "000004.SZ",
                    "end_date": "20241231",
                    "n_income_attr_p": None,
                    "n_income": 12.5,
                    "continued_net_profit": None,
                    "end_net_profit": None,
                }
            ],
            "balance": [{"ts_code": "000004.SZ", "end_date": "20241231"}],
            "cashflow": [{"ts_code": "000004.SZ", "end_date": "20241231"}],
        }
    ]

    records = list(cleaner.clean(raw))
    assert records[0]["net_profit"] == 12.5


def test_fundamental_data_cleaner_truncates_long_stock_code() -> None:
    cleaner = FundamentalDataCleaner(overwrite=False)
    raw = [
        {
            "income": [{"ts_code": "12345678901.SH", "end_date": "20241231"}],
            "balance": [{"ts_code": "12345678901.SH", "end_date": "20241231"}],
            "cashflow": [{"ts_code": "12345678901.SH", "end_date": "20241231"}],
        }
    ]

    records = list(cleaner.clean(raw))
    assert records[0]["stock_code"] == "1234567890"


def test_fundamental_data_cleaner_total_liabilities_missing() -> None:
    cleaner = FundamentalDataCleaner(overwrite=False)
    raw = [
        {
            "income": [{"ts_code": "000006.SZ", "end_date": "20240331"}],
            "balance": [{"ts_code": "000006.SZ", "end_date": "20240331"}],
            "cashflow": [{"ts_code": "000006.SZ", "end_date": "20240331"}],
        }
    ]

    records = list(cleaner.clean(raw))
    assert records[0]["total_liabilities"] is None


def test_fundamental_data_ttm_forward_fill(postgres_engine) -> None:
    from sqlalchemy.engine import Engine

    from infra.db.tables import fundamental_data
    from services.pipelines.fundamental_data_pipeline import _apply_operating_profit_ttm

    engine = postgres_engine
    assert isinstance(engine, Engine)

    with engine.begin() as connection:
        connection.execute(
            fundamental_data.insert(),
            [
                {
                    "stock_code": "000003.SZ",
                    "report_date": date(2023, 12, 31),
                    "operating_profit": 100.0,
                    "operating_profit_ttm": 1000.0,
                },
                {
                    "stock_code": "000003.SZ",
                    "report_date": date(2023, 3, 31),
                    "operating_profit": 10.0,
                    "operating_profit_ttm": None,
                },
            ],
        )

    records = [
        {"stock_code": "000003.SZ", "report_date": date(2024, 3, 31), "operating_profit": 20.0},
        {"stock_code": "000003.SZ", "report_date": date(2024, 6, 30), "operating_profit": None},
    ]

    enriched = _apply_operating_profit_ttm(records, engine, fundamental_data, overwrite=False)
    assert enriched[0]["operating_profit_ttm"] == 110.0
    assert enriched[1]["operating_profit_ttm"] == 110.0


def test_fundamental_data_ttm_overwrite(postgres_engine) -> None:
    from infra.db.tables import fundamental_data
    from services.pipelines.fundamental_data_pipeline import _apply_operating_profit_ttm

    engine = postgres_engine
    with engine.begin() as connection:
        connection.execute(
            fundamental_data.insert(),
            [
                {
                    "stock_code": "000005.SZ",
                    "report_date": date(2023, 12, 31),
                    "operating_profit": 100.0,
                    "operating_profit_ttm": 999.0,
                },
                {
                    "stock_code": "000005.SZ",
                    "report_date": date(2023, 3, 31),
                    "operating_profit": 10.0,
                    "operating_profit_ttm": None,
                },
            ],
        )

    records = [
        {"stock_code": "000005.SZ", "report_date": date(2024, 3, 31), "operating_profit": 20.0}
    ]
    enriched = _apply_operating_profit_ttm(records, engine, fundamental_data, overwrite=True)
    assert enriched[0]["operating_profit_ttm"] == 110.0


def test_fundamental_data_ttm_cur_op_nan_skips(postgres_engine) -> None:
    from infra.db.tables import fundamental_data
    from services.pipelines.fundamental_data_pipeline import _apply_operating_profit_ttm

    engine = postgres_engine
    with engine.begin() as connection:
        connection.execute(
            fundamental_data.insert(),
            [
                {
                    "stock_code": "000006.SZ",
                    "report_date": date(2023, 12, 31),
                    "operating_profit": 100.0,
                },
                {
                    "stock_code": "000006.SZ",
                    "report_date": date(2023, 3, 31),
                    "operating_profit": 10.0,
                },
            ],
        )

    records = [
        {
            "stock_code": "000006.SZ",
            "report_date": date(2024, 3, 31),
            "operating_profit": float("nan"),
        }
    ]
    enriched = _apply_operating_profit_ttm(records, engine, fundamental_data, overwrite=True)
    assert enriched[0]["operating_profit_ttm"] is None


def test_fundamental_data_ttm_last_annual_nan_skips(postgres_engine) -> None:
    from infra.db.tables import fundamental_data
    from services.pipelines.fundamental_data_pipeline import _apply_operating_profit_ttm

    engine = postgres_engine
    with engine.begin() as connection:
        connection.execute(
            fundamental_data.insert(),
            [
                {
                    "stock_code": "000007.SZ",
                    "report_date": date(2023, 12, 31),
                    "operating_profit": float("nan"),
                },
                {
                    "stock_code": "000007.SZ",
                    "report_date": date(2023, 3, 31),
                    "operating_profit": 10.0,
                },
            ],
        )

    records = [
        {"stock_code": "000007.SZ", "report_date": date(2024, 3, 31), "operating_profit": 20.0}
    ]
    enriched = _apply_operating_profit_ttm(records, engine, fundamental_data, overwrite=True)
    assert enriched[0]["operating_profit_ttm"] is None


def test_fundamental_data_ttm_last_same_nan_skips(postgres_engine) -> None:
    from infra.db.tables import fundamental_data
    from services.pipelines.fundamental_data_pipeline import _apply_operating_profit_ttm

    engine = postgres_engine
    with engine.begin() as connection:
        connection.execute(
            fundamental_data.insert(),
            [
                {
                    "stock_code": "000008.SZ",
                    "report_date": date(2023, 12, 31),
                    "operating_profit": 100.0,
                },
                {
                    "stock_code": "000008.SZ",
                    "report_date": date(2023, 3, 31),
                    "operating_profit": float("nan"),
                },
            ],
        )

    records = [
        {"stock_code": "000008.SZ", "report_date": date(2024, 3, 31), "operating_profit": 20.0}
    ]
    enriched = _apply_operating_profit_ttm(records, engine, fundamental_data, overwrite=True)
    assert enriched[0]["operating_profit_ttm"] is None


def test_fundamental_data_ttm_all_missing(postgres_engine) -> None:
    from infra.db.tables import fundamental_data
    from services.pipelines.fundamental_data_pipeline import _apply_operating_profit_ttm

    engine = postgres_engine
    with engine.begin() as connection:
        connection.execute(
            fundamental_data.insert(),
            [
                {
                    "stock_code": "000009.SZ",
                    "report_date": date(2023, 12, 31),
                    "operating_profit": None,
                },
                {
                    "stock_code": "000009.SZ",
                    "report_date": date(2023, 3, 31),
                    "operating_profit": None,
                },
            ],
        )

    records = [
        {"stock_code": "000009.SZ", "report_date": date(2024, 3, 31), "operating_profit": None}
    ]
    enriched = _apply_operating_profit_ttm(records, engine, fundamental_data, overwrite=True)
    assert enriched[0]["operating_profit_ttm"] is None
