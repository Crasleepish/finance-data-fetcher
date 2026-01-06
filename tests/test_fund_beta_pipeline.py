from __future__ import annotations

from unittest.mock import Mock

from sqlalchemy.engine import Engine

from core.calendar.service import TradingCalendarService
from services.pipelines.fund_beta_pipeline import FundBetaPipeline


def test_fund_beta_pipeline_plan_chunks() -> None:
    engine = Mock(spec=Engine)
    calendar = Mock(spec=TradingCalendarService)
    pipeline = FundBetaPipeline(engine=engine, calendar=calendar)
    object.__setattr__(
        pipeline,
        "_fetcher",
        Mock(
            load_fund_codes_with_data=Mock(return_value=["A", "B"]),
            get_bootstrap_range=Mock(return_value=None),
            prime_fund_net_values=Mock(),
        ),
    )

    chunks = pipeline.plan_chunks(
        {"params": {"start_date": "2024-01-02", "end_date": "2024-01-05"}}
    )

    assert chunks == [
        {
            "params": {
                "fund_code": "A",
                "start_date": "2024-01-02",
                "end_date": "2024-01-05",
                "mode": "realtime",
            }
        },
        {
            "params": {
                "fund_code": "B",
                "start_date": "2024-01-02",
                "end_date": "2024-01-05",
                "mode": "realtime",
            }
        },
    ]


def test_fund_beta_pipeline_fetch_historical(monkeypatch) -> None:
    engine = Mock(spec=Engine)
    calendar = Mock(spec=TradingCalendarService)
    pipeline = FundBetaPipeline(engine=engine, calendar=calendar)

    expected = [{"code": "A", "date": "2024-01-02"}]
    estimator = Mock()
    estimator.run_historical_beta = Mock(return_value=expected)
    object.__setattr__(pipeline, "_estimator", estimator)

    raw = pipeline.fetch(
        {
            "params": {
                "fund_code": "A",
                "start_date": "2024-01-02",
                "end_date": "2024-01-05",
                "mode": "historical",
            }
        }
    )
    assert raw == expected
    estimator.run_historical_beta.assert_called_once()
