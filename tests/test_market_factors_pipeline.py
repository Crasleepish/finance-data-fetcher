from __future__ import annotations

from unittest.mock import Mock

from sqlalchemy.engine import Engine

from services.factor_fetcher import FactorFetcher
from services.pipelines.market_factors_pipeline import MarketFactorsPipeline


def test_market_factors_pipeline_plan_chunks_defaults() -> None:
    engine = Mock(spec=Engine)
    pipeline = MarketFactorsPipeline(engine=engine)
    chunks = pipeline.plan_chunks(
        {
            "params": {
                "start_date": "2023-01-02",
                "end_date": "2023-01-31",
            }
        }
    )
    assert chunks == [
        {
            "params": {
                "start_date": "2023-01-02",
                "end_date": "2023-01-31",
                "mode": "history",
            }
        }
    ]


def test_market_factors_pipeline_fetch_and_clean(monkeypatch) -> None:
    engine = Mock(spec=Engine)
    pipeline = MarketFactorsPipeline(engine=engine)
    expected_raw = [
        {
            "date": "2023-01-02",
            "MKT": 0.01,
            "SMB": 0.02,
            "HML": 0.03,
            "QMJ": 0.04,
            "VOL": None,
            "LIQ": None,
        }
    ]

    def _fake_fetch_all(
        self, start_date: str, end_date: str, mode: str, *, cancel_check=None
    ) -> list[dict[str, object]]:
        assert start_date == "2023-01-02"
        assert end_date == "2023-01-31"
        assert mode == "history"
        return expected_raw

    monkeypatch.setattr(FactorFetcher, "fetch_all", _fake_fetch_all)
    raw = pipeline.fetch(
        {
            "params": {
                "start_date": "2023-01-02",
                "end_date": "2023-01-31",
                "mode": "history",
            }
        }
    )
    assert raw == expected_raw
    normalized = pipeline.clean(raw)
    assert normalized == expected_raw
