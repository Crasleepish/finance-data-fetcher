from __future__ import annotations

from datetime import date
from unittest.mock import Mock

from sqlalchemy.engine import Engine

from config.settings import GoldDataConfig
from infra.gold_derivatives_fetcher import GoldDerivativesFetcher
from services.pipelines.gold_future_curve_pipeline import GoldFutureCurvePipeline


def test_gold_future_curve_pipeline_plan_chunks() -> None:
    engine = Mock(spec=Engine)
    pipeline = GoldFutureCurvePipeline(engine=engine, config=GoldDataConfig())
    assert pipeline.plan_chunks({}) == [{}]


def test_gold_future_curve_pipeline_fetch_and_clean(monkeypatch) -> None:
    engine = Mock(spec=Engine)
    pipeline = GoldFutureCurvePipeline(engine=engine, config=GoldDataConfig())
    expected = [
        {
            "trade_date": date(2026, 1, 2),
            "symbol": "GCZ26",
            "last_price": 2040.5,
        }
    ]

    def _fake_fetch(self, *, cancel_check=None) -> list[dict[str, object]]:
        return expected

    monkeypatch.setattr(GoldDerivativesFetcher, "update_barchart_future_curve", _fake_fetch)
    raw = pipeline.fetch({})
    assert raw == expected
    normalized = pipeline.clean(raw)
    assert normalized == expected
