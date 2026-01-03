from __future__ import annotations

from datetime import date
from unittest.mock import Mock

from sqlalchemy.engine import Engine

from config.settings import GoldDataConfig
from infra.gold_derivatives_fetcher import GoldDerivativesFetcher
from services.pipelines.gold_cftc_report_pipeline import GoldCftcReportPipeline


def test_gold_cftc_report_pipeline_plan_chunks() -> None:
    engine = Mock(spec=Engine)
    pipeline = GoldCftcReportPipeline(engine=engine, config=GoldDataConfig())
    chunks = pipeline.plan_chunks({"params": {"as_of_date": "2026-01-02"}})
    assert chunks == [{"params": {"as_of_date": "2026-01-02"}}]


def test_gold_cftc_report_pipeline_fetch_and_clean(monkeypatch) -> None:
    engine = Mock(spec=Engine)
    pipeline = GoldCftcReportPipeline(engine=engine, config=GoldDataConfig())
    expected = [
        {
            "market_name": "GOLD - COMMODITY EXCHANGE INC.",
            "as_of_date": date(2026, 1, 2),
            "report_date": date(2026, 1, 6),
            "contract_market_code": "088691",
            "market_code": "088691",
        }
    ]

    def _fake_fetch(self, as_of_date: date) -> list[dict[str, object]]:
        assert as_of_date == date(2026, 1, 2)
        return expected

    monkeypatch.setattr(GoldDerivativesFetcher, "ensure_cftc_reports", _fake_fetch)
    raw = pipeline.fetch({"params": {"as_of_date": "2026-01-02"}})
    assert raw == expected
    normalized = pipeline.clean(raw)
    assert normalized == expected
