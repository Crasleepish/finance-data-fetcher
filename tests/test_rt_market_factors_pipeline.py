from __future__ import annotations

from unittest.mock import Mock

import pytest
from sqlalchemy.engine import Engine

from services.pipelines import rt_market_factors_pipeline
from services.pipelines.rt_market_factors_pipeline import RtMarketFactorsPipeline


def test_rt_market_factors_plan_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = Mock(spec=Engine)
    monkeypatch.setattr(rt_market_factors_pipeline, "_should_fetch", lambda *_: True)

    pipeline = RtMarketFactorsPipeline(
        engine=engine,
        rt_fetch_interval_s=600,
    )

    chunks = pipeline.plan_chunks({"params": {}})
    assert chunks == [{"params": {}}]
