from __future__ import annotations

from unittest.mock import Mock

import pytest
from sqlalchemy.engine import Engine

from services.pipelines import rt_index_hist_pipeline
from services.pipelines.rt_index_hist_pipeline import (
    RtIndexHistTusharePipeline,
    RtIndexHistXueqiuPipeline,
)


def test_rt_index_hist_xueqiu_plan_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = Mock(spec=Engine)
    monkeypatch.setattr(rt_index_hist_pipeline, "require_index_codes", lambda *_: ["000985.CSI"])
    monkeypatch.setattr(rt_index_hist_pipeline, "_should_fetch", lambda *_: True)

    pipeline = RtIndexHistXueqiuPipeline(
        engine=engine,
        rt_fetch_interval_s=600,
        codes_raw="000985.CSI",
    )

    chunks = pipeline.plan_chunks({"params": {}})
    assert chunks == [{"params": {}}]
    assert pipeline._codes == [{"index_code": "000985.CSI", "api_code": "CSI000985"}]


def test_rt_index_hist_tushare_plan_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = Mock(spec=Engine)
    monkeypatch.setattr(
        rt_index_hist_pipeline,
        "require_index_codes",
        lambda *_: ["000985.CSI", "000001.SH"],
    )
    monkeypatch.setattr(rt_index_hist_pipeline, "_should_fetch", lambda *_: True)

    pipeline = RtIndexHistTusharePipeline(
        engine=engine,
        rt_fetch_interval_s=600,
        fetcher=Mock(),
        codes_raw="000985.CSI,000001.SH",
    )

    chunks = pipeline.plan_chunks({"params": {}})
    assert chunks == [{"params": {"codes": ["000985.CSI", "000001.SH"]}}]
