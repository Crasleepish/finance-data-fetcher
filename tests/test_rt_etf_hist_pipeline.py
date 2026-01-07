from __future__ import annotations

from unittest.mock import Mock

import pytest
from sqlalchemy.engine import Engine

from services.pipelines import rt_etf_hist_pipeline
from services.pipelines.rt_etf_hist_pipeline import (
    RtEtfHistAksharePipeline,
    RtEtfHistXueqiuPipeline,
)


def test_rt_etf_hist_akshare_plan_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = Mock(spec=Engine)
    monkeypatch.setattr(rt_etf_hist_pipeline, "_should_fetch", lambda *_: True)

    pipeline = RtEtfHistAksharePipeline(
        engine=engine,
        rt_fetch_interval_s=600,
        fetcher=Mock(),
    )

    chunks = pipeline.plan_chunks({"params": {}})
    assert chunks == [{"params": {}}]


def test_rt_etf_hist_xueqiu_plan_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = Mock(spec=Engine)
    codes = [f"{i:06d}.SH" for i in range(150)]
    monkeypatch.setattr(rt_etf_hist_pipeline, "load_etf_codes", lambda *_: codes)
    monkeypatch.setattr(rt_etf_hist_pipeline, "_should_fetch", lambda *_: True)

    pipeline = RtEtfHistXueqiuPipeline(
        engine=engine,
        rt_fetch_interval_s=600,
    )

    chunks = pipeline.plan_chunks({"params": {}})
    assert len(chunks) == 2
    assert len(chunks[0]["params"]["codes"]) == 100
    assert len(chunks[1]["params"]["codes"]) == 50
    assert chunks[0]["params"]["codes"][0]["api_code"] == "SH000000"
