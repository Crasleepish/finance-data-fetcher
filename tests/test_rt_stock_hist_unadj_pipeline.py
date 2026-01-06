from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import MagicMock, Mock

from sqlalchemy.engine import Engine

from services.pipelines.rt_stock_hist_unadj_pipeline import RtStockHistUnadjTusharePipeline


def test_rt_stock_hist_unadj_interval_gate_skips(monkeypatch: object) -> None:
    engine = Mock(spec=Engine)
    connection = Mock()
    manager = MagicMock()
    manager.__enter__.return_value = connection
    engine.begin.return_value = manager
    now = datetime.now()
    connection.execute.return_value.scalar.return_value = now - timedelta(seconds=10)

    pipeline = RtStockHistUnadjTusharePipeline(
        engine=engine,
        fetcher=Mock(),
        rt_fetch_interval_s=600,
    )

    chunks = pipeline.plan_chunks({"params": {}})

    assert chunks == []
