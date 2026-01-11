from __future__ import annotations

import json
import sys
from datetime import date
from types import SimpleNamespace
from unittest.mock import Mock

from infra.fetcher.akshare_index_hist_fetcher import _safe_csindex
from services.pipelines.index_hist_bond_pipeline import IndexHistBondPipeline


def test_akshare_csindex_json_decode_error_returns_empty(
    monkeypatch,
) -> None:
    def raise_decode_error(*_args, **_kwargs) -> None:
        raise json.JSONDecodeError("Expecting value", "", 0)

    monkeypatch.setitem(
        sys.modules,
        "akshare",
        SimpleNamespace(stock_zh_index_hist_csindex=raise_decode_error),
    )

    rows = _safe_csindex("H11001", "20250101", "20250101")

    assert rows == []


def test_index_hist_bond_pipeline_year_chunks() -> None:
    pipeline = IndexHistBondPipeline(
        calendar=Mock(),
        retry_policy=Mock(),
        engine=Mock(),
        index_info_table=Mock(),
        codes_raw="",
    )
    object.__setattr__(pipeline, "_validated", True)
    object.__setattr__(
        pipeline,
        "_codes",
        [{"index_code": "H11001.CSI", "api_code": "H11001"}],
    )

    chunks = pipeline.plan_chunks(
        {"params": {"start_date": "2007-12-01", "end_date": "2008-01-02"}}
    )

    assert chunks == [
        {
            "params": {
                "start_date": date(2007, 12, 1).isoformat(),
                "end_date": date(2007, 12, 31).isoformat(),
                "codes": [{"index_code": "H11001.CSI", "api_code": "H11001"}],
            }
        },
        {
            "params": {
                "start_date": date(2008, 1, 1).isoformat(),
                "end_date": date(2008, 1, 2).isoformat(),
                "codes": [{"index_code": "H11001.CSI", "api_code": "H11001"}],
            }
        },
    ]
