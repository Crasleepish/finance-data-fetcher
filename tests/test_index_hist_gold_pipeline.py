from __future__ import annotations

from datetime import date
from unittest.mock import Mock

from services.pipelines.index_hist_gold_pipeline import IndexHistGoldPipeline


def test_index_hist_gold_pipeline_year_chunks() -> None:
    pipeline = IndexHistGoldPipeline(
        calendar=Mock(),
        client=Mock(),
        retry_policy=Mock(),
        engine=Mock(),
        index_info_table=Mock(),
        codes_raw="",
    )
    object.__setattr__(pipeline, "_validated", True)
    object.__setattr__(
        pipeline,
        "_codes",
        [{"index_code": "Au99.99.SGE", "api_code": "Au99.99"}],
    )

    chunks = pipeline.plan_chunks(
        {"params": {"start_date": "2024-12-31", "end_date": "2025-01-02"}}
    )

    assert chunks == [
        {
            "params": {
                "start_date": date(2024, 12, 31).isoformat(),
                "end_date": date(2024, 12, 31).isoformat(),
                "codes": [{"index_code": "Au99.99.SGE", "api_code": "Au99.99"}],
            }
        },
        {
            "params": {
                "start_date": date(2025, 1, 1).isoformat(),
                "end_date": date(2025, 1, 2).isoformat(),
                "codes": [{"index_code": "Au99.99.SGE", "api_code": "Au99.99"}],
            }
        },
    ]
