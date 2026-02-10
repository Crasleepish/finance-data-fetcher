from __future__ import annotations

from services.pipelines.index_hist_global_pipeline import _convert_global_code


def test_index_hist_global_code_mapping() -> None:
    assert _convert_global_code("HKTECH.GLB") == "HSTECH"
    assert _convert_global_code("HSI.GLB") == "HSI"
