from __future__ import annotations

import zipfile
from datetime import date
from io import BytesIO

from core.clean.stock_is_st_from_file_cleaner import StockIsStFromFileCleaner
from infra.fetcher.stock_is_st_from_file_fetcher import StockIsStFromFileFetcher
from services.pipelines.stock_is_st_from_file_pipeline import StockIsStFromFilePipeline


def _build_zip_bytes() -> bytes:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, mode="w") as archive:
        archive.writestr(
            "A_stock_daily_unadj/000001.csv",
            (
                "日期,代码,名称,是否ST\n"
                "2024-01-02,000001,平安银行,是\n"
                "2024-01-03,000001,平安银行,否\n"
            ),
        )
        archive.writestr(
            "A_stock_daily_unadj/600000.csv",
            "日期,代码,名称,是否ST\n2024-01-02,600000,浦发银行,否\n",
        )
    return buffer.getvalue()


def test_stock_is_st_from_file_pipeline_plans_member_groups(tmp_path) -> None:
    zip_path = tmp_path / "A_stock_daily_unadj.zip"
    zip_path.write_bytes(_build_zip_bytes())
    pipeline = StockIsStFromFilePipeline()

    chunks = pipeline.plan_chunks({"params": {"zip_path": str(zip_path)}})

    assert len(chunks) == 1
    assert chunks[0]["params"]["zip_path"] == str(zip_path)
    assert chunks[0]["params"]["member_names"] == [
        "A_stock_daily_unadj/000001.csv",
        "A_stock_daily_unadj/600000.csv",
    ]


def test_stock_is_st_from_file_fetcher_reads_member_rows(tmp_path) -> None:
    zip_path = tmp_path / "A_stock_daily_unadj.zip"
    zip_path.write_bytes(_build_zip_bytes())
    fetcher = StockIsStFromFileFetcher()

    raw = fetcher.fetch(
        {
            "params": {
                "zip_path": str(zip_path),
                "member_names": ["A_stock_daily_unadj/000001.csv"],
            }
        }
    )

    assert raw == [
        {"date": "2024-01-02", "code": "000001", "is_st": "是"},
        {"date": "2024-01-03", "code": "000001", "is_st": "否"},
    ]


def test_stock_is_st_from_file_cleaner_normalizes_rows() -> None:
    cleaner = StockIsStFromFileCleaner()

    cleaned = cleaner.clean(
        [
            {"date": "2024-01-02", "code": "000001", "is_st": "是"},
            {"date": "2024-01-02", "code": "600000", "is_st": "否"},
        ]
    )

    assert cleaned == [
        {"stock_code": "000001.SZ", "date": date(2024, 1, 2), "is_st": 1},
        {"stock_code": "600000.SH", "date": date(2024, 1, 2), "is_st": 0},
    ]
