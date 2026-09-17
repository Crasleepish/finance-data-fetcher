from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

import pytest
from sqlalchemy.dialects import postgresql

from core.clean.etf_info_cleaner import EtfInfoCleaner
from core.fetch.retry import RetryPolicy
from infra.db.repository import PostgresUpsertStrategy
from infra.db.tables import etf_info
from infra.fetcher.tushare_fund_basic_fetcher import (
    _DEFAULT_FIELDS,
    DuplicateFundCodeError,
    TushareFundBasicFetcher,
    _normalize_fields,
)
from services.pipelines.etf_info_pipeline import EtfInfoPipeline


@dataclass
class FakeTushareClient:
    pages: dict[tuple[str, str, int], list[dict[str, object]]] = field(default_factory=dict)
    calls: list[tuple[str, str, str, int, int]] = field(default_factory=list)

    def fund_basic(
        self,
        market: str,
        status: str,
        fields: str,
        offset: int,
        limit: int,
    ) -> list[dict[str, object]]:
        self.calls.append((market, status, fields, offset, limit))
        return self.pages.get((market, status, offset), [])


def _build_pipeline(client: FakeTushareClient, page_size: int = 4000) -> EtfInfoPipeline:
    pipeline = EtfInfoPipeline(client=client, retry_policy=RetryPolicy())
    object.__setattr__(
        pipeline,
        "_fetcher",
        TushareFundBasicFetcher(client=client, retry_policy=RetryPolicy(), page_size=page_size),
    )
    return pipeline


def test_etf_info_cleaner_maps_fields() -> None:
    cleaner = EtfInfoCleaner()
    raw = [
        {
            "ts_code": "510300.SH",
            "name": "沪深300ETF",
            "fund_type": "ETF",
            "invest_type": "被动指数型",
            "found_date": "20120528",
            "list_date": "20120528",
            "delist_date": "20240102",
        },
        {
            "ts_code": "510500.SH",
            "name": "中证500ETF",
            "fund_type": "ETF",
            "invest_type": None,
            "found_date": None,
            "list_date": "20130206",
            "delist_date": None,
        },
    ]

    cleaned = list(cleaner.clean(raw))

    assert cleaned == [
        {
            "etf_code": "510300.SH",
            "etf_name": "沪深300ETF",
            "fund_type": "ETF",
            "invest_type": "被动指数型",
            "found_date": date(2012, 5, 28),
            "list_date": date(2012, 5, 28),
            "delist_date": date(2024, 1, 2),
        },
        {
            "etf_code": "510500.SH",
            "etf_name": "中证500ETF",
            "fund_type": "ETF",
            "invest_type": None,
            "found_date": None,
            "list_date": date(2013, 2, 6),
            "delist_date": None,
        },
    ]


def test_etf_info_cleaner_normalizes_blank_dates_to_none() -> None:
    cleaner = EtfInfoCleaner()
    raw = [
        {
            "ts_code": "510600.SH",
            "name": "空白日期ETF",
            "fund_type": "ETF",
            "invest_type": None,
            "list_date": "   ",
            "delist_date": "",
        },
        {
            "ts_code": "510700.SH",
            "name": "显式None ETF",
            "fund_type": "ETF",
            "invest_type": None,
            "list_date": None,
            "delist_date": None,
        },
    ]

    cleaned = list(cleaner.clean(raw))

    # Both date keys are always emitted when upstream returned the field, so the
    # batch keeps a stable key set for multi-row upserts.
    assert cleaned == [
        {
            "etf_code": "510600.SH",
            "etf_name": "空白日期ETF",
            "fund_type": "ETF",
            "invest_type": None,
            "list_date": None,
            "delist_date": None,
        },
        {
            "etf_code": "510700.SH",
            "etf_name": "显式None ETF",
            "fund_type": "ETF",
            "invest_type": None,
            "list_date": None,
            "delist_date": None,
        },
    ]


def test_etf_info_cleaner_output_compiles_as_single_upsert_statement() -> None:
    """A cleaned mixed batch must compile as one multi-row upsert with list_date.

    This is compile-only evidence: no engine/connection is created and nothing is
    written. It guards the heterogeneous-values case where one row has a valid
    ``list_date`` and another has ``None``.
    """
    cleaner = EtfInfoCleaner()
    raw = [
        {
            "ts_code": "510300.SH",
            "name": "沪深300ETF",
            "fund_type": "ETF",
            "invest_type": "被动指数型",
            "found_date": "20120528",
            "list_date": "20120528",
            "delist_date": None,
        },
        {
            "ts_code": "510600.SH",
            "name": "空白日期ETF",
            "fund_type": "ETF",
            "invest_type": None,
            "found_date": "20130101",
            "list_date": "   ",
            "delist_date": "",
        },
    ]
    records = list(cleaner.clean(raw))
    assert records[0]["list_date"] == date(2012, 5, 28)
    assert records[1]["list_date"] is None
    assert records[1]["delist_date"] is None

    stmt = PostgresUpsertStrategy().build_upsert(etf_info, records, ["etf_code"])
    sql = str(stmt.compile(dialect=postgresql.dialect()))

    assert "INSERT INTO etf_info" in sql
    assert "ON CONFLICT (etf_code) DO UPDATE" in sql
    assert sql.count("list_date") >= 2  # insert column + excluded reference
    assert "delist_date" in sql


def test_etf_info_cleaner_strips_date_whitespace() -> None:
    cleaner = EtfInfoCleaner()
    raw = [
        {
            "ts_code": "510800.SH",
            "name": "空白填充ETF",
            "found_date": " 20150101 ",
            "list_date": "\t20120528\n",
            "delist_date": " 20200102 ",
        }
    ]

    cleaned = list(cleaner.clean(raw))

    assert cleaned == [
        {
            "etf_code": "510800.SH",
            "etf_name": "空白填充ETF",
            "found_date": date(2015, 1, 1),
            "list_date": date(2012, 5, 28),
            "delist_date": date(2020, 1, 2),
        }
    ]


def test_etf_info_fetcher_default_fields_include_mandatory_columns() -> None:
    fields = _DEFAULT_FIELDS.split(",")
    for expected in ("ts_code", "name", "fund_type", "invest_type", "found_date", "m_fee", "c_fee"):
        assert expected in fields
    assert "market" in fields
    assert "list_date" in fields
    assert "delist_date" in fields


def test_normalize_fields_appends_mandatory_columns() -> None:
    assert _normalize_fields("ts_code,close") == "ts_code,close,name,list_date,delist_date"
    assert _normalize_fields("ts_code,name") == "ts_code,name,list_date,delist_date"
    assert _normalize_fields("name, ts_code ,name") == "name,ts_code,list_date,delist_date"
    assert _normalize_fields(["delist_date", "list_date", "ts_code", "name"]) == (
        "delist_date,list_date,ts_code,name"
    )


def test_normalize_fields_rejects_invalid_type() -> None:
    with pytest.raises(ValueError, match="fields"):
        _normalize_fields(123)


def test_etf_info_plan_chunks_defaults_to_single_chunk_with_all_statuses() -> None:
    pipeline = _build_pipeline(FakeTushareClient())

    chunks = pipeline.plan_chunks({"params": {}})

    assert chunks == [{"params": {"market": "E", "statuses": ["L", "D", "I"]}}]


def test_etf_info_plan_chunks_explicit_single_status() -> None:
    pipeline = _build_pipeline(FakeTushareClient())

    chunks = pipeline.plan_chunks({"params": {"status": "D"}})

    assert chunks == [{"params": {"market": "E", "statuses": ["D"]}}]


def test_etf_info_plan_chunks_accepts_status_list_deduped_and_ordered() -> None:
    pipeline = _build_pipeline(FakeTushareClient())

    chunks = pipeline.plan_chunks({"params": {"status": ["I", "L", "I", "D"]}})

    assert chunks == [{"params": {"market": "E", "statuses": ["L", "D", "I"]}}]


@pytest.mark.parametrize("status", ["X", "P", "", ["L", "X"], ["L", ""]])
def test_etf_info_plan_chunks_rejects_invalid_status(status: object) -> None:
    pipeline = _build_pipeline(FakeTushareClient())

    with pytest.raises(ValueError, match="status"):
        pipeline.plan_chunks({"params": {"status": status}})


def test_etf_info_plan_chunks_rejects_non_string_status() -> None:
    pipeline = _build_pipeline(FakeTushareClient())

    with pytest.raises(ValueError, match="status"):
        pipeline.plan_chunks({"params": {"status": 123}})


def test_etf_info_fetcher_paginates_all_statuses_in_one_call() -> None:
    client = FakeTushareClient(
        pages={
            ("E", "L", 0): [{"ts_code": "510300.SH", "list_date": "20120528"}],
            ("E", "L", 2): [],
            ("E", "D", 0): [{"ts_code": "510001.SH", "delist_date": "20200102"}],
            ("E", "D", 2): [],
            ("E", "I", 0): [],
        }
    )
    pipeline = _build_pipeline(client, page_size=2)

    chunk = pipeline.plan_chunks({"params": {}})[0]
    rows = pipeline.fetch(chunk)

    assert [row["ts_code"] for row in rows] == ["510300.SH", "510001.SH"]
    assert [call[:2] for call in client.calls] == [
        ("E", "L"),
        ("E", "L"),
        ("E", "D"),
        ("E", "D"),
        ("E", "I"),
    ]


def test_etf_info_fetcher_sends_augmented_custom_fields() -> None:
    client = FakeTushareClient(pages={("E", "L", 0): []})
    pipeline = _build_pipeline(client)

    pipeline.fetch({"params": {"market": "E", "statuses": ["L"], "fields": "ts_code,name"}})

    assert client.calls == [("E", "L", "ts_code,name,list_date,delist_date", 0, 4000)]


def test_etf_info_fetcher_only_fetches_requested_statuses() -> None:
    client = FakeTushareClient(
        pages={("E", "L", 0): [], ("E", "D", 0): [], ("E", "I", 0): [{"ts_code": "1"}]}
    )
    pipeline = _build_pipeline(client, page_size=10)

    pipeline.fetch({"params": {"market": "E", "statuses": ["L", "D"]}})

    assert [call[:2] for call in client.calls] == [("E", "L"), ("E", "D")]


def test_etf_info_fetcher_rejects_duplicate_ts_code_across_pages() -> None:
    client = FakeTushareClient(
        pages={
            ("E", "L", 0): [{"ts_code": "510300.SH"}],
            ("E", "L", 2): [{"ts_code": "510300.SH"}],
        }
    )
    pipeline = _build_pipeline(client, page_size=2)

    with pytest.raises(DuplicateFundCodeError, match="510300.SH"):
        pipeline.fetch({"params": {"market": "E", "statuses": ["L"]}})


def test_etf_info_fetcher_rejects_duplicate_ts_code_across_statuses() -> None:
    client = FakeTushareClient(
        pages={
            ("E", "L", 0): [{"ts_code": "510300.SH"}],
            ("E", "D", 0): [{"ts_code": "510300.SH"}],
        }
    )
    pipeline = _build_pipeline(client, page_size=10)

    with pytest.raises(DuplicateFundCodeError, match="510300.SH"):
        pipeline.fetch({"params": {"market": "E", "statuses": ["L", "D"]}})


def test_etf_info_fetcher_ignores_missing_ts_code() -> None:
    client = FakeTushareClient(
        pages={
            ("E", "L", 0): [{"name": "missing-code"}],
            ("E", "L", 2): [{"ts_code": "", "name": "blank-code"}],
            ("E", "L", 4): [],
        }
    )
    pipeline = _build_pipeline(client, page_size=2)

    rows = pipeline.fetch({"params": {"market": "E", "statuses": ["L"]}})

    assert rows == [{"name": "missing-code"}, {"ts_code": "", "name": "blank-code"}]
