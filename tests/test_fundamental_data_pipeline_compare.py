from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

from sqlalchemy.engine import Engine

from core.fetch.retry import RetryPolicy
from infra.db.tables import fundamental_data, stock_info
from infra.fetcher.tushare_fundamental_fetcher import (
    _BALANCE_FIELDS as VIP_BALANCE_FIELDS,
)
from infra.fetcher.tushare_fundamental_fetcher import (
    _CASHFLOW_FIELDS as VIP_CASHFLOW_FIELDS,
)
from infra.fetcher.tushare_fundamental_fetcher import (
    _INCOME_FIELDS as VIP_INCOME_FIELDS,
)
from infra.fetcher.tushare_fundamental_single_fetcher import (
    _BALANCE_FIELDS as SINGLE_BALANCE_FIELDS,
)
from infra.fetcher.tushare_fundamental_single_fetcher import (
    _CASHFLOW_FIELDS as SINGLE_CASHFLOW_FIELDS,
)
from infra.fetcher.tushare_fundamental_single_fetcher import (
    _INCOME_FIELDS as SINGLE_INCOME_FIELDS,
)
from services.pipelines.fundamental_data_pipeline import FundamentalDataPipeline
from services.pipelines.fundamental_data_single_pipeline import FundamentalDataSinglePipeline


@dataclass
class FakeTushareClient:
    income_rows: list[dict[str, object]] = field(default_factory=list)
    balance_rows: list[dict[str, object]] = field(default_factory=list)
    cashflow_rows: list[dict[str, object]] = field(default_factory=list)

    def income_vip(self, period: str, fields: str) -> list[dict[str, object]]:
        return [row for row in self.income_rows if row.get("end_date") == period]

    def balancesheet_vip(self, period: str, fields: str) -> list[dict[str, object]]:
        return [row for row in self.balance_rows if row.get("end_date") == period]

    def cashflow_vip(self, period: str, fields: str) -> list[dict[str, object]]:
        return [row for row in self.cashflow_rows if row.get("end_date") == period]

    def income(
        self, ts_code: str, start_date: str, end_date: str, fields: str
    ) -> list[dict[str, object]]:
        return _filter_rows(self.income_rows, ts_code, start_date, end_date)

    def balancesheet(
        self, ts_code: str, start_date: str, end_date: str, fields: str
    ) -> list[dict[str, object]]:
        return _filter_rows(self.balance_rows, ts_code, start_date, end_date)

    def cashflow(
        self, ts_code: str, start_date: str, end_date: str, fields: str
    ) -> list[dict[str, object]]:
        return _filter_rows(self.cashflow_rows, ts_code, start_date, end_date)


def _filter_rows(
    rows: list[dict[str, object]],
    ts_code: str,
    start_date: str,
    end_date: str,
) -> list[dict[str, object]]:
    start = _normalize_date(start_date)
    end = _normalize_date(end_date)
    matched: list[dict[str, object]] = []
    for row in rows:
        if row.get("ts_code") != ts_code:
            continue
        row_date = _normalize_date(str(row.get("end_date")))
        if start <= row_date <= end:
            matched.append(row)
    return matched


def _normalize_date(value: str) -> date:
    if len(value) == 8 and value.isdigit():
        return datetime.strptime(value, "%Y%m%d").date()
    return datetime.strptime(value, "%Y-%m-%d").date()


def _run_pipeline(pipeline, arguments) -> list[dict[str, object]]:
    records: list[dict[str, object]] = []
    for chunk in pipeline.plan_chunks(arguments):
        raw = pipeline.fetch(chunk)
        cleaned = pipeline.clean(raw)
        records.extend(list(cleaned))
    return records


def test_fundamental_data_pipelines_match(postgres_engine: Engine) -> None:
    with postgres_engine.begin() as connection:
        connection.execute(
            stock_info.insert(),
            [
                {"stock_code": "000001.SZ", "stock_name": "A", "listing_date": date(2000, 1, 1)},
                {"stock_code": "000002.SZ", "stock_name": "B", "listing_date": date(2000, 1, 1)},
            ],
        )

    fake = FakeTushareClient(
        income_rows=[
            {"ts_code": "000001.SZ", "end_date": "20230331", "operate_profit": 10.0},
            {"ts_code": "000001.SZ", "end_date": "20231231", "operate_profit": 100.0},
            {
                "ts_code": "000001.SZ",
                "end_date": "20240331",
                "f_ann_date": "20240415",
                "operate_profit": 20.0,
            },
            {"ts_code": "000002.SZ", "end_date": "20230331", "operate_profit": 30.0},
            {"ts_code": "000002.SZ", "end_date": "20231231", "operate_profit": 300.0},
            {
                "ts_code": "000002.SZ",
                "end_date": "20240331",
                "f_ann_date": "20240416",
                "operate_profit": 40.0,
            },
        ],
        balance_rows=[
            {
                "ts_code": "000001.SZ",
                "end_date": "20240331",
                "f_ann_date": "20240418",
                "total_hldr_eqy_exc_min_int": 1.0,
                "total_assets": 2.0,
                "total_cur_liab": 3.0,
                "total_ncl": 4.0,
                "total_liab": 5.0,
            },
            {
                "ts_code": "000002.SZ",
                "end_date": "20240331",
                "f_ann_date": "20240417",
                "total_hldr_eqy_exc_min_int": 6.0,
                "total_assets": 7.0,
                "total_cur_liab": 8.0,
                "total_ncl": 9.0,
                "total_liab": 10.0,
            },
        ],
        cashflow_rows=[
            {
                "ts_code": "000001.SZ",
                "end_date": "20240331",
                "f_ann_date": "20240420",
                "n_cashflow_act": 11.0,
                "c_pay_acq_const_fiolta": 12.0,
            },
            {
                "ts_code": "000002.SZ",
                "end_date": "20240331",
                "f_ann_date": "20240415",
                "n_cashflow_act": 13.0,
                "c_pay_acq_const_fiolta": 14.0,
            },
        ],
    )

    vip_pipeline = FundamentalDataPipeline(
        client=fake,
        retry_policy=RetryPolicy(),
        engine=postgres_engine,
        table=fundamental_data,
    )
    single_pipeline = FundamentalDataSinglePipeline(
        client=fake,
        retry_policy=RetryPolicy(),
        engine=postgres_engine,
        stock_table=stock_info,
        fundamental_table=fundamental_data,
    )
    arguments = {
        "params": {
            "start_period": "2023-03-31",
            "end_period": "2024-03-31",
            "overwrite": True,
        }
    }

    vip_records = _run_pipeline(vip_pipeline, arguments)
    single_records = _run_pipeline(single_pipeline, arguments)

    vip_sorted = sorted(vip_records, key=lambda row: (row["stock_code"], row["report_date"]))
    single_sorted = sorted(single_records, key=lambda row: (row["stock_code"], row["report_date"]))
    assert vip_sorted == single_sorted
    by_key = {(row["stock_code"], row["report_date"]): row for row in vip_sorted}
    assert by_key[("000001.SZ", date(2024, 3, 31))]["f_ann_date"] == date(2024, 4, 20)
    assert by_key[("000002.SZ", date(2024, 3, 31))]["f_ann_date"] == date(2024, 4, 17)


def test_fundamental_fetchers_request_f_ann_date() -> None:
    expected = "f_ann_date"

    assert expected in VIP_INCOME_FIELDS
    assert expected in VIP_BALANCE_FIELDS
    assert expected in VIP_CASHFLOW_FIELDS
    assert expected in SINGLE_INCOME_FIELDS
    assert expected in SINGLE_BALANCE_FIELDS
    assert expected in SINGLE_CASHFLOW_FIELDS
