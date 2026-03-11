from __future__ import annotations

from datetime import date

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import insert
from sqlalchemy.engine import Engine

from api.routers.data import router as data_router
from infra.db.tables import (
    etf_hist,
    etf_info,
    fund_hist,
    fund_info,
    index_hist,
    index_info,
    stock_hist_unadj,
    stock_info,
)
from services.data_query_service import DataQueryService


def _build_test_app(engine: Engine) -> FastAPI:
    app = FastAPI()
    app.state.data_query_service = DataQueryService(engine=engine)
    app.include_router(data_router)
    return app


def _seed_stock(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            insert(stock_info).values(
                stock_code="600519",
                stock_name="贵州茅台",
                market="SSE",
                exchange="SSE",
                industry="liquor",
                listing_date=date(2001, 1, 1),
                list_status="L",
            )
        )
        connection.execute(
            insert(stock_hist_unadj).values(
                stock_code="600519",
                date=date(2024, 1, 2),
                open=185.2,
                close=188.4,
                high=189.0,
                low=184.5,
                volume=3200000,
                amount=100.0,
                pre_close=180.0,
                change_percent=1.0,
                change=1.0,
                turnover_rate=1.0,
                turnover_rate_f=1.0,
                volume_ratio=1.0,
                pe=1.0,
                pe_ttm=1.0,
                pb=1.0,
                ps=1.0,
                ps_ttm=1.0,
                dv_ratio=1.0,
                dv_ttm=1.0,
                total_share=100,
                float_share=100,
                free_share=100,
                mkt_cap=100,
                circ_mv=100,
                is_suspend="N",
            )
        )


def _seed_index(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            insert(index_info).values(index_code="000300.SH", index_name="沪深300", market="CSI")
        )
        connection.execute(
            insert(index_hist).values(
                index_code="000300.SH",
                date=date(2024, 1, 2),
                open=100.0,
                close=110.0,
                high=120.0,
                low=90.0,
                volume=1000,
                amount=200.0,
                change_percent=1.0,
                change=1.0,
            )
        )


def _seed_etf(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(insert(etf_info).values(etf_code="510300.SH", etf_name="沪深300ETF"))
        connection.execute(
            insert(etf_hist).values(
                etf_code="510300.SH",
                date=date(2024, 1, 2),
                open=1.0,
                close=1.1,
                high=1.2,
                low=0.9,
                volume=100,
                amount=10.0,
                change_percent=1.0,
                change=1.0,
            )
        )


def _seed_fund(engine: Engine) -> None:
    with engine.begin() as connection:
        connection.execute(
            insert(fund_info).values(
                fund_code="000001.OF",
                fund_name="测试基金",
                fund_type="指数",
                invest_type="被动指数型",
                found_date=date(2000, 1, 1),
                fee_rate=0.1,
                commission_rate=0.1,
                market="CNY",
            )
        )
        connection.execute(
            insert(fund_hist).values(
                fund_code="000001.OF",
                date=date(2024, 1, 2),
                value=1.5,
                net_value=1.6,
            )
        )


def test_results_stock(postgres_engine: Engine) -> None:
    _seed_stock(postgres_engine)
    client = TestClient(_build_test_app(postgres_engine))
    response = client.get(
        "/data/results",
        params={
            "data_type": "stock",
            "asset_code": "600519",
            "start_date": "2024-01-01",
            "end_date": "2024-01-03",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert payload["data"]["asset_code"] == "600519"
    assert payload["meta"]["count"] == 1
    point = payload["data"]["points"][0]
    assert point["open"] == 185.2
    assert point["close"] == 188.4
    assert point["amount"] == 100.0
    assert point["pct_change"] == 1.0


def test_results_index(postgres_engine: Engine) -> None:
    _seed_index(postgres_engine)
    client = TestClient(_build_test_app(postgres_engine))
    response = client.get(
        "/data/results",
        params={
            "data_type": "index",
            "asset_code": "000300.SH",
            "start_date": "2024-01-01",
            "end_date": "2024-01-03",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert payload["data"]["asset_code"] == "000300.SH"
    assert payload["meta"]["count"] == 1


def test_results_etf(postgres_engine: Engine) -> None:
    _seed_etf(postgres_engine)
    client = TestClient(_build_test_app(postgres_engine))
    response = client.get(
        "/data/results",
        params={
            "data_type": "etf",
            "asset_code": "510300.SH",
            "start_date": "2024-01-01",
            "end_date": "2024-01-03",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert payload["meta"]["count"] == 1


def test_results_fund_maps_net_value(postgres_engine: Engine) -> None:
    _seed_fund(postgres_engine)
    client = TestClient(_build_test_app(postgres_engine))
    response = client.get(
        "/data/results",
        params={
            "data_type": "fund",
            "asset_code": "000001.OF",
            "start_date": "2024-01-01",
            "end_date": "2024-01-03",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    point = payload["data"]["points"][0]
    assert point["open"] == 1.6
    assert point["high"] == 1.6
    assert point["low"] == 1.6
    assert point["close"] == 1.6
    assert point["volume"] is None
    assert point["amount"] is None
    assert point["pct_change"] is None


def test_results_empty_returns_success(postgres_engine: Engine) -> None:
    client = TestClient(_build_test_app(postgres_engine))
    response = client.get(
        "/data/results",
        params={
            "data_type": "stock",
            "asset_code": "600519",
            "start_date": "2024-01-01",
            "end_date": "2024-01-03",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert payload["data"]["points"] == []
    assert payload["meta"]["count"] == 0


def test_results_validation_error_envelope(postgres_engine: Engine) -> None:
    client = TestClient(_build_test_app(postgres_engine))
    response = client.get(
        "/data/results",
        params={
            "data_type": "stock",
            "asset_code": "600519",
            "start_date": "2024-01-03",
            "end_date": "2024-01-01",
        },
    )
    assert response.status_code == 400
    payload = response.json()
    assert payload["success"] is False
    assert payload["error"] == "start_date must be <= end_date"
    assert payload["data"] is None
    assert payload["meta"] is None


def test_list_assets_with_keyword_and_pagination(postgres_engine: Engine) -> None:
    _seed_stock(postgres_engine)
    client = TestClient(_build_test_app(postgres_engine))
    response = client.get(
        "/data/list",
        params={
            "data_type": "stock",
            "keyword": "贵州",
            "page": 1,
            "page_size": 10,
            "order": "name_asc",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert payload["data"]["items"][0]["code"] == "600519"
    assert payload["data"]["items"][0]["currency"] == "CNY"
    assert payload["meta"]["page"] == 1
    assert payload["meta"]["total"] == 1


def test_list_assets_empty_result(postgres_engine: Engine) -> None:
    client = TestClient(_build_test_app(postgres_engine))
    response = client.get(
        "/data/list",
        params={
            "data_type": "stock",
            "keyword": "missing",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
    assert payload["data"]["items"] == []
    assert payload["meta"]["total"] == 0


def test_list_validation_error_envelope(postgres_engine: Engine) -> None:
    client = TestClient(_build_test_app(postgres_engine))
    response = client.get(
        "/data/list",
        params={
            "data_type": "stock",
            "page": 0,
        },
    )
    assert response.status_code == 400
    payload = response.json()
    assert payload["success"] is False
    assert payload["error"] == "page must be positive"
    assert payload["data"] is None
    assert payload["meta"] is None


def test_data_list_sql_injection_keyword_is_safe(postgres_engine: Engine) -> None:
    _seed_stock(postgres_engine)
    client = TestClient(_build_test_app(postgres_engine))
    response = client.get(
        "/data/list",
        params={
            "data_type": "stock",
            "keyword": "' OR '1'='1",
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["success"] is True
