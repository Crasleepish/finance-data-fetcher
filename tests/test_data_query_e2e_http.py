from __future__ import annotations

from datetime import date

from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import insert
from sqlalchemy.engine import Engine

from api.routers.data import router as data_router
from infra.db.tables import stock_hist_unadj, stock_info
from services.data_query_service import DataQueryService


def _build_test_app(engine: Engine) -> FastAPI:
    app = FastAPI()
    app.state.data_query_service = DataQueryService(engine=engine)
    app.include_router(data_router)
    return app


def test_http_e2e_results_flow(postgres_engine: Engine) -> None:
    with postgres_engine.begin() as connection:
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

    client = TestClient(_build_test_app(postgres_engine))
    list_response = client.get(
        "/data/list",
        params={"data_type": "stock", "keyword": "茅台"},
    )
    assert list_response.status_code == 200
    list_payload = list_response.json()
    assert list_payload["success"] is True
    assert list_payload["data"]["items"][0]["code"] == "600519"

    results_response = client.get(
        "/data/results",
        params={
            "data_type": "stock",
            "asset_code": "600519",
            "start_date": "2024-01-01",
            "end_date": "2024-01-03",
        },
    )
    assert results_response.status_code == 200
    results_payload = results_response.json()
    assert results_payload["success"] is True
    assert results_payload["meta"]["count"] == 1
    point = results_payload["data"]["points"][0]
    assert point["amount"] == 100.0
    assert point["pct_change"] == 1.0
