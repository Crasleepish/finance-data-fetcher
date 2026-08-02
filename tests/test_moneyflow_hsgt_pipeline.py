from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from types import SimpleNamespace

from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.engine import Engine

import api.main as api_main
from core.calendar.service import TradingCalendarService
from core.clean.moneyflow_hsgt_cleaner import MoneyflowHsgtCleaner
from core.fetch.retry import RetryPolicy
from core.pipeline.registry import PipelineRegistry
from infra.db.repository import Repository
from infra.db.tables import moneyflow_hsgt
from infra.fetcher.tushare_moneyflow_hsgt_fetcher import TushareMoneyflowHsgtFetcher
from infra.idempotency.guard import IdempotencyGuard
from infra.queue.in_memory import InMemoryTaskQueue
from infra.task_state.store import TaskStatusStore
from infra.worker_runtime.runtime import WorkerRuntime
from models.task_payload import PipelineTask
from models.task_spec import TaskSpec
from models.task_status import TaskState
from services.pipeline_selector import PipelineSelector
from services.pipelines.moneyflow_hsgt_pipeline import MoneyflowHsgtPipeline
from services.task_service import TaskService
from services.workflow_engine import WorkflowEngine


@dataclass
class FakeCalendarStore:
    dates: set[date]

    def get_bounds(self) -> tuple[date, date] | None:
        return (min(self.dates), max(self.dates)) if self.dates else None

    def get_trade_days(self, start: date, end: date) -> list[date]:
        return sorted(day for day in self.dates if start <= day <= end)

    def is_trade_day(self, day: date) -> bool:
        return day in self.dates

    def prev_trade_day(self, day: date) -> date | None:
        return None

    def next_trade_day(self, day: date) -> date | None:
        return None

    def insert_trade_days(self, days: list[date]) -> int:
        before = len(self.dates)
        self.dates.update(days)
        return len(self.dates) - before


@dataclass(frozen=True)
class FakeSyncer:
    def fetch_trade_days(self, start: date, end: date, exchange: str) -> list[date]:
        return []


@dataclass
class FakeTushareClient:
    calls: list[tuple[str, str, str]] = field(default_factory=list)

    def moneyflow_hsgt(
        self,
        start_date: str,
        end_date: str,
        fields: str,
    ) -> list[dict[str, object]]:
        self.calls.append((start_date, end_date, fields))
        return [
            {
                "trade_date": "20240102",
                "ggt_ss": 1.0,
                "ggt_sz": 2.0,
                "hgt": 3.0,
                "sgt": 4.0,
                "north_money": 7.0,
                "south_money": 3.0,
            },
            {
                "trade_date": "20240103",
                "ggt_ss": None,
                "ggt_sz": 5.5,
                "hgt": 6.0,
                "sgt": 7.0,
                "north_money": 13.0,
                "south_money": 5.5,
            },
        ]


def test_moneyflow_hsgt_fetcher_uses_date_range_and_default_fields() -> None:
    client = FakeTushareClient()
    fetcher = TushareMoneyflowHsgtFetcher(client=client, retry_policy=RetryPolicy())

    rows = fetcher.fetch(
        {
            "params": {
                "start_date": "2024-01-02",
                "end_date": "2024-01-05",
            }
        }
    )

    assert rows == [
        {
            "trade_date": "20240102",
            "ggt_ss": 1.0,
            "ggt_sz": 2.0,
            "hgt": 3.0,
            "sgt": 4.0,
            "north_money": 7.0,
            "south_money": 3.0,
        },
        {
            "trade_date": "20240103",
            "ggt_ss": None,
            "ggt_sz": 5.5,
            "hgt": 6.0,
            "sgt": 7.0,
            "north_money": 13.0,
            "south_money": 5.5,
        },
    ]
    assert client.calls == [
        (
            "20240102",
            "20240105",
            "trade_date,ggt_ss,ggt_sz,hgt,sgt,north_money,south_money",
        )
    ]


def test_moneyflow_hsgt_cleaner_maps_trade_date_and_scales_all_money_columns_to_yuan() -> None:
    cleaner = MoneyflowHsgtCleaner()

    cleaned = cleaner.clean(
        [
            {
                "trade_date": "20240102",
                "ggt_ss": 1.0,
                "ggt_sz": 2.25,
                "hgt": 3.5,
                "sgt": 4.75,
                "north_money": 5.0,
                "south_money": 6.125,
            }
        ]
    )

    assert cleaned == [
        {
            "date": date(2024, 1, 2),
            "ggt_ss": Decimal("1000000.00"),
            "ggt_sz": Decimal("2250000.00"),
            "hgt": Decimal("3500000.00"),
            "sgt": Decimal("4750000.00"),
            "north_money": Decimal("5000000.00"),
            "south_money": Decimal("6125000.00"),
        }
    ]


def test_moneyflow_hsgt_pipeline_plans_trade_day_chunks_of_300() -> None:
    start = date(2024, 1, 2)
    trade_days = {start + timedelta(days=offset) for offset in range(301)}
    ordered_trade_days = sorted(trade_days)
    calendar = TradingCalendarService(store=FakeCalendarStore(trade_days), syncer=FakeSyncer())
    pipeline = MoneyflowHsgtPipeline(
        calendar=calendar,
        client=FakeTushareClient(),
        retry_policy=RetryPolicy(),
    )

    chunks = pipeline.plan_chunks(
        {
            "params": {
                "start_date": ordered_trade_days[0].isoformat(),
                "end_date": ordered_trade_days[-1].isoformat(),
            }
        }
    )

    assert chunks == [
        {
            "params": {
                "start_date": ordered_trade_days[0].isoformat(),
                "end_date": ordered_trade_days[299].isoformat(),
            }
        },
        {
            "params": {
                "start_date": ordered_trade_days[300].isoformat(),
                "end_date": ordered_trade_days[300].isoformat(),
            }
        },
    ]


def test_moneyflow_hsgt_create_app_wiring_runs_task(
    monkeypatch,
    postgres_engine: Engine,
) -> None:
    trade_days = {date(2024, 1, 2), date(2024, 1, 3)}
    calendar = TradingCalendarService(store=FakeCalendarStore(trade_days), syncer=FakeSyncer())
    client = FakeTushareClient()

    monkeypatch.setattr(api_main, "create_engine_from_config", lambda config: postgres_engine)
    monkeypatch.setattr(
        api_main,
        "build_calendar_service",
        lambda config: SimpleNamespace(calendar=calendar),
    )
    monkeypatch.setattr(api_main, "TushareProClient", lambda token: client)

    with TestClient(api_main.create_app()) as app_client:
        response = app_client.post(
            "/tasks/start",
            json={
                "spec": "get_moneyflow_hsgt",
                "pipeline_id": "moneyflow_hsgt",
                "source": "unit-test",
                "task_type": "moneyflow_hsgt",
                "arguments": {"params": {"start_date": "2024-01-02", "end_date": "2024-01-03"}},
                "options": {},
            },
        )
        assert response.status_code == 200
        task_id = response.json()["task_id"]

        deadline = time.time() + 5
        while time.time() < deadline:
            status = app_client.get(f"/tasks/{task_id}")
            assert status.status_code == 200
            payload = status.json()
            if payload["state"] == "SUCCEEDED":
                break
            if payload["state"] == "FAILED":
                raise AssertionError(payload["error"])
            time.sleep(0.1)
        else:
            raise AssertionError("moneyflow_hsgt app task did not complete")

    with postgres_engine.begin() as connection:
        rows = connection.execute(
            select(
                moneyflow_hsgt.c.date,
                moneyflow_hsgt.c.ggt_ss,
                moneyflow_hsgt.c.ggt_sz,
                moneyflow_hsgt.c.hgt,
                moneyflow_hsgt.c.sgt,
                moneyflow_hsgt.c.north_money,
                moneyflow_hsgt.c.south_money,
            ).order_by(moneyflow_hsgt.c.date)
        ).all()

    assert client.calls == [
        (
            "20240102",
            "20240103",
            "trade_date,ggt_ss,ggt_sz,hgt,sgt,north_money,south_money",
        )
    ]
    assert rows == [
        (
            date(2024, 1, 2),
            Decimal("1000000.00"),
            Decimal("2000000.00"),
            Decimal("3000000.00"),
            Decimal("4000000.00"),
            Decimal("7000000.00"),
            Decimal("3000000.00"),
        ),
        (
            date(2024, 1, 3),
            None,
            Decimal("5500000.00"),
            Decimal("6000000.00"),
            Decimal("7000000.00"),
            Decimal("13000000.00"),
            Decimal("5500000.00"),
        ),
    ]


def test_moneyflow_hsgt_workflow_persists_rows(postgres_engine: Engine) -> None:
    trade_days = {date(2024, 1, 2), date(2024, 1, 3)}
    calendar = TradingCalendarService(store=FakeCalendarStore(trade_days), syncer=FakeSyncer())
    pipeline = MoneyflowHsgtPipeline(
        calendar=calendar,
        client=FakeTushareClient(),
        retry_policy=RetryPolicy(),
    )

    registry = PipelineRegistry()
    registry.register("moneyflow_hsgt", pipeline)

    store = TaskStatusStore(engine=postgres_engine)
    queue = InMemoryTaskQueue()
    guard = IdempotencyGuard(engine=postgres_engine)
    service = TaskService(store=store, queue=queue, guard=guard)
    selector = PipelineSelector(mapping={TaskSpec.GET_MONEYFLOW_HSGT: ["moneyflow_hsgt"]})
    repo = Repository(engine=postgres_engine, table=moneyflow_hsgt)
    workflow = WorkflowEngine(
        store=store,
        registry=registry,
        selector=selector,
        repo=repo,
        repo_by_pipeline={"moneyflow_hsgt": repo},
        upsert_keys_by_pipeline={"moneyflow_hsgt": ["date"]},
    )
    runtime = WorkerRuntime(queue=queue, store=store, handler=workflow)
    runtime.start()

    task = service.start_task(
        PipelineTask(
            spec=TaskSpec.GET_MONEYFLOW_HSGT,
            pipeline_id="moneyflow_hsgt",
            source="unit-test",
            task_type="moneyflow_hsgt",
            arguments={"params": {"start_date": "2024-01-02", "end_date": "2024-01-03"}},
            options={},
        )
    )

    deadline = time.time() + 5
    while time.time() < deadline:
        record = store.get_by_id(task.task_id)
        if record.state == TaskState.SUCCEEDED:
            runtime.stop()
            break
        time.sleep(0.1)
    else:
        runtime.stop()
        raise AssertionError("moneyflow_hsgt task did not complete")

    with postgres_engine.begin() as connection:
        rows = connection.execute(
            select(
                moneyflow_hsgt.c.date,
                moneyflow_hsgt.c.ggt_ss,
                moneyflow_hsgt.c.ggt_sz,
                moneyflow_hsgt.c.hgt,
                moneyflow_hsgt.c.sgt,
                moneyflow_hsgt.c.north_money,
                moneyflow_hsgt.c.south_money,
            ).order_by(moneyflow_hsgt.c.date)
        ).all()

    assert rows == [
        (
            date(2024, 1, 2),
            Decimal("1000000.00"),
            Decimal("2000000.00"),
            Decimal("3000000.00"),
            Decimal("4000000.00"),
            Decimal("7000000.00"),
            Decimal("3000000.00"),
        ),
        (
            date(2024, 1, 3),
            None,
            Decimal("5500000.00"),
            Decimal("6000000.00"),
            Decimal("7000000.00"),
            Decimal("13000000.00"),
            Decimal("5500000.00"),
        ),
    ]
