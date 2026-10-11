from __future__ import annotations

import importlib.util
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import create_engine

from core.pipeline.registry import PipelineRegistry
from core.workflow.failover import FetchCleanFailoverPolicy
from infra.db.tables import stock_hist_unadj, stock_info
from models.task_payload import PipelineTask
from models.task_spec import TaskSpec
from models.task_status import TaskState
from services.pipeline_selector import PipelineSelector
from services.pipelines.stock_code_change_pipeline import StockCodeChangePipeline
from services.workflow_engine import WorkflowEngine


def _load_task_pipeline_mapping() -> dict[Any, list[str]]:
    path = Path(__file__).resolve().parents[1] / "config" / "task_pipeline_mapping.py"
    spec = importlib.util.spec_from_file_location("_task_pipeline_mapping", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.TASK_PIPELINE_MAPPING


class FakeBasicFetcher:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.calls = 0

    def fetch(self, chunk_args: Any) -> list[dict[str, Any]]:
        self.calls += 1
        return list(self._rows)


class FakeCodeChangeFetcher:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows
        self.calls = 0
        self.last_codes: list[str] = []
        self.last_cancel_check: Any = None

    def fetch(self, codes: Any, *, cancel_check: Any = None) -> list[dict[str, Any]]:
        self.calls += 1
        self.last_codes = list(codes)
        self.last_cancel_check = cancel_check
        return list(self._rows)


class FailingEngine:
    def begin(self) -> Any:
        raise RuntimeError("db unavailable")


def _build_engine() -> Any:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    stock_hist_unadj.create(engine)
    stock_info.create(engine)
    with engine.begin() as conn:
        conn.execute(
            stock_hist_unadj.insert(),
            [{"stock_code": "001914.SZ", "date": date(2020, 1, 1)}],
        )
        conn.execute(
            stock_info.insert(),
            [
                {
                    "stock_code": "600000.SH",
                    "stock_name": "浦发银行",
                    "listing_date": date(1999, 11, 10),
                }
            ],
        )
    return engine


def _build_pipeline(engine: Any, basic: Any, code_change: Any) -> StockCodeChangePipeline:
    return StockCodeChangePipeline(
        basic_fetcher=basic,
        code_change_fetcher=code_change,
        engine=engine,
        stock_hist_table=stock_hist_unadj,
        stock_info_table=stock_info,
    )


def test_task_spec_and_mapping_registered() -> None:
    assert TaskSpec.GET_STOCK_CODE_CHANGE == "get_stock_code_change"
    mapping = _load_task_pipeline_mapping()
    assert mapping[TaskSpec.GET_STOCK_CODE_CHANGE] == ["stock_code_change"]


def test_plan_chunks_merges_universe_sources() -> None:
    basic = FakeBasicFetcher(
        [
            {"ts_code": "000043.SZ", "list_status": "L"},
            {"ts_code": "920001.BJ", "list_status": "D"},
        ]
    )
    code_change = FakeCodeChangeFetcher([])
    pipeline = _build_pipeline(_build_engine(), basic, code_change)
    chunks = pipeline.plan_chunks({})
    assert len(chunks) == 1
    codes = chunks[0]["params"]["codes"]
    assert codes == ["000043.SZ", "001914.SZ", "600000.SH", "920001.BJ"]


def test_universe_malformed_basic_record_aborts() -> None:
    basic = FakeBasicFetcher([{"ts_code": "000043.SZ", "list_status": "L"}, "not-a-mapping"])
    code_change = FakeCodeChangeFetcher([])
    pipeline = _build_pipeline(_build_engine(), basic, code_change)
    with pytest.raises(ValueError, match="malformed stock_basic record"):
        pipeline.plan_chunks({})
    assert code_change.calls == 0


def test_universe_missing_basic_code_aborts() -> None:
    basic = FakeBasicFetcher([{"ts_code": None, "list_status": "L"}])
    code_change = FakeCodeChangeFetcher([])
    pipeline = _build_pipeline(_build_engine(), basic, code_change)
    with pytest.raises(ValueError, match="malformed stock_basic code"):
        pipeline.plan_chunks({})
    assert code_change.calls == 0


def test_universe_no_fresh_listed_codes_aborts() -> None:
    basic = FakeBasicFetcher([{"ts_code": "000043.SZ", "list_status": "D"}])
    code_change = FakeCodeChangeFetcher([])
    pipeline = _build_pipeline(_build_engine(), basic, code_change)
    with pytest.raises(ValueError, match="no fresh listed"):
        pipeline.plan_chunks({})
    assert code_change.calls == 0


_EXCLUDED_CODES = ["T600018.SH", "T00018.SH", "TS0018.SH"]


@pytest.mark.parametrize("excluded", _EXCLUDED_CODES)
def test_excluded_legacy_basic_code_skipped(excluded: str) -> None:
    basic = FakeBasicFetcher(
        [
            {"ts_code": "000043.SZ", "list_status": "L"},
            {"ts_code": excluded, "list_status": "D"},
        ]
    )
    code_change = FakeCodeChangeFetcher([])
    pipeline = _build_pipeline(_build_engine(), basic, code_change)
    chunks = pipeline.plan_chunks({})
    codes = chunks[0]["params"]["codes"]
    assert excluded not in codes
    assert "000043.SZ" in codes
    assert code_change.calls == 0


def _build_engine_with_codes(hist_codes: list[str], info_codes: list[str]) -> Any:
    engine = create_engine("sqlite+pysqlite:///:memory:")
    stock_hist_unadj.create(engine)
    stock_info.create(engine)
    with engine.begin() as conn:
        if hist_codes:
            conn.execute(
                stock_hist_unadj.insert(),
                [{"stock_code": code, "date": date(2020, 1, 1)} for code in hist_codes],
            )
        if info_codes:
            conn.execute(
                stock_info.insert(),
                [
                    {"stock_code": code, "stock_name": "N", "listing_date": date(1999, 11, 10)}
                    for code in info_codes
                ],
            )
    return engine


@pytest.mark.parametrize("excluded", _EXCLUDED_CODES)
def test_excluded_legacy_db_code_skipped(excluded: str) -> None:
    engine = _build_engine_with_codes([excluded, "001914.SZ"], [excluded])
    basic = FakeBasicFetcher([{"ts_code": "000043.SZ", "list_status": "L"}])
    code_change = FakeCodeChangeFetcher([])
    pipeline = _build_pipeline(engine, basic, code_change)
    chunks = pipeline.plan_chunks({})
    codes = chunks[0]["params"]["codes"]
    assert excluded not in codes
    assert "000043.SZ" in codes
    assert code_change.calls == 0


@pytest.mark.parametrize("malformed", ["T99999.SH", "BAD.SH"])
def test_non_excluded_malformed_code_still_aborts(malformed: str) -> None:
    basic = FakeBasicFetcher(
        [
            {"ts_code": "000043.SZ", "list_status": "L"},
            {"ts_code": malformed, "list_status": "D"},
        ]
    )
    code_change = FakeCodeChangeFetcher([])
    pipeline = _build_pipeline(_build_engine(), basic, code_change)
    with pytest.raises(ValueError, match="expected suffixed"):
        pipeline.plan_chunks({})
    assert code_change.calls == 0


def test_excluded_count_surfaced(caplog: pytest.LogCaptureFixture) -> None:
    basic = FakeBasicFetcher(
        [
            {"ts_code": "000043.SZ", "list_status": "L"},
            {"ts_code": "T600018.SH", "list_status": "D"},
        ]
    )
    engine = _build_engine_with_codes(["T00018.SH", "TS0018.SH"], [])
    code_change = FakeCodeChangeFetcher([])
    pipeline = _build_pipeline(engine, basic, code_change)

    with caplog.at_level("INFO", logger="services.pipelines.stock_code_change_pipeline"):
        pipeline.plan_chunks({})

    record = next(
        r for r in caplog.records if r.message == "stock code change universe excluded legacy codes"
    )
    assert record.excluded_count == 3
    assert record.excluded_basic_count == 1
    assert record.excluded_db_count == 2


def test_excluded_legacy_codes_absent_from_valid_universe() -> None:
    basic = FakeBasicFetcher(
        [
            {"ts_code": "000043.SZ", "list_status": "L"},
            {"ts_code": "TS0018.SH", "list_status": "D"},
        ]
    )
    code_change = FakeCodeChangeFetcher([])
    pipeline = _build_pipeline(_build_engine(), basic, code_change)
    codes = pipeline.plan_chunks({})[0]["params"]["codes"]
    assert codes == ["000043.SZ", "001914.SZ", "600000.SH"]


def test_db_read_failure_aborts_before_ftshare() -> None:
    basic = FakeBasicFetcher([{"ts_code": "000043.SZ", "list_status": "L"}])
    code_change = FakeCodeChangeFetcher([])
    pipeline = _build_pipeline(FailingEngine(), basic, code_change)
    with pytest.raises(RuntimeError, match="db unavailable"):
        pipeline.plan_chunks({})
    assert code_change.calls == 0


def test_fetch_forwards_codes_and_cancel_check() -> None:
    basic = FakeBasicFetcher([{"ts_code": "000043.SZ", "list_status": "L"}])
    code_change = FakeCodeChangeFetcher([{"trade_code": "000043.SZ"}])
    pipeline = _build_pipeline(_build_engine(), basic, code_change)
    chunk = pipeline.plan_chunks({})[0]

    def cancel() -> bool:
        return False

    chunk["cancel_check"] = cancel
    rows = pipeline.fetch(chunk)
    assert code_change.last_codes == chunk["params"]["codes"]
    assert code_change.last_cancel_check is cancel
    assert list(rows) == [{"trade_code": "000043.SZ"}]


def test_universe_rejects_unsuffixed_basic_code() -> None:
    basic = FakeBasicFetcher([{"ts_code": "000043"}])
    code_change = FakeCodeChangeFetcher([])
    pipeline = _build_pipeline(_build_engine(), basic, code_change)
    with pytest.raises(ValueError, match="expected suffixed"):
        pipeline.plan_chunks({})


def test_clean_delegates_to_cleaner() -> None:
    basic = FakeBasicFetcher([])
    code_change = FakeCodeChangeFetcher([])
    pipeline = _build_pipeline(_build_engine(), basic, code_change)
    rows = pipeline.clean(
        [
            {
                "trade_code": "000043.SZ",
                "code": "000043.SZ",
                "name": "中航善达",
                "start_date": "19910101",
                "end_date": None,
            }
        ]
    )
    assert len(list(rows)) == 1


# ---------------------------------------------------------------------------
# Workflow integration (fakes for store/repo; no DB or network).
# ---------------------------------------------------------------------------


@dataclass
class FakeStatus:
    state: TaskState


class FakeStore:
    """Minimal TaskStatusStore double: no persistence."""

    def __init__(self, arguments: dict[str, Any]) -> None:
        self.arguments = arguments
        self.states: list[TaskState] = []
        self.errors: list[str] = []

    def get_task_payload(self, task_id: int) -> PipelineTask:
        return PipelineTask(
            spec=TaskSpec.GET_STOCK_CODE_CHANGE,
            pipeline_id="stock_code_change",
            source="unit-test",
            task_type="stock_code_change",
            arguments=self.arguments,  # type: ignore[arg-type]
            options={},
        )

    def update_state(
        self, task_id: int, new_state: TaskState, error: str | None = None
    ) -> FakeStatus:
        self.states.append(new_state)
        if error:
            self.errors.append(error)
        return FakeStatus(state=new_state)

    def update_progress(self, task_id: int, progress: Any) -> None:
        return None

    def get_by_id(self, task_id: int) -> FakeStatus:
        return FakeStatus(state=self.states[-1] if self.states else TaskState.PENDING)


class FakeRepository:
    """Records replace/upsert/insert calls and holds the current snapshot."""

    def __init__(self, initial: list[dict[str, Any]] | None = None) -> None:
        self.rows: list[dict[str, Any]] = list(initial or [])
        self.replace_calls = 0
        self.upsert_calls = 0
        self.insert_calls = 0

    def replace_all(self, records: Any) -> int:
        self.replace_calls += 1
        self.rows = list(records)
        return len(self.rows)

    def upsert_batch(self, records: Any, unique_keys: Any) -> int:
        self.upsert_calls += 1
        return 0

    def insert_batch(self, records: Any) -> int:
        self.insert_calls += 1
        return 0


def _build_workflow(store: FakeStore, repo: FakeRepository) -> WorkflowEngine:
    return WorkflowEngine(
        store=store,  # type: ignore[arg-type]
        registry=PipelineRegistry(),
        selector=PipelineSelector(mapping={TaskSpec.GET_STOCK_CODE_CHANGE: ["stock_code_change"]}),
        repo=repo,  # type: ignore[arg-type]
        repo_by_pipeline={},  # uses fallback repo
        replace_by_pipeline={"stock_code_change"},
        failover=FetchCleanFailoverPolicy(),
    )


def _valid_raw() -> list[dict[str, Any]]:
    return [
        {
            "trade_code": "600000.SH",
            "code": "600000.SH",
            "name": "浦发银行",
            "start_date": "19991110",
            "end_date": None,
        }
    ]


def _previous_rows() -> list[dict[str, Any]]:
    return [
        {
            "trade_code": "OLD.SH",
            "code": "OLD.SH",
            "name": "旧",
            "start_date": date(2000, 1, 1),
            "end_date": None,
        }
    ]


def test_workflow_replace_all_once_after_global_verification() -> None:
    previous = _previous_rows()
    repo = FakeRepository(initial=previous)
    store = FakeStore(arguments={"params": {}})
    basic = FakeBasicFetcher([{"ts_code": "600000.SH", "list_status": "L"}])
    code_change = FakeCodeChangeFetcher(_valid_raw())
    pipeline = _build_pipeline(_build_engine(), basic, code_change)
    workflow = _build_workflow(store, repo)
    workflow.registry.register("stock_code_change", pipeline)

    workflow.handle(1, store.get_task_payload(1))

    assert store.states[-1] == TaskState.SUCCEEDED
    assert repo.replace_calls == 1
    assert repo.upsert_calls == 0
    assert repo.insert_calls == 0
    assert repo.rows == [
        {
            "trade_code": "600000.SH",
            "code": "600000.SH",
            "name": "浦发银行",
            "start_date": date(1999, 11, 10),
            "end_date": None,
        }
    ]


def test_workflow_clean_failure_leaves_previous_snapshot_intact() -> None:
    previous = _previous_rows()
    repo = FakeRepository(initial=previous)
    store = FakeStore(arguments={"params": {}})
    basic = FakeBasicFetcher([{"ts_code": "600000.SH", "list_status": "L"}])
    # Invalid: no open interval -> cleaner aborts globally.
    code_change = FakeCodeChangeFetcher(
        [
            {
                "trade_code": "600000.SH",
                "code": "600000.SH",
                "name": "浦发银行",
                "start_date": "19991110",
                "end_date": "20001231",
            }
        ]
    )
    pipeline = _build_pipeline(_build_engine(), basic, code_change)
    workflow = _build_workflow(store, repo)
    workflow.registry.register("stock_code_change", pipeline)

    workflow.handle(1, store.get_task_payload(1))

    assert store.states[-1] == TaskState.FAILED
    assert repo.replace_calls == 0
    assert repo.rows == previous


def test_workflow_fetch_failure_leaves_previous_snapshot_intact() -> None:
    previous = _previous_rows()
    repo = FakeRepository(initial=previous)
    store = FakeStore(arguments={"params": {}})
    basic = FakeBasicFetcher([{"ts_code": "600000.SH", "list_status": "L"}])

    class BoomFetcher(FakeCodeChangeFetcher):
        def fetch(self, codes: Any, *, cancel_check: Any = None) -> list[dict[str, Any]]:
            self.calls += 1
            raise RuntimeError("ftshare unavailable")

    pipeline = _build_pipeline(_build_engine(), basic, BoomFetcher([]))
    workflow = _build_workflow(store, repo)
    workflow.registry.register("stock_code_change", pipeline)

    workflow.handle(1, store.get_task_payload(1))

    assert store.states[-1] == TaskState.FAILED
    assert repo.replace_calls == 0
    assert repo.rows == previous
