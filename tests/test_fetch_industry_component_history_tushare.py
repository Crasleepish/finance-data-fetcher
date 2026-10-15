from __future__ import annotations

import importlib.util
import sys
from datetime import date
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import (
    Column,
    Date,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    insert,
    inspect,
    select,
)

MODULE_PATH = (
    Path(__file__).resolve().parents[1] / "extra" / "fetch_industry_component_history_tushare.py"
)
_spec = importlib.util.spec_from_file_location(
    "fetch_industry_component_history_tushare", MODULE_PATH
)
assert _spec is not None and _spec.loader is not None
mod = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = mod
_spec.loader.exec_module(mod)


def _row(
    *,
    l1_code: str = "801010.SI",
    l2_code: str = "801011.SI",
    l3_code: str = "80101101.SI",
    ts_code: str = "000001.SZ",
    name: str = "平安银行",
    in_date: Any = "20000101",
    out_date: Any = None,
    is_new: str = "Y",
) -> dict[str, Any]:
    return {
        "l1_code": l1_code,
        "l1_name": "农林牧渔",
        "l2_code": l2_code,
        "l2_name": "种植业",
        "l3_code": l3_code,
        "l3_name": "粮食种植",
        "ts_code": ts_code,
        "name": name,
        "in_date": in_date,
        "out_date": out_date,
        "is_new": is_new,
    }


# ---------------------------------------------------------------------------
# Pagination
# ---------------------------------------------------------------------------


def test_paginate_handles_server_short_pages() -> None:
    dataset = [_row(ts_code=f"{i:06d}.SZ") for i in range(7)]
    calls: list[tuple[int, int]] = []

    def fetch_page(offset: int, limit: int) -> list[dict[str, Any]]:
        calls.append((offset, limit))
        return dataset[offset : offset + 3]  # server caps at 3 regardless of limit

    result = mod.paginate(fetch_page, page_size=5, max_pages=20)

    assert len(result) == 7
    assert [offset for offset, _ in calls] == [0, 3, 6, 7]


def test_paginate_rejects_repeated_page() -> None:
    page = [_row(ts_code="000001.SZ")]

    def fetch_page(offset: int, limit: int) -> list[dict[str, Any]]:
        return list(page)

    with pytest.raises(mod.PaginationError, match="repeated page"):
        mod.paginate(fetch_page, page_size=1, max_pages=10)


def test_paginate_rejects_page_overrun() -> None:
    counter = {"n": 0}

    def fetch_page(offset: int, limit: int) -> list[dict[str, Any]]:
        counter["n"] += 1
        return [_row(ts_code=f"{counter['n']:06d}.SZ")]

    with pytest.raises(mod.PaginationError, match="max_pages"):
        mod.paginate(fetch_page, page_size=1, max_pages=3)


class _NonePro:
    def index_member_all(self, **_kwargs: Any) -> None:
        return None


def test_api_fetch_page_rejects_none_payload() -> None:
    with pytest.raises(ValueError, match="None"):
        mod._api_fetch_page(_NonePro(), is_new="Y", l1_code=None, offset=0, limit=10)


# ---------------------------------------------------------------------------
# fetch_l1_codes strictness
# ---------------------------------------------------------------------------


class _ClassifyPro:
    def __init__(self, payload: Any) -> None:
        self._payload = payload

    def index_classify(self, **_kwargs: Any) -> Any:
        return self._payload


def _fetch_codes(pro: Any) -> set[str]:
    return mod.fetch_l1_codes(
        pro,
        attempts=1,
        base_delay_s=0.0,
        sleep=lambda _s: None,
        limiter=mod._RateLimiter(0.0, lambda _s: None),
    )


def test_fetch_l1_codes_rejects_none() -> None:
    with pytest.raises(ValueError, match="None"):
        _fetch_codes(_ClassifyPro(None))


def test_fetch_l1_codes_rejects_missing_field() -> None:
    with pytest.raises(ValueError, match="missing index_code"):
        _fetch_codes(_ClassifyPro([{"level": "L1"}]))


def test_fetch_l1_codes_rejects_empty() -> None:
    with pytest.raises(ValueError, match="no L1 industry codes"):
        _fetch_codes(_ClassifyPro([]))

    with pytest.raises(ValueError, match="no L1 industry codes"):
        _fetch_codes(_ClassifyPro([{"index_code": "", "level": "L1"}]))


def test_fetch_l1_codes_union_of_sources() -> None:
    # A single payload is returned for both SW2014 and SW2021; assert dedupe works.
    codes = _fetch_codes(_ClassifyPro([{"index_code": "801010.SI", "level": "L1"}]))
    assert codes == {"801010.SI"}


# ---------------------------------------------------------------------------
# Normalization / hashing
# ---------------------------------------------------------------------------


def test_parse_date_strict_and_null() -> None:
    assert mod.parse_date("20240102") == date(2024, 1, 2)
    assert mod.parse_date(None) is None
    assert mod.parse_date("") is None
    assert mod.parse_date("nan") is None
    assert mod.parse_date(float("nan")) is None
    with pytest.raises(ValueError):
        mod.parse_date("2024-01-02")
    with pytest.raises(ValueError):
        mod.parse_date("2024010")


def test_normalize_record_maps_dates_and_nulls() -> None:
    record = mod.normalize_record(_row(in_date="20100101", out_date=""))
    assert record["in_date"] == date(2010, 1, 1)
    assert record["out_date"] is None
    assert record["is_new"] == "Y"
    assert isinstance(record["row_hash"], str) and len(record["row_hash"]) == 64


def test_normalize_record_rejects_missing_and_invalid() -> None:
    incomplete = _row()
    del incomplete["out_date"]
    with pytest.raises(ValueError, match="missing required column"):
        mod.normalize_record(incomplete)

    with pytest.raises(ValueError, match="null required field"):
        mod.normalize_record(_row(l1_code=""))

    with pytest.raises(ValueError, match="is_new"):
        mod.normalize_record(_row(is_new="X"))


def test_row_hash_is_deterministic_and_null_aware() -> None:
    base = mod.normalize_record(_row(out_date=None))
    same = mod.normalize_record(_row(out_date=None))
    different_out = mod.normalize_record(_row(out_date="20200101"))
    rebuilt = mod.compute_row_hash(base)

    assert base["row_hash"] == same["row_hash"] == rebuilt
    assert base["row_hash"] != different_out["row_hash"]

    # None must not collide with the literal string "None".
    literal = dict(base)
    literal["out_date"] = "None"
    assert mod.compute_row_hash(literal) != base["row_hash"]


def test_dedupe_preserves_distinct_rows_with_same_stock() -> None:
    first = mod.normalize_record(_row(in_date="20000101", out_date="20100101", is_new="N"))
    second = mod.normalize_record(_row(in_date="20100102", out_date=None, is_new="Y"))
    duplicate = mod.normalize_record(_row(in_date="20000101", out_date="20100101", is_new="N"))

    deduped = mod.dedupe_records([first, second, duplicate])
    assert len(deduped) == 2


# ---------------------------------------------------------------------------
# collect_all (global vs per-L1 cross-check)
# ---------------------------------------------------------------------------


class FakePro:
    def __init__(
        self,
        *,
        global_rows: dict[str, list[dict[str, Any]]],
        shard_rows: dict[tuple[str, str], list[dict[str, Any]]],
        classify_codes: list[str] | None = None,
    ) -> None:
        self._global = global_rows
        self._shards = shard_rows
        self._classify = classify_codes or []
        self.member_calls: list[dict[str, Any]] = []

    def index_member_all(self, **kwargs: Any) -> list[dict[str, Any]]:
        self.member_calls.append(kwargs)
        l1_code = kwargs.get("l1_code")
        is_new = kwargs["is_new"]
        offset = int(kwargs["offset"])
        limit = int(kwargs["limit"])
        if l1_code:
            rows = self._shards.get((str(l1_code), is_new), [])
        else:
            rows = self._global.get(is_new, [])
        return [dict(row) for row in rows[offset : offset + limit]]

    def index_classify(self, **kwargs: Any) -> list[dict[str, str]]:
        return [{"index_code": code, "level": "L1"} for code in self._classify]


def _collect(pro: Any) -> list[dict[str, Any]]:
    return mod.collect_all(
        pro,
        page_size=1000,
        max_pages=50,
        attempts=2,
        base_delay_s=0.0,
        sleep=lambda _s: None,
        min_interval_s=0.0,
    )


def _matching_shards() -> dict[tuple[str, str], list[dict[str, Any]]]:
    return {
        ("801010.SI", "Y"): [
            _row(l1_code="801010.SI", ts_code="000001.SZ", is_new="Y"),
            _row(l1_code="801010.SI", ts_code="000002.SZ", is_new="Y"),
        ],
        ("801010.SI", "N"): [
            _row(l1_code="801010.SI", ts_code="000003.SZ", out_date="20100101", is_new="N")
        ],
        ("801020.SI", "Y"): [_row(l1_code="801020.SI", ts_code="600000.SH", is_new="Y")],
        ("801020.SI", "N"): [],
    }


def test_collect_all_merges_y_and_n_and_matches_shards() -> None:
    global_rows = {
        "Y": [
            _row(l1_code="801010.SI", ts_code="000001.SZ", is_new="Y"),
            _row(l1_code="801010.SI", ts_code="000002.SZ", is_new="Y"),
            _row(l1_code="801020.SI", ts_code="600000.SH", is_new="Y"),
        ],
        "N": [
            _row(l1_code="801010.SI", ts_code="000003.SZ", out_date="20100101", is_new="N"),
        ],
    }
    pro = FakePro(
        global_rows=global_rows,
        shard_rows=_matching_shards(),
        classify_codes=["801010.SI", "801020.SI"],
    )

    records = _collect(pro)

    assert len(records) == 4
    assert {r["is_new"] for r in records} == {"Y", "N"}
    # Both is_new values were requested globally.
    global_flags = [c["is_new"] for c in pro.member_calls if "l1_code" not in c]
    assert {"Y", "N"}.issubset(set(global_flags))


def test_collect_all_adds_l1_codes_from_classify() -> None:
    # A retired L1 code comes only from index_classify (no global rows).
    global_rows = {
        "Y": [
            _row(l1_code="801010.SI", ts_code="000001.SZ", is_new="Y"),
            _row(l1_code="801010.SI", ts_code="000002.SZ", is_new="Y"),
            _row(l1_code="801020.SI", ts_code="600000.SH", is_new="Y"),
        ],
        "N": [
            _row(l1_code="801010.SI", ts_code="000003.SZ", out_date="20100101", is_new="N"),
        ],
    }
    shards = _matching_shards()
    shards[("801030.SI", "Y")] = []
    shards[("801030.SI", "N")] = []
    pro = FakePro(
        global_rows=global_rows,
        shard_rows=shards,
        classify_codes=["801010.SI", "801020.SI", "801030.SI"],
    )

    _collect(pro)

    shard_l1 = {c["l1_code"] for c in pro.member_calls if "l1_code" in c}
    assert "801030.SI" in shard_l1


def test_collect_all_rejects_set_difference() -> None:
    global_rows = {
        "Y": [
            _row(l1_code="801010.SI", ts_code="000001.SZ", is_new="Y"),
            _row(l1_code="801010.SI", ts_code="000002.SZ", is_new="Y"),
        ],
        "N": [],
    }
    shards = _matching_shards()
    # Drop one shard row -> shard set is a subset of global set.
    shards[("801010.SI", "Y")] = [_row(l1_code="801010.SI", ts_code="000001.SZ", is_new="Y")]
    pro = FakePro(global_rows=global_rows, shard_rows=shards, classify_codes=["801010.SI"])

    with pytest.raises(ValueError, match="differ"):
        _collect(pro)


def test_collect_all_rejects_empty_global_dataset() -> None:
    pro = FakePro(global_rows={"Y": [], "N": []}, shard_rows={}, classify_codes=[])

    with pytest.raises(ValueError, match="empty"):
        _collect(pro)


def test_collect_all_rejects_global_is_new_mismatch() -> None:
    # Global "N" request returns a row flagged "Y" -> must abort, not be silently
    # merged into the wrong bucket.
    class MismatchPro(FakePro):
        def index_member_all(self, **kwargs: Any) -> list[dict[str, Any]]:
            if kwargs.get("l1_code") is None and kwargs["is_new"] == "N":
                if int(kwargs["offset"]) > 0:
                    return []
                return [_row(ts_code="000009.SZ", is_new="Y")]
            return super().index_member_all(**kwargs)

    pro = MismatchPro(
        global_rows={"Y": [_row(ts_code="000001.SZ", is_new="Y")], "N": []},
        shard_rows=_matching_shards(),
        classify_codes=["801010.SI"],
    )

    with pytest.raises(ValueError, match="is_new"):
        _collect(pro)


# ---------------------------------------------------------------------------
# Persistence (sqlite) + atomicity
# ---------------------------------------------------------------------------


def _sqlite_engine(tmp_path: Path) -> Any:
    return create_engine(f"sqlite:///{tmp_path / 'target.db'}")


def _sample_records() -> list[dict[str, Any]]:
    return mod.dedupe_records(
        [
            mod.normalize_record(_row(ts_code="000001.SZ", out_date=None, is_new="Y")),
            mod.normalize_record(_row(ts_code="000001.SZ", out_date="20100101", is_new="N")),
        ]
    )


def test_publish_refuses_empty_dataset(tmp_path: Path) -> None:
    engine = _sqlite_engine(tmp_path)
    with pytest.raises(ValueError, match="empty"):
        mod.publish(engine, [])


def test_publish_creates_table_and_roundtrips(tmp_path: Path) -> None:
    engine = _sqlite_engine(tmp_path)
    records = _sample_records()

    affected = mod.publish(engine, records, batch_size=1)

    assert affected == 2
    table = mod.industry_component_history_tushare
    with engine.begin() as conn:
        stored = [dict(row) for row in conn.execute(select(table)).mappings()]
    assert {r["row_hash"] for r in stored} == {r["row_hash"] for r in records}
    null_out = next(r for r in stored if r["is_new"] == "Y")
    assert null_out["out_date"] is None


def test_publish_rolls_back_on_verification_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = _sqlite_engine(tmp_path)
    original = _sample_records()
    mod.publish(engine, original, batch_size=10)

    table = mod.industry_component_history_tushare
    replacement = [mod.normalize_record(_row(ts_code="600000.SH", is_new="Y"))]

    def boom(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("verification failed")

    monkeypatch.setattr(mod, "_verify_published", boom)
    with pytest.raises(RuntimeError, match="verification failed"):
        mod.publish(engine, replacement, batch_size=10)

    with engine.begin() as conn:
        stored_hashes = {row[0] for row in conn.execute(select(table.c.row_hash))}
    assert stored_hashes == {r["row_hash"] for r in original}


def test_publish_refuses_incompatible_existing_schema(tmp_path: Path) -> None:
    engine = _sqlite_engine(tmp_path)
    incompatible = Table(
        mod.TABLE_NAME,
        MetaData(),
        Column("row_hash", Text, primary_key=True),
        Column("unexpected", Text),
    )
    incompatible.create(engine)
    with engine.begin() as conn:
        conn.execute(insert(incompatible), [{"row_hash": "sentinel", "unexpected": "x"}])

    with pytest.raises(RuntimeError, match="incompatible"):
        mod.publish(engine, _sample_records(), batch_size=10)

    with engine.begin() as conn:
        remaining = list(conn.execute(select(incompatible.c.row_hash)))
    assert [row[0] for row in remaining] == ["sentinel"]


def test_collect_failure_does_not_publish(tmp_path: Path) -> None:
    engine = _sqlite_engine(tmp_path)
    shards = _matching_shards()
    shards[("801010.SI", "Y")] = []
    pro = FakePro(
        global_rows={
            "Y": [
                _row(l1_code="801010.SI", ts_code="000001.SZ", is_new="Y"),
                _row(l1_code="801010.SI", ts_code="000002.SZ", is_new="Y"),
            ],
            "N": [],
        },
        shard_rows=shards,
        classify_codes=["801010.SI"],
    )

    with pytest.raises(ValueError):
        records = _collect(pro)
        mod.publish(engine, records)  # pragma: no cover - collect raises first

    assert not inspect(engine).has_table(mod.TABLE_NAME)


def test_publish_accepts_varchar_text_family(tmp_path: Path) -> None:
    engine = _sqlite_engine(tmp_path)
    compatible = Table(
        mod.TABLE_NAME,
        MetaData(),
        Column("row_hash", String(64), primary_key=True),
        *[Column(name, Text) for name in mod.RAW_FIELDS if name not in mod.DATE_FIELDS],
        Column("in_date", Date),
        Column("out_date", Date),
    )
    compatible.create(engine)

    records = _sample_records()
    mod.publish(engine, records, batch_size=10)

    with engine.begin() as conn:
        stored = {row[0] for row in conn.execute(select(compatible.c.row_hash))}
    assert stored == {r["row_hash"] for r in records}


def test_publish_rejects_wrong_column_type_family(tmp_path: Path) -> None:
    engine = _sqlite_engine(tmp_path)
    incompatible = Table(
        mod.TABLE_NAME,
        MetaData(),
        Column("row_hash", Text, primary_key=True),
        *[Column(name, Text) for name in mod.RAW_FIELDS if name not in mod.DATE_FIELDS],
        Column("in_date", Text),  # Date expected
        Column("out_date", Date),
    )
    incompatible.create(engine)
    with engine.begin() as conn:
        conn.execute(
            insert(incompatible),
            [{"row_hash": "sentinel", "in_date": "20200101"}],
        )

    with pytest.raises(RuntimeError, match="incompatible"):
        mod.publish(engine, _sample_records(), batch_size=10)

    with engine.begin() as conn:
        assert [r[0] for r in conn.execute(select(incompatible.c.row_hash))] == ["sentinel"]


def test_publish_rejects_wrong_primary_key(tmp_path: Path) -> None:
    engine = _sqlite_engine(tmp_path)
    incompatible = Table(
        mod.TABLE_NAME,
        MetaData(),
        Column("id", Text, primary_key=True),
        Column("row_hash", Text),
        *[Column(name, Text) for name in mod.RAW_FIELDS if name not in mod.DATE_FIELDS],
        Column("in_date", Date),
        Column("out_date", Date),
    )
    incompatible.create(engine)

    with pytest.raises(RuntimeError, match="incompatible"):
        mod.publish(engine, _sample_records(), batch_size=10)


def test_publish_failure_leaves_no_half_written_data(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine = _sqlite_engine(tmp_path)
    assert not inspect(engine).has_table(mod.TABLE_NAME)

    records = _sample_records()
    expected_hashes = {r["row_hash"] for r in records}

    def boom(*_args: Any, **_kwargs: Any) -> None:
        raise RuntimeError("verification failed")

    monkeypatch.setattr(mod, "_verify_published", boom)
    with pytest.raises(RuntimeError, match="verification failed"):
        mod.publish(engine, records, batch_size=10)

    # Postgres rolls back the atomic DDL; sqlite may retain an empty table.
    # Either way no half-written row may be visible.
    if inspect(engine).has_table(mod.TABLE_NAME):
        table = mod.industry_component_history_tushare
        with engine.begin() as conn:
            stored = {row[0] for row in conn.execute(select(table.c.row_hash))}
        assert stored == set()
    assert expected_hashes


# ---------------------------------------------------------------------------
# CLI safety
# ---------------------------------------------------------------------------


def test_main_reports_failure_without_leaking_details(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    secret_token = "SUPERSECRET-TOKEN-123"
    detailed_url = "postgresql://user:pw@localhost:5432/dbname"

    class FakeConfig:
        class tushare:  # noqa: N801 - mirrors config attribute casing
            token_private = secret_token

        class database:  # noqa: N801
            url = detailed_url

    monkeypatch.setattr(mod, "load_config", lambda: FakeConfig())

    def boom(*_args: Any, **_kwargs: Any) -> list[dict[str, Any]]:
        raise RuntimeError(f"fetch failed against {detailed_url} token={secret_token}")

    monkeypatch.setattr(mod, "collect_all", boom)

    with caplog.at_level("ERROR"):
        exit_code = mod.main([])

    assert exit_code == 1
    logged = caplog.text
    assert "RuntimeError" in logged
    assert secret_token not in logged
    assert detailed_url not in logged
    assert "Traceback" not in logged
