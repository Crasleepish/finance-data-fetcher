from __future__ import annotations

from datetime import date

import pytest

from core.clean.stock_code_change_cleaner import StockCodeChangeCleaner


def _row(
    trade_code: str,
    code: str,
    name: str,
    start: str,
    end: str | None,
) -> dict[str, object]:
    return {
        "trade_code": trade_code,
        "code": code,
        "name": name,
        "start_date": start,
        "end_date": end,
    }


def test_maps_same_stock_renumber_with_sz_suffix() -> None:
    cleaner = StockCodeChangeCleaner()
    records = cleaner.clean(
        [
            _row("001914.SZ", "000043.SZ", "招商积余", "19910101", "20151231"),
            _row("001914.SZ", "001914.SZ", "招商积余", "20160101", None),
        ]
    )
    by_key = {
        (record["trade_code"], record["code"], record["start_date"]): record for record in records
    }
    assert by_key[("001914.SZ", "000043.SZ", date(1991, 1, 1))]["end_date"] == date(2015, 12, 31)
    open_interval = by_key[("001914.SZ", "001914.SZ", date(2016, 1, 1))]
    assert open_interval["name"] == "招商积余"
    assert open_interval["end_date"] is None


def test_maps_bse_83_to_920_with_bj_suffix() -> None:
    cleaner = StockCodeChangeCleaner()
    records = cleaner.clean(
        [
            _row("920001.BJ", "830001.BJ", "某北交所公司", "20200101", "20230930"),
            _row("920001.BJ", "920001.BJ", "某北交所公司", "20231001", None),
        ]
    )
    codes = {record["code"] for record in records}
    assert codes == {"830001.BJ", "920001.BJ"}


def test_bare_code_receives_trade_code_market_suffix() -> None:
    cleaner = StockCodeChangeCleaner()
    records = cleaner.clean(
        [
            _row("600000.SH", "000001", "浦发银行", "20000101", "20001231"),
            _row("600000.SH", "600000.SH", "浦发银行", "20010101", None),
        ]
    )
    assert any(record["code"] == "000001.SH" for record in records)


def test_same_code_repeated_intervals_kept_distinct() -> None:
    cleaner = StockCodeChangeCleaner()
    records = cleaner.clean(
        [
            _row("600000.SH", "000001.SH", "浦发银行", "20000101", "20001231"),
            _row("600000.SH", "600000.SH", "浦发银行", "20010101", "20021231"),
            _row("600000.SH", "000001.SH", "浦发银行", "20030101", "20031231"),
            _row("600000.SH", "600000.SH", "浦发银行", "20040101", None),
        ]
    )
    keys = {(record["trade_code"], record["code"], record["start_date"]) for record in records}
    assert ("600000.SH", "000001.SH", date(2000, 1, 1)) in keys
    assert ("600000.SH", "000001.SH", date(2003, 1, 1)) in keys
    assert len(records) == 4


def test_conflicting_name_within_chain_aborts() -> None:
    cleaner = StockCodeChangeCleaner()
    with pytest.raises(ValueError, match="does not match latest name"):
        cleaner.clean(
            [
                _row("600000.SH", "000001.SH", "旧名称", "20000101", "20001231"),
                _row("600000.SH", "600000.SH", "浦发银行", "20010101", None),
            ]
        )


def test_identity_code_reused_as_other_chain_history_aborts() -> None:
    cleaner = StockCodeChangeCleaner()
    with pytest.raises(ValueError, match="ambiguous historical code"):
        cleaner.clean(
            [
                _row("600001.SH", "600001.SH", "甲", "20000101", None),
                _row("600002.SH", "600001.SH", "乙", "20000101", "20001231"),
                _row("600002.SH", "600002.SH", "乙", "20010101", None),
            ]
        )


def test_identical_duplicates_deduped() -> None:
    cleaner = StockCodeChangeCleaner()
    row = _row("600000.SH", "600000.SH", "浦发银行", "20000101", None)
    records = cleaner.clean([row, dict(row)])
    assert len(records) == 1


def test_conflicting_duplicate_aborts() -> None:
    cleaner = StockCodeChangeCleaner()
    with pytest.raises(ValueError, match="conflicting duplicate"):
        cleaner.clean(
            [
                _row("600000.SH", "600000.SH", "浦发银行", "20000101", None),
                _row("600000.SH", "600000.SH", "名称变更", "20000101", None),
            ]
        )


def test_overlapping_intervals_abort() -> None:
    cleaner = StockCodeChangeCleaner()
    with pytest.raises(ValueError, match="overlapping"):
        cleaner.clean(
            [
                _row("600000.SH", "000001.SH", "A", "20000101", "20001231"),
                _row("600000.SH", "600000.SH", "B", "20001201", None),
            ]
        )


def test_gap_between_intervals_aborts() -> None:
    cleaner = StockCodeChangeCleaner()
    with pytest.raises(ValueError, match="gap"):
        cleaner.clean(
            [
                _row("600000.SH", "000001.SH", "A", "20000101", "20001231"),
                _row("600000.SH", "600000.SH", "B", "20010102", None),
            ]
        )


def test_missing_open_interval_aborts() -> None:
    cleaner = StockCodeChangeCleaner()
    with pytest.raises(ValueError, match="exactly one open interval"):
        cleaner.clean([_row("600000.SH", "600000.SH", "A", "20000101", "20001231")])


def test_open_interval_code_must_equal_trade_code() -> None:
    cleaner = StockCodeChangeCleaner()
    with pytest.raises(ValueError, match="open interval code must equal"):
        cleaner.clean(
            [
                _row("600000.SH", "000001.SH", "A", "20000101", "20001231"),
                _row("600000.SH", "000001.SH", "B", "20010101", None),
            ]
        )


def test_mapping_ambiguity_aborts() -> None:
    cleaner = StockCodeChangeCleaner()
    with pytest.raises(ValueError, match="ambiguous historical code"):
        cleaner.clean(
            [
                _row("600001.SH", "000001.SH", "A", "20000101", "20001231"),
                _row("600001.SH", "600001.SH", "A", "20010101", None),
                _row("600002.SH", "000001.SH", "B", "19900101", "20001231"),
                _row("600002.SH", "600002.SH", "B", "20010101", None),
            ]
        )


def test_suffixed_code_suffix_must_match_market() -> None:
    cleaner = StockCodeChangeCleaner()
    with pytest.raises(ValueError, match="does not match latest market"):
        cleaner.clean(
            [
                _row("600000.SH", "000001.SZ", "A", "20000101", "20001231"),
                _row("600000.SH", "600000.SH", "B", "20010101", None),
            ]
        )


def test_end_before_start_aborts() -> None:
    cleaner = StockCodeChangeCleaner()
    with pytest.raises(ValueError, match="end_date before start_date"):
        cleaner.clean([_row("600000.SH", "600000.SH", "A", "20200101", "20190101")])


def test_empty_snapshot_aborts() -> None:
    cleaner = StockCodeChangeCleaner()
    with pytest.raises(ValueError, match="empty stock code change snapshot"):
        cleaner.clean([])


def test_missing_required_fields_abort() -> None:
    cleaner = StockCodeChangeCleaner()
    with pytest.raises(ValueError, match="missing required field: name"):
        cleaner.clean(
            [
                {
                    "trade_code": "600000.SH",
                    "code": "600000.SH",
                    "start_date": "20000101",
                    "end_date": None,
                }
            ]
        )
