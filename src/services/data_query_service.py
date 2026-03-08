from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any

from sqlalchemy import Engine, Select, String, Table, and_, func, or_, select
from sqlalchemy import cast as sql_cast
from sqlalchemy.sql.elements import ColumnElement

from core.data_query.mapping import default_currency_unit
from core.data_query.validation import (
    normalize_limit,
    normalize_list_order,
    normalize_page,
    normalize_page_size,
    normalize_results_order,
    parse_date_range,
    validate_asset_code,
    validate_data_type,
)
from infra.db.engine import transaction
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
from models.data_query import (
    AssetItem,
    DataListPayload,
    DataListResponse,
    DataPoint,
    DataResultsPayload,
    DataResultsResponse,
    ListMeta,
    ResultsMeta,
)


@dataclass(frozen=True)
class DataQueryService:
    """Service for querying historical data and asset lists."""

    engine: Engine

    def get_results(
        self,
        *,
        data_type: str,
        asset_code: str,
        start_date: str,
        end_date: str,
        limit: int | None,
        order: str | None,
    ) -> DataResultsResponse:
        normalized_type = validate_data_type(data_type)
        normalized_code = validate_asset_code(asset_code)
        start, end = parse_date_range(start_date, end_date)
        resolved_limit = normalize_limit(limit)
        resolved_order = normalize_results_order(order)
        currency, unit = default_currency_unit(normalized_type)

        points = self._load_points(
            data_type=normalized_type,
            asset_code=normalized_code,
            start=start,
            end=end,
            limit=resolved_limit,
            order=resolved_order,
        )

        payload = DataResultsPayload(
            data_type=normalized_type,
            asset_code=normalized_code,
            start_date=start,
            end_date=end,
            currency=currency,
            unit=unit,
            points=points,
        )
        return DataResultsResponse(
            success=True,
            data=payload,
            error=None,
            meta=ResultsMeta(count=len(points)),
        )

    def list_assets(
        self,
        *,
        data_type: str,
        keyword: str | None,
        page: int | None,
        page_size: int | None,
        order: str | None,
    ) -> DataListResponse:
        normalized_type = validate_data_type(data_type)
        resolved_page = normalize_page(page)
        resolved_page_size = normalize_page_size(page_size)
        resolved_order = normalize_list_order(order)

        items, total = self._load_assets(
            data_type=normalized_type,
            keyword=keyword,
            page=resolved_page,
            page_size=resolved_page_size,
            order=resolved_order,
        )

        payload = DataListPayload(data_type=normalized_type, items=items)
        meta = ListMeta(page=resolved_page, page_size=resolved_page_size, total=total)
        return DataListResponse(success=True, data=payload, error=None, meta=meta)

    def _load_points(
        self,
        *,
        data_type: str,
        asset_code: str,
        start: date,
        end: date,
        limit: int,
        order: str,
    ) -> list[DataPoint]:
        table, code_column, date_column = _hist_table_for(data_type)
        if data_type == "fund":
            net_value = table.c.net_value.label("open")
            stmt = (
                select(
                    date_column.label("date"),
                    net_value,
                    net_value.label("high"),
                    net_value.label("low"),
                    net_value.label("close"),
                    sql_cast(None, table.c.net_value.type).label("volume"),
                )
                .where(
                    and_(
                        code_column == asset_code,
                        date_column >= start,
                        date_column <= end,
                    )
                )
                .limit(limit)
            )
        else:
            stmt = (
                select(
                    date_column.label("date"),
                    table.c.open.label("open"),
                    table.c.high.label("high"),
                    table.c.low.label("low"),
                    table.c.close.label("close"),
                    table.c.volume.label("volume"),
                )
                .where(
                    and_(
                        code_column == asset_code,
                        date_column >= start,
                        date_column <= end,
                    )
                )
                .limit(limit)
            )

        if order == "desc":
            stmt = stmt.order_by(date_column.desc())
        else:
            stmt = stmt.order_by(date_column.asc())

        with transaction(self.engine) as connection:
            rows = connection.execute(stmt).fetchall()

        return [
            DataPoint(
                date=row.date,
                open=row.open,
                high=row.high,
                low=row.low,
                close=row.close,
                volume=row.volume,
            )
            for row in rows
        ]

    def _load_assets(
        self,
        *,
        data_type: str,
        keyword: str | None,
        page: int,
        page_size: int,
        order: str,
    ) -> tuple[list[AssetItem], int]:
        table, code_column, name_column, market_column = _info_table_for(data_type)
        filters = []
        if keyword:
            pattern = f"%{keyword}%"
            filters.append(
                or_(
                    sql_cast(code_column, String).ilike(pattern),
                    sql_cast(name_column, String).ilike(pattern),
                )
            )

        stmt = select(
            code_column.label("code"),
            name_column.label("name"),
            market_column.label("market"),
        )
        count_stmt = select(func.count()).select_from(table)
        if filters:
            stmt = stmt.where(and_(*filters))
            count_stmt = count_stmt.where(and_(*filters))

        stmt = _apply_list_order(stmt, code_column, name_column, order)
        stmt = stmt.offset((page - 1) * page_size).limit(page_size)

        with transaction(self.engine) as connection:
            total = int(connection.execute(count_stmt).scalar_one())
            rows = connection.execute(stmt).fetchall()

        currency = default_currency_unit(data_type)[0]
        items = [
            AssetItem(
                code=row.code,
                name=row.name,
                market=row.market,
                currency=currency,
            )
            for row in rows
        ]
        return items, total


def _hist_table_for(
    data_type: str,
) -> tuple[
    Table,
    ColumnElement[Any],
    ColumnElement[Any],
]:
    if data_type == "stock":
        return stock_hist_unadj, stock_hist_unadj.c.stock_code, stock_hist_unadj.c.date
    if data_type == "index":
        return index_hist, index_hist.c.index_code, index_hist.c.date
    if data_type == "etf":
        return etf_hist, etf_hist.c.etf_code, etf_hist.c.date
    if data_type == "fund":
        return fund_hist, fund_hist.c.fund_code, fund_hist.c.date
    raise ValueError("unsupported data_type")


def _info_table_for(
    data_type: str,
) -> tuple[
    Table,
    ColumnElement[Any],
    ColumnElement[Any],
    ColumnElement[Any],
]:
    if data_type == "stock":
        return stock_info, stock_info.c.stock_code, stock_info.c.stock_name, stock_info.c.market
    if data_type == "index":
        return index_info, index_info.c.index_code, index_info.c.index_name, index_info.c.market
    if data_type == "etf":
        return etf_info, etf_info.c.etf_code, etf_info.c.etf_name, sql_cast(None, String)
    if data_type == "fund":
        return fund_info, fund_info.c.fund_code, fund_info.c.fund_name, fund_info.c.market
    raise ValueError("unsupported data_type")


def _apply_list_order(
    stmt: Select,
    code_column: ColumnElement[Any],
    name_column: ColumnElement[Any],
    order: str,
) -> Select:
    if order == "code_desc":
        return stmt.order_by(code_column.desc())
    if order == "name_asc":
        return stmt.order_by(name_column.asc())
    if order == "name_desc":
        return stmt.order_by(name_column.desc())
    return stmt.order_by(code_column.asc())
