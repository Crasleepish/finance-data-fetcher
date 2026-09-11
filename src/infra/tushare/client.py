from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Protocol, cast

import tushare as ts


class TushareClient(Protocol):
    """Tushare client interface used by fetchers."""

    def stock_basic(
        self,
        exchange: str,
        list_status: str,
        fields: str,
        offset: int,
        limit: int,
    ) -> list[dict[str, object]]:
        """Return stock_basic rows as a list of dicts."""

    def daily(self, trade_date: str, fields: str) -> list[dict[str, object]]:
        """Return daily rows as a list of dicts."""

    def daily_basic(self, trade_date: str, fields: str) -> list[dict[str, object]]:
        """Return daily_basic rows as a list of dicts."""

    def suspend_d(self, trade_date: str, suspend_type: str, fields: str) -> list[dict[str, object]]:
        """Return suspend_d rows as a list of dicts."""

    def income_vip(self, period: str, fields: str) -> list[dict[str, object]]:
        """Return income_vip rows as a list of dicts."""

    def balancesheet_vip(self, period: str, fields: str) -> list[dict[str, object]]:
        """Return balancesheet_vip rows as a list of dicts."""

    def cashflow_vip(self, period: str, fields: str) -> list[dict[str, object]]:
        """Return cashflow_vip rows as a list of dicts."""

    def income(
        self, ts_code: str, start_date: str, end_date: str, fields: str
    ) -> list[dict[str, object]]:
        """Return income rows as a list of dicts."""

    def balancesheet(
        self, ts_code: str, start_date: str, end_date: str, fields: str
    ) -> list[dict[str, object]]:
        """Return balancesheet rows as a list of dicts."""

    def cashflow(
        self, ts_code: str, start_date: str, end_date: str, fields: str
    ) -> list[dict[str, object]]:
        """Return cashflow rows as a list of dicts."""

    def adj_factor(self, trade_date: str, fields: str) -> list[dict[str, object]]:
        """Return adj_factor rows as a list of dicts."""

    def moneyflow_hsgt(
        self, start_date: str, end_date: str, fields: str
    ) -> list[dict[str, object]]:
        """Return moneyflow_hsgt rows as a list of dicts."""

    def margin(self, start_date: str, end_date: str, fields: str) -> list[dict[str, object]]:
        """Return margin rows as a list of dicts."""

    def daily_info(
        self,
        start_date: str,
        end_date: str,
        fields: str,
        exchange: str | None = None,
    ) -> list[dict[str, object]]:
        """Return daily_info rows as a list of dicts."""

    def sz_daily_info(self, start_date: str, end_date: str) -> list[dict[str, object]]:
        """Return sz_daily_info rows as a list of dicts."""

    def index_basic(
        self, market: str, fields: str, offset: int, limit: int
    ) -> list[dict[str, object]]:
        """Return index_basic rows as a list of dicts."""

    def index_daily(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
        fields: str,
        offset: int,
        limit: int,
    ) -> list[dict[str, object]]:
        """Return index_daily rows as a list of dicts."""

    def index_dailybasic(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
        fields: str,
    ) -> list[dict[str, object]]:
        """Return index_dailybasic rows as a list of dicts."""

    def index_global(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
        fields: str,
        offset: int,
        limit: int,
    ) -> list[dict[str, object]]:
        """Return index_global rows as a list of dicts."""

    def sge_daily(
        self, ts_code: str, start_date: str, end_date: str, fields: str
    ) -> list[dict[str, object]]:
        """Return sge_daily rows as a list of dicts."""

    def fund_basic(
        self,
        market: str,
        status: str,
        fields: str,
        offset: int,
        limit: int,
    ) -> list[dict[str, object]]:
        """Return fund_basic rows as a list of dicts."""

    def fund_nav(self, ts_code: str, nav_date: str, fields: str) -> list[dict[str, object]]:
        """Return fund_nav rows as a list of dicts."""

    def fund_daily(
        self, trade_date: str, fields: str, offset: int, limit: int
    ) -> list[dict[str, object]]:
        """Return fund_daily rows as a list of dicts."""

    def rt_k(self, ts_code: str, fields: str, offset: int, limit: int) -> list[dict[str, object]]:
        """Return rt_k rows as a list of dicts."""

    def rt_idx_k(self, ts_code: str, fields: str) -> list[dict[str, object]]:
        """Return rt_idx_k rows as a list of dicts."""


@dataclass(frozen=True)
class TushareProClient(TushareClient):
    """Tushare PRO client wrapper."""

    token: str
    min_interval_s: float = 1.0
    _rate_limiter: _RateLimiter = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_rate_limiter", _RateLimiter(self.min_interval_s))

    def stock_basic(
        self,
        exchange: str,
        list_status: str,
        fields: str,
        offset: int,
        limit: int,
    ) -> list[dict[str, object]]:
        """Query stock_basic via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.stock_basic(
            exchange=exchange,
            list_status=list_status,
            fields=fields,
            offset=offset,
            limit=limit,
        )
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))

    def daily(self, trade_date: str, fields: str) -> list[dict[str, object]]:
        """Query daily data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.daily(trade_date=trade_date, fields=fields)
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))

    def daily_basic(self, trade_date: str, fields: str) -> list[dict[str, object]]:
        """Query daily_basic data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.daily_basic(trade_date=trade_date, fields=fields)
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))

    def suspend_d(self, trade_date: str, suspend_type: str, fields: str) -> list[dict[str, object]]:
        """Query suspend_d data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.suspend_d(trade_date=trade_date, suspend_type=suspend_type, fields=fields)
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))

    def income_vip(self, period: str, fields: str) -> list[dict[str, object]]:
        """Query income_vip data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.income_vip(period=period, fields=fields)
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))

    def balancesheet_vip(self, period: str, fields: str) -> list[dict[str, object]]:
        """Query balancesheet_vip data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.balancesheet_vip(period=period, fields=fields)
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))

    def cashflow_vip(self, period: str, fields: str) -> list[dict[str, object]]:
        """Query cashflow_vip data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.cashflow_vip(period=period, fields=fields)
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))

    def income(
        self, ts_code: str, start_date: str, end_date: str, fields: str
    ) -> list[dict[str, object]]:
        """Query income data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.income(ts_code=ts_code, start_date=start_date, end_date=end_date, fields=fields)
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))

    def balancesheet(
        self, ts_code: str, start_date: str, end_date: str, fields: str
    ) -> list[dict[str, object]]:
        """Query balancesheet data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.balancesheet(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
        )
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))

    def cashflow(
        self, ts_code: str, start_date: str, end_date: str, fields: str
    ) -> list[dict[str, object]]:
        """Query cashflow data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.cashflow(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
        )
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))

    def adj_factor(self, trade_date: str, fields: str) -> list[dict[str, object]]:
        """Query adj_factor data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.adj_factor(trade_date=trade_date, fields=fields)
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))

    def moneyflow_hsgt(
        self, start_date: str, end_date: str, fields: str
    ) -> list[dict[str, object]]:
        """Query moneyflow_hsgt data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.moneyflow_hsgt(start_date=start_date, end_date=end_date, fields=fields)
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))

    def margin(self, start_date: str, end_date: str, fields: str) -> list[dict[str, object]]:
        """Query margin data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.margin(start_date=start_date, end_date=end_date, fields=fields)
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))

    def daily_info(
        self,
        start_date: str,
        end_date: str,
        fields: str,
        exchange: str | None = None,
    ) -> list[dict[str, object]]:
        """Query daily_info (市场交易统计) data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        params: dict[str, str] = {
            "start_date": start_date,
            "end_date": end_date,
            "fields": fields,
        }
        if exchange:
            params["exchange"] = exchange
        data = pro.daily_info(**params)
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))

    def sz_daily_info(self, start_date: str, end_date: str) -> list[dict[str, object]]:
        """Query sz_daily_info (深圳市场每日交易概况) data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.sz_daily_info(start_date=start_date, end_date=end_date)
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))

    def index_basic(
        self, market: str, fields: str, offset: int, limit: int
    ) -> list[dict[str, object]]:
        """Query index_basic data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.index_basic(market=market, fields=fields, offset=offset, limit=limit)
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))

    def index_daily(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
        fields: str,
        offset: int,
        limit: int,
    ) -> list[dict[str, object]]:
        """Query index_daily data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.index_daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
            offset=offset,
            limit=limit,
        )
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))

    def index_dailybasic(
        self, ts_code: str, start_date: str, end_date: str, fields: str
    ) -> list[dict[str, object]]:
        """Query index_dailybasic data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.index_dailybasic(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
        )
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))

    def index_global(
        self,
        ts_code: str,
        start_date: str,
        end_date: str,
        fields: str,
        offset: int,
        limit: int,
    ) -> list[dict[str, object]]:
        """Query index_global data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.index_global(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
            offset=offset,
            limit=limit,
        )
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))

    def sge_daily(
        self, ts_code: str, start_date: str, end_date: str, fields: str
    ) -> list[dict[str, object]]:
        """Query sge_daily data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.sge_daily(
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            fields=fields,
        )
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))

    def fund_basic(
        self,
        market: str,
        status: str,
        fields: str,
        offset: int,
        limit: int,
    ) -> list[dict[str, object]]:
        """Query fund_basic data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.fund_basic(
            market=market,
            status=status,
            fields=fields,
            offset=offset,
            limit=limit,
        )
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))

    def fund_nav(self, ts_code: str, nav_date: str, fields: str) -> list[dict[str, object]]:
        """Query fund_nav data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.fund_nav(ts_code=ts_code, nav_date=nav_date, fields=fields)
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))

    def fund_daily(
        self, trade_date: str, fields: str, offset: int, limit: int
    ) -> list[dict[str, object]]:
        """Query fund_daily data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.fund_daily(
            trade_date=trade_date,
            fields=fields,
            offset=offset,
            limit=limit,
        )
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))

    def rt_k(self, ts_code: str, fields: str, offset: int, limit: int) -> list[dict[str, object]]:
        """Query rt_k data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.rt_k(ts_code=ts_code, fields=fields, offset=offset, limit=limit)
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))

    def rt_idx_k(self, ts_code: str, fields: str) -> list[dict[str, object]]:
        """Query rt_idx_k data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.rt_idx_k(ts_code=ts_code, fields=fields)
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))


@dataclass
class _RateLimiter:
    min_interval_s: float
    _last_call: float = field(default=0.0, init=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_call
            if elapsed < self.min_interval_s:
                time.sleep(self.min_interval_s - elapsed)
            self._last_call = time.monotonic()
