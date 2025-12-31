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

    def index_basic(
        self, market: str, fields: str, offset: int, limit: int
    ) -> list[dict[str, object]]:
        """Return index_basic rows as a list of dicts."""

    def index_daily(self, ts_code: str, trade_date: str, fields: str) -> list[dict[str, object]]:
        """Return index_daily rows as a list of dicts."""

    def sge_daily(self, ts_code: str, trade_date: str, fields: str) -> list[dict[str, object]]:
        """Return sge_daily rows as a list of dicts."""


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

    def index_daily(self, ts_code: str, trade_date: str, fields: str) -> list[dict[str, object]]:
        """Query index_daily data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.index_daily(ts_code=ts_code, trade_date=trade_date, fields=fields)
        if data is None or data.empty:
            return []
        return cast(list[dict[str, object]], data.to_dict("records"))

    def sge_daily(self, ts_code: str, trade_date: str, fields: str) -> list[dict[str, object]]:
        """Query sge_daily data via Tushare PRO API."""
        if not self.token:
            raise ValueError("Tushare token is required")
        self._rate_limiter.wait()
        pro = ts.pro_api(self.token)
        data = pro.sge_daily(ts_code=ts_code, trade_date=trade_date, fields=fields)
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
