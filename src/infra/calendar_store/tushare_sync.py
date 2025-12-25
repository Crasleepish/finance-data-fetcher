from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

import tushare as ts


@dataclass(frozen=True)
class TushareCalendarSyncer:
    token: str
    exchange: str = "SSE"

    def fetch_trade_days(self, start: date, end: date, exchange: str) -> list[date]:
        if not self.token:
            raise ValueError("Tushare token is required")

        pro = ts.pro_api(self.token)
        data = pro.trade_cal(
            exchange=exchange,
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
            fields="cal_date,is_open",
        )

        if data is None or data.empty:
            return []

        open_days = data[data["is_open"] == 1]
        return [
            datetime.strptime(value, "%Y%m%d").date() for value in open_days["cal_date"].tolist()
        ]
