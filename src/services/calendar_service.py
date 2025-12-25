from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from config.settings import AppConfig
from core.calendar.service import TradingCalendarService
from infra.calendar_store.store import CalendarStore
from infra.calendar_store.tushare_sync import TushareCalendarSyncer
from infra.db.engine import create_engine_from_config


@dataclass(frozen=True)
class CalendarService:
    calendar: TradingCalendarService

    def sync(self, start: date, end: date, exchange: str | None = None) -> int:
        return int(self.calendar.sync_range(start, end, exchange))


def build_calendar_service(config: AppConfig) -> CalendarService:
    engine = create_engine_from_config(config.database)
    store = CalendarStore(engine=engine)
    syncer = TushareCalendarSyncer(
        token=config.tushare.token,
        exchange=config.tushare.exchange,
    )
    calendar = TradingCalendarService(store=store, syncer=syncer, exchange=config.tushare.exchange)
    return CalendarService(calendar=calendar)
