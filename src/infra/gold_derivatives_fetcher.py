from __future__ import annotations

import csv
import io
import json
import logging
import zipfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import requests
from sqlalchemy import func, select
from sqlalchemy.engine import Engine

from config.settings import GoldDataConfig
from core.pipeline.types import RawBatch
from infra.db.tables import gold_cftc_report

logger = logging.getLogger(__name__)


class GoldDerivativesFetcher:
    """Fetch gold derivatives data for CFTC reports and futures curve."""

    def __init__(self, engine: Engine, config: GoldDataConfig) -> None:
        self.engine = engine
        self.config = config

    def ensure_cftc_reports(self, as_of_date: date) -> RawBatch:
        """Return CFTC report rows if updates are needed; otherwise empty."""
        latest_report_date: date | None
        total_rows: int
        with self.engine.begin() as connection:
            latest_report_date = connection.execute(
                select(func.max(gold_cftc_report.c.report_date))
            ).scalar_one_or_none()
            total_rows = (
                connection.execute(select(func.count(gold_cftc_report.c.id))).scalar_one()
                or 0
            )

        need_update = False
        years_to_fetch: set[int] = set()

        if latest_report_date is None:
            need_update = True
            years_to_fetch.update({as_of_date.year, as_of_date.year - 1})
        else:
            if (as_of_date - latest_report_date).days > 7:
                need_update = True
                years_to_fetch.add(as_of_date.year)

        if total_rows < 90:
            need_update = True
            years_to_fetch.update({as_of_date.year, as_of_date.year - 1})

        if not need_update:
            return []

        records: dict[tuple[date, str | None, str | None], dict[str, Any]] = {}
        for year in sorted(years_to_fetch):
            try:
                zip_path = self._download_cftc_zip_if_needed(year)
            except requests.HTTPError as exc:
                response = exc.response
                if response is not None and response.status_code == 404:
                    fallback_year = year - 1
                    logger.warning(
                        "CFTC history zip not found for %s; trying %s instead",
                        year,
                        fallback_year,
                    )
                    try:
                        zip_path = self._download_cftc_zip_if_needed(fallback_year)
                    except requests.HTTPError as fallback_exc:
                        fallback_response = fallback_exc.response
                        if fallback_response is not None and fallback_response.status_code == 404:
                            logger.warning(
                                "CFTC history zip not found for fallback %s; skipping",
                                fallback_year,
                            )
                            continue
                        raise
                else:
                    raise
            for item in self._parse_cftc_zip(zip_path):
                key = (
                    item["report_date"],
                    item.get("contract_market_code"),
                    item.get("market_code"),
                )
                records[key] = item

        return list(records.values())

    def update_barchart_future_curve(self) -> RawBatch:
        """Return futures curve rows from Barchart."""
        data = self._fetch_barchart_raw()
        if not data:
            return []

        now = datetime.now(UTC).replace(tzinfo=None)
        records: list[dict[str, Any]] = []
        for item in data:
            raw = item.get("raw") or {}
            symbol = raw.get("symbol") or item.get("symbol")
            trade_ts = raw.get("tradeTime")
            if not symbol or not trade_ts:
                continue

            trade_date = datetime.utcfromtimestamp(trade_ts).date()
            records.append(
                {
                    "trade_date": trade_date,
                    "symbol": symbol,
                    "contract_symbol": raw.get("contractSymbol"),
                    "last_price": self._num(raw.get("lastPrice")),
                    "price_change": self._num(raw.get("priceChange")),
                    "open_price": self._num(raw.get("openPrice")),
                    "high_price": self._num(raw.get("highPrice")),
                    "low_price": self._num(raw.get("lowPrice")),
                    "previous_price": self._num(raw.get("previousPrice")),
                    "volume": self._to_int(raw.get("volume")),
                    "open_interest": self._to_int(raw.get("openInterest")),
                    "trade_time_str": item.get("tradeTime"),
                    "product_code": item.get("symbolCode"),
                    "symbol_code": item.get("symbolCode"),
                    "symbol_type": str(item.get("symbolType")) if item.get("symbolType") else None,
                    "has_options": "Yes" if item.get("hasOptions") else "No",
                    "created_at": now,
                    "updated_at": now,
                }
            )
        return records

    def _num(self, value: object) -> float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        raw = str(value).replace(",", "").strip()
        if raw in ("", "-", "N/A"):
            return None
        try:
            return float(raw)
        except ValueError:
            return None

    def _to_int(self, value: object) -> int | None:
        if value is None:
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        raw = str(value).replace(",", "").strip()
        if not raw or raw in ("N/A", "-"):
            return None
        try:
            return int(float(raw))
        except ValueError:
            return None

    def _download_cftc_zip_if_needed(self, year: int) -> Path:
        tmp_dir = Path(self.config.tmp_dir)
        tmp_dir.mkdir(parents=True, exist_ok=True)

        zip_filename = f"com_disagg_txt_{year}.zip"
        zip_path = tmp_dir / zip_filename
        flag_path = tmp_dir / f"{zip_filename}.flag.json"
        today = date.today().isoformat()

        if flag_path.exists() and zip_path.exists():
            try:
                info = json.loads(flag_path.read_text(encoding="utf-8"))
                if info.get("date") == today:
                    return zip_path
            except json.JSONDecodeError:
                pass

        url = self.config.cftc_history_url_template.format(year=year)
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        zip_path.write_bytes(response.content)
        flag_path.write_text(
            json.dumps(
                {
                    "filename": zip_filename,
                    "date": today,
                    "downloaded_at": datetime.utcnow().isoformat(),
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        return zip_path

    def _parse_cftc_zip(self, zip_path: Path) -> list[dict[str, Any]]:
        with zipfile.ZipFile(zip_path, "r") as archive:
            txt_name = next((name for name in archive.namelist() if name.lower().endswith(".txt")), None)
            if not txt_name:
                raise RuntimeError(f"No txt in {zip_path}")
            text = archive.read(txt_name).decode("latin-1")

        now = datetime.now(UTC).replace(tzinfo=None)
        reader = csv.DictReader(io.StringIO(text))
        records: list[dict[str, Any]] = []

        for row in reader:
            if row.get("Market_and_Exchange_Names") != "GOLD - COMMODITY EXCHANGE INC.":
                continue
            if row.get("FutOnly_or_Combined") != "Combined":
                continue

            as_of_raw = row.get("As_of_Date_In_Form_YYMMDD")
            report_raw = row.get("Report_Date_as_YYYY-MM-DD")
            if not as_of_raw or not report_raw:
                continue

            try:
                as_of_dt = datetime.strptime(as_of_raw, "%y%m%d").date()
                report_dt = datetime.strptime(report_raw, "%Y-%m-%d").date()
            except ValueError:
                continue

            record: dict[str, Any] = {
                "market_name": row.get("Market_and_Exchange_Names"),
                "as_of_date": as_of_dt,
                "report_date": report_dt,
                "contract_market_code": row.get("CFTC_Contract_Market_Code"),
                "market_code": row.get("CFTC_Market_Code"),
                "region_code": row.get("CFTC_Region_Code"),
                "commodity_code": row.get("CFTC_Commodity_Code"),
                "futonly_or_combined": "Combined",
                "open_interest_all": self._int_field(row.get("Open_Interest_All")),
                "prod_merc_long_all": self._int_field(row.get("Prod_Merc_Positions_Long_All")),
                "prod_merc_short_all": self._int_field(row.get("Prod_Merc_Positions_Short_All")),
                "swap_long_all": self._int_field(row.get("Swap_Positions_Long_All")),
                "swap_short_all": self._int_field(row.get("Swap__Positions_Short_All")),
                "swap_spread_all": self._int_field(row.get("Swap__Positions_Spread_All")),
                "m_money_long_all": self._int_field(row.get("M_Money_Positions_Long_All")),
                "m_money_short_all": self._int_field(row.get("M_Money_Positions_Short_All")),
                "m_money_spread_all": self._int_field(row.get("M_Money_Positions_Spread_All")),
                "other_rept_long_all": self._int_field(row.get("Other_Rept_Positions_Long_All")),
                "other_rept_short_all": self._int_field(row.get("Other_Rept_Positions_Short_All")),
                "other_rept_spread_all": self._int_field(row.get("Other_Rept_Positions_Spread_All")),
                "tot_rept_long_all": self._int_field(row.get("Tot_Rept_Positions_Long_All")),
                "tot_rept_short_all": self._int_field(row.get("Tot_Rept_Positions_Short_All")),
                "nonrept_long_all": self._int_field(row.get("NonRept_Positions_Long_All")),
                "nonrept_short_all": self._int_field(row.get("NonRept_Positions_Short_All")),
                "created_at": now,
                "updated_at": now,
            }

            mm_long = record.get("m_money_long_all") or 0
            mm_short = record.get("m_money_short_all") or 0
            other_long = record.get("other_rept_long_all") or 0
            other_short = record.get("other_rept_short_all") or 0
            record["noncomm_net_all"] = (mm_long + other_long) - (mm_short + other_short)

            records.append(record)

        return records

    def _int_field(self, value: object) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _fetch_barchart_raw(self) -> list[dict[str, Any]]:
        html_url = "https://www.barchart.com/futures/quotes/GC*0/futures-prices"
        api_url = self.config.barchart_quotes_url

        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,"
                "image/webp,image/apng,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
            }
        )

        response = session.get(html_url, timeout=15)
        response.raise_for_status()

        xsrf = session.cookies.get("XSRF-TOKEN")
        if xsrf is None:
            raise RuntimeError(
                "Unable to fetch XSRF-TOKEN from Barchart cookies.",
            )

        xsrf_header = requests.utils.unquote(xsrf)

        headers = {
            "User-Agent": session.headers["User-Agent"],
            "Accept": "application/json",
            "x-xsrf-token": xsrf_header,
            "Referer": html_url,
        }

        params = {
            "fields": (
                "symbol,contractSymbol,lastPrice,priceChange,openPrice,highPrice,lowPrice,"
                "previousPrice,volume,openInterest,tradeTime,symbolCode,symbolType,hasOptions"
            ),
            "lists": "futures.contractInRoot",
            "root": "GC",
            "meta": "field.shortName,field.type,field.description,lists.lastUpdate",
            "hasOptions": "true",
            "page": "1",
            "limit": "100",
            "raw": "1",
        }

        response = session.get(api_url, headers=headers, params=params, timeout=15)
        response.raise_for_status()

        payload = response.json()
        data = payload.get("data")
        if not isinstance(data, list):
            logger.warning("Unexpected Barchart payload: %s", payload)
            return []
        return data
