#!/usr/bin/env python3
import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request

BASE_URL = os.environ.get("FTSHARE_BASE_URL", "https://market.ft.tech/gateway").rstrip("/")
ENDPOINT = "/api/v2/market/data/stock-minute-seal"
SAFE_URLOPENER = urllib.request.build_opener()
_REQUEST_HEADERS = {"FTSHARE_API_KEY": os.environ["FTSHARE_API_KEY"], "Content-Type": "application/json"} if os.environ.get("FTSHARE_API_KEY") else {}

TRADE_DATE_PATTERN = re.compile(r"^\d{8}$")


def _require_api_key():
    key = os.environ.get("FTSHARE_API_KEY")
    if not key:
        print("FTSHARE_API_KEY environment variable is required", file=sys.stderr)
        raise SystemExit(2)
    return key


def safe_urlopen(request, timeout=30):
    url = request.full_url if isinstance(request, urllib.request.Request) else str(request)
    parsed, base = urllib.parse.urlparse(url), urllib.parse.urlparse(BASE_URL)
    if parsed.scheme != base.scheme or parsed.netloc != base.netloc:
        print(f"Invalid URL for safe_urlopen: {url}", file=sys.stderr)
        raise SystemExit(1)
    if not isinstance(request, urllib.request.Request):
        request = urllib.request.Request(url, method="GET")
    request.add_unredirected_header("FTSHARE_API_KEY", _require_api_key())
    request.add_unredirected_header("Content-Type", "application/json")
    return SAFE_URLOPENER.open(request, timeout=timeout)


def main():
    key = _require_api_key()
    parser = argparse.ArgumentParser(description="查询股票分钟封单金额")
    parser.add_argument("--trade-date", dest="trade_date", required=True,
                        help="交易日期，八位 YYYYMMDD，如 20260923；仅支持单日")
    parser.add_argument("--symbol", help="股票代码，如 600825.SH、000001.SZ，也支持六位裸代码；不传返回该日全部有封单记录的股票")
    args = parser.parse_args()
    if not TRADE_DATE_PATTERN.match(args.trade_date):
        parser.error("trade-date 须为八位 YYYYMMDD，如 20260923（不接受 YYYY-MM-DD）")
    params = {"trade_date": args.trade_date}
    if args.symbol:
        params["symbol"] = args.symbol
    request = urllib.request.Request(BASE_URL + ENDPOINT + "?" + urllib.parse.urlencode(params),
                                     headers={**_REQUEST_HEADERS, "FTSHARE_API_KEY": key, "X-Client-Name": "ft-claw", "Content-Type": "application/json"}, method="GET")
    try:
        with safe_urlopen(request) as response:
            print(json.dumps(json.loads(response.read().decode()), ensure_ascii=False, indent=2))
    except urllib.error.HTTPError as error:
        print(f"HTTP {error.code}: {error.read().decode()}", file=sys.stderr)
        raise SystemExit(1)
    except urllib.error.URLError as error:
        print(f"请求失败: {error.reason}", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
