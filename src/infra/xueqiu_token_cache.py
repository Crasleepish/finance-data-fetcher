from __future__ import annotations

from datetime import datetime

from infra.xueqiu_token_fetcher import PROJECT_ROOT, XueqiuTokenFetcher

TOKEN_PREFIX = "xq_a_token_"
TOKEN_SUFFIX = ".txt"
TOKEN_REFRESH_DAYS = 7
TOKEN_ERROR_REFRESH_SECONDS = 60


def load_latest_token() -> tuple[str, datetime] | None:
    """Load the newest cached xq_a_token from project root."""
    latest: tuple[str, datetime] | None = None
    for path in PROJECT_ROOT.glob(f"{TOKEN_PREFIX}*{TOKEN_SUFFIX}"):
        timestamp = parse_token_timestamp(path.name)
        if timestamp is None:
            continue
        if latest is None or timestamp > latest[1]:
            token = path.read_text(encoding="utf-8").strip()
            if token:
                latest = (token, timestamp)
    return latest


def refresh_token(fetcher: XueqiuTokenFetcher) -> tuple[str, datetime]:
    """Fetch and persist a new xq_a_token, returning token and timestamp."""
    path = fetcher.fetch_and_store()
    token = path.read_text(encoding="utf-8").strip()
    timestamp = parse_token_timestamp(path.name)
    if not token or timestamp is None:
        raise RuntimeError("failed to refresh xq_a_token")
    return token, timestamp


def parse_token_timestamp(name: str) -> datetime | None:
    """Parse token timestamp from filename."""
    if not name.startswith(TOKEN_PREFIX) or not name.endswith(TOKEN_SUFFIX):
        return None
    stem = name[len(TOKEN_PREFIX) : -len(TOKEN_SUFFIX)]
    try:
        return datetime.strptime(stem, "%Y%m%d%H%M%S")
    except ValueError:
        return None


def token_age_days(token_time: datetime) -> int:
    """Return token age in days."""
    return (datetime.now() - token_time).days


def token_age_seconds(token_time: datetime) -> int:
    """Return token age in seconds."""
    return int((datetime.now() - token_time).total_seconds())
