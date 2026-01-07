from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
XUEQIU_URL = "https://xueqiu.com/S/SH600519"


@dataclass(frozen=True)
class XueqiuTokenFetcher:
    """Fetch xq_a_token from Xueqiu and persist it to a local file."""

    timeout_s: float = 10.0

    def fetch_token(self) -> str:
        """Fetch xq_a_token from Xueqiu."""
        session = requests.Session()
        response = session.get(
            XUEQIU_URL,
            headers=_default_headers(),
            timeout=self.timeout_s,
        )
        response.raise_for_status()
        csrf_response = session.get(
            "https://xueqiu.com/service/csrf",
            headers=_csrf_headers(),
            timeout=self.timeout_s,
        )
        csrf_response.raise_for_status()
        token = session.cookies.get("xq_a_token")
        if not token:
            logger.error("xq_a_token missing in response cookies")
            raise RuntimeError("xq_a_token not found")
        logger.info("xq_a_token fetched", extra={"token_length": len(token)})
        return token

    def fetch_and_store(self) -> Path:
        """Fetch token and write it to a timestamped file in project root."""
        token = self.fetch_token()
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"xq_a_token_{timestamp}.txt"
        path = PROJECT_ROOT / filename
        path.write_text(token, encoding="utf-8")
        logger.info("xq_a_token stored", extra={"path": str(path)})
        return path


def _default_headers() -> dict[str, str]:
    return {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }


def _csrf_headers() -> dict[str, str]:
    headers = _default_headers()
    headers["Accept"] = "application/json, text/plain, */*"
    headers["Referer"] = XUEQIU_URL
    return headers
