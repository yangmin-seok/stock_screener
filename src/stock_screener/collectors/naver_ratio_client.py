from __future__ import annotations

import logging
import re
import time
from dataclasses import dataclass
from urllib.error import URLError
from urllib.request import Request, urlopen

import pandas as pd


logger = logging.getLogger(__name__)


@dataclass
class NaverRatioCollector:
    retries: int = 3
    sleep_seconds: float = 0.5
    timeout_seconds: int = 8

    @staticmethod
    def _extract_latest_reserve_ratio_from_html(html: str) -> float | None:
        marker = "유보율"
        idx = html.find(marker)
        if idx < 0:
            return None

        snippet = html[max(0, idx - 3000): idx + 3000]
        candidates = re.findall(r">\s*(-?\d+(?:,\d{3})*(?:\.\d+)?)\s*<", snippet)
        values: list[float] = []
        for raw in candidates:
            try:
                v = float(raw.replace(",", ""))
            except ValueError:
                continue
            if -1000.0 <= v <= 100000.0:
                values.append(v)

        if not values:
            return None

        # Reserve ratio is typically a non-trivial positive percent figure.
        positives = [v for v in values if v > 0]
        if positives:
            return positives[0]
        return values[0]

    def _fetch_html(self, ticker: str) -> str | None:
        url = f"https://finance.naver.com/item/main.naver?code={ticker}"
        req = Request(url, headers={"User-Agent": "Mozilla/5.0"})

        last_error: Exception | None = None
        for idx in range(self.retries):
            try:
                with urlopen(req, timeout=self.timeout_seconds) as resp:
                    raw = resp.read()
                return raw.decode("euc-kr", errors="ignore")
            except (URLError, TimeoutError, OSError) as exc:
                last_error = exc
                if idx + 1 < self.retries:
                    time.sleep(self.sleep_seconds * (2**idx))

        logger.warning("Failed to fetch Naver ratio page for ticker=%s: %s", ticker, last_error)
        return None

    def latest_reserve_ratio(self, tickers: list[str]) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        for ticker in tickers:
            html = self._fetch_html(ticker)
            if not html:
                continue
            ratio = self._extract_latest_reserve_ratio_from_html(html)
            if ratio is None:
                continue
            rows.append({"ticker": ticker, "reserve_ratio": ratio})

        if not rows:
            return pd.DataFrame(columns=["ticker", "reserve_ratio"])
        return pd.DataFrame(rows)
