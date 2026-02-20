from __future__ import annotations

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    max_workers: int = 16

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
        total = len(tickers)
        logger.info("Starting Naver reserve-ratio crawl: tickers=%s", total)

        rows: dict[str, float] = {}
        failed_fetch = 0
        missing_ratio = 0
        done = 0
        started_at = time.perf_counter()

        def _collect_one(ticker: str) -> tuple[str, float | None, str | None]:
            html = self._fetch_html(ticker)
            if not html:
                return ticker, None, "fetch_fail"

            ratio = self._extract_latest_reserve_ratio_from_html(html)
            if ratio is None:
                return ticker, None, "parse_miss"

            return ticker, ratio, None

        worker_count = max(1, min(self.max_workers, total))
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [executor.submit(_collect_one, ticker) for ticker in tickers]
            for future in as_completed(futures):
                ticker, ratio, error = future.result()
                done += 1
                if error == "fetch_fail":
                    failed_fetch += 1
                elif error == "parse_miss":
                    missing_ratio += 1
                elif ratio is not None:
                    rows[ticker] = ratio

                if done % 50 == 0 or done == total:
                    elapsed = time.perf_counter() - started_at
                    per_item = elapsed / done if done else 0.0
                    remaining = max(total - done, 0)
                    eta_minutes = (remaining * per_item) / 60
                    logger.info(
                        "Reserve-ratio crawl progress: %s/%s (success=%s, fetch_fail=%s, parse_miss=%s, elapsed=%.1fs, eta=%.1fmin)",
                        done,
                        total,
                        len(rows),
                        failed_fetch,
                        missing_ratio,
                        elapsed,
                        eta_minutes,
                    )

        logger.info(
            "Reserve-ratio crawl completed: total=%s, success=%s, fetch_fail=%s, parse_miss=%s",
            total,
            len(rows),
            failed_fetch,
            missing_ratio,
        )

        if not rows:
            return pd.DataFrame(columns=["ticker", "reserve_ratio"])
        ordered_rows = [{"ticker": ticker, "reserve_ratio": rows[ticker]} for ticker in tickers if ticker in rows]
        return pd.DataFrame(ordered_rows)
