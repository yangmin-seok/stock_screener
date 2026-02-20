from __future__ import annotations

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd


logger = logging.getLogger(__name__)


@dataclass
class NaverRatioCollector:
    retries: int = 3
    sleep_seconds: float = 0.5
    timeout_seconds: int = 8
    max_workers: int = 8

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

        positives = [v for v in values if v > 0]
        if positives:
            return positives[0]
        return values[0]

    @staticmethod
    def _is_blocked_response(html: str) -> bool:
        blocked_markers = [
            "비정상적인 접근",
            "접근이 제한",
            "Access Denied",
            "자동화된 요청",
        ]
        return any(marker in html for marker in blocked_markers)

    @staticmethod
    def _preview_html(html: str, max_chars: int = 120) -> str:
        compact = re.sub(r"\s+", " ", html)
        return compact[:max_chars]

    def _fetch_html(self, ticker: str) -> str | None:
        query = urlencode({"cmp_cd": ticker, "fin_typ": 0, "freq_typ": "Y"})
        url = f"https://navercomp.wisereport.co.kr/v2/company/cF1001.aspx?{query}"
        req = Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0",
                "Referer": f"https://finance.naver.com/item/main.naver?code={ticker}",
                "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            },
        )

        last_error: Exception | None = None
        for idx in range(self.retries):
            try:
                with urlopen(req, timeout=self.timeout_seconds) as resp:
                    raw = resp.read()
                html = raw.decode("utf-8", errors="ignore")
                if self._is_blocked_response(html):
                    last_error = RuntimeError("blocked-response")
                    if idx + 1 < self.retries:
                        time.sleep(self.sleep_seconds * (2**idx))
                    continue
                return html
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
        parse_miss_examples = 0
        started_at = time.perf_counter()

        def _collect_one(ticker: str) -> tuple[str, float | None, str | None, str | None]:
            html = self._fetch_html(ticker)
            if not html:
                return ticker, None, "fetch_fail", None

            ratio = self._extract_latest_reserve_ratio_from_html(html)
            if ratio is None:
                return ticker, None, "parse_miss", self._preview_html(html)

            return ticker, ratio, None, None

        worker_count = max(1, min(self.max_workers, total))
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [executor.submit(_collect_one, ticker) for ticker in tickers]
            for future in as_completed(futures):
                ticker, ratio, error, preview = future.result()
                done += 1
                if error == "fetch_fail":
                    failed_fetch += 1
                elif error == "parse_miss":
                    missing_ratio += 1
                    if parse_miss_examples < 5:
                        parse_miss_examples += 1
                        logger.warning(
                            "Reserve-ratio parse miss sample: ticker=%s, html_preview=%s",
                            ticker,
                            preview,
                        )
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
