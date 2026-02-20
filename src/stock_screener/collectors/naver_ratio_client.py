from __future__ import annotations

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from threading import Lock
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
    save_parse_miss_html: bool = True
    parse_miss_html_path: str = "artifacts/naver_ratio_parse_miss_sample.html"

    @staticmethod
    def _extract_latest_reserve_ratio_from_html(html: str) -> float | None:
        ratio, _ = NaverRatioCollector._extract_latest_reserve_ratio_with_status(html)
        return ratio

    @staticmethod
    def _extract_latest_reserve_ratio_with_status(html: str) -> tuple[float | None, str]:
        # Prefer row-based parse: distinguish "no data" (blank cells) from parse errors.
        row_match = re.search(
            r"<tr[^>]*>\s*<th[^>]*>\s*(?:자본유보율|유보율)\s*</th>(.*?)</tr>",
            html,
            flags=re.S,
        )
        if row_match:
            row_html = row_match.group(1)
            td_cells = re.findall(r"<td[^>]*>(.*?)</td>", row_html, flags=re.S)
            normalized = [re.sub(r"<[^>]+>", "", cell).strip() for cell in td_cells]
            if normalized and all(not v or v == "-" for v in normalized):
                return None, "no_data"

            row_numbers: list[str] = []
            for value in normalized:
                row_numbers.extend(re.findall(r"-?\d+(?:,\d{3})*(?:\.\d+)?", value))
            values = NaverRatioCollector._parse_valid_numbers(row_numbers)
            if values:
                positives = [v for v in values if v > 0]
                return (positives[0] if positives else values[0]), "success"
            return None, "parse_error"

        # Fallback: marker-near scanning for variants of content format.
        markers = ["자본유보율", "유보율"]
        positions = [html.find(marker) for marker in markers if html.find(marker) >= 0]
        if not positions:
            return None, "marker_missing"

        values: list[float] = []
        for idx in positions:
            snippet = html[max(0, idx - 3000): idx + 3000]
            tag_numbers = re.findall(r">\s*(-?\d+(?:,\d{3})*(?:\.\d+)?)\s*<", snippet)
            values.extend(NaverRatioCollector._parse_valid_numbers(tag_numbers))
            nearby_numbers = re.findall(r"유보율[^0-9-]{0,30}(-?\d+(?:,\d{3})*(?:\.\d+)?)", snippet)
            values.extend(NaverRatioCollector._parse_valid_numbers(nearby_numbers))

        if not values:
            return None, "parse_error"

        positives = [v for v in values if v > 0]
        return (positives[0] if positives else values[0]), "success"

    @staticmethod
    def _parse_valid_numbers(raw_values: list[str]) -> list[float]:
        values: list[float] = []
        for raw in raw_values:
            try:
                v = float(raw.replace(",", ""))
            except ValueError:
                continue
            if -1000.0 <= v <= 100000.0:
                values.append(v)
        return values

    @staticmethod
    def _is_blocked_response(html: str) -> bool:
        blocked_markers = ["비정상적인 접근", "접근이 제한", "Access Denied", "자동화된 요청"]
        return any(marker in html for marker in blocked_markers)

    @staticmethod
    def _preview_html(html: str, max_chars: int = 120) -> str:
        compact = re.sub(r"\s+", " ", html)
        return compact[:max_chars]

    @staticmethod
    def _decode_response(raw: bytes, content_charset: str | None = None) -> str:
        encodings: list[str] = []
        if content_charset:
            encodings.append(content_charset)
        encodings.extend(["utf-8", "euc-kr", "cp949"])

        for encoding in encodings:
            try:
                return raw.decode(encoding)
            except UnicodeDecodeError:
                continue

        return raw.decode("utf-8", errors="ignore")

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
                    content_charset = resp.headers.get_content_charset()

                html = self._decode_response(raw, content_charset)
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

    def _save_html_sample(self, ticker: str, html: str) -> None:
        out_path = Path(self.parse_miss_html_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(f"<!-- ticker={ticker} -->\n{html}", encoding="utf-8")

    def latest_reserve_ratio(self, tickers: list[str]) -> pd.DataFrame:
        total = len(tickers)
        logger.info("Starting Naver reserve-ratio crawl: tickers=%s", total)

        rows: dict[str, float] = {}
        failed_fetch = 0
        no_data = 0
        parse_error = 0
        done = 0
        parse_issue_examples = 0
        started_at = time.perf_counter()
        html_sample_saved = False
        html_sample_lock = Lock()

        def _collect_one(ticker: str) -> tuple[str, float | None, str, str | None]:
            nonlocal html_sample_saved
            html = self._fetch_html(ticker)
            if not html:
                return ticker, None, "fetch_fail", None

            ratio, status = self._extract_latest_reserve_ratio_with_status(html)
            if ratio is None:
                if status in {"parse_error", "marker_missing"} and self.save_parse_miss_html:
                    with html_sample_lock:
                        if not html_sample_saved:
                            self._save_html_sample(ticker, html)
                            html_sample_saved = True
                return ticker, None, status, self._preview_html(html)

            return ticker, ratio, "success", None

        worker_count = max(1, min(self.max_workers, total))
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [executor.submit(_collect_one, ticker) for ticker in tickers]
            for future in as_completed(futures):
                ticker, ratio, status, preview = future.result()
                done += 1

                if status == "fetch_fail":
                    failed_fetch += 1
                elif status == "no_data":
                    no_data += 1
                elif status in {"parse_error", "marker_missing"}:
                    parse_error += 1
                    if parse_issue_examples < 5:
                        parse_issue_examples += 1
                        logger.warning(
                            "Reserve-ratio parse issue sample: ticker=%s, status=%s, html_preview=%s",
                            ticker,
                            status,
                            preview,
                        )
                        if self.save_parse_miss_html and html_sample_saved:
                            logger.warning("Saved parse-miss HTML sample to %s", self.parse_miss_html_path)
                elif ratio is not None:
                    rows[ticker] = ratio

                if done % 50 == 0 or done == total:
                    elapsed = time.perf_counter() - started_at
                    per_item = elapsed / done if done else 0.0
                    remaining = max(total - done, 0)
                    eta_minutes = (remaining * per_item) / 60
                    logger.info(
                        "Reserve-ratio crawl progress: %s/%s (success=%s, fetch_fail=%s, no_data=%s, parse_error=%s, elapsed=%.1fs, eta=%.1fmin)",
                        done,
                        total,
                        len(rows),
                        failed_fetch,
                        no_data,
                        parse_error,
                        elapsed,
                        eta_minutes,
                    )

        logger.info(
            "Reserve-ratio crawl completed: total=%s, success=%s, fetch_fail=%s, no_data=%s, parse_error=%s",
            total,
            len(rows),
            failed_fetch,
            no_data,
            parse_error,
        )

        if not rows:
            return pd.DataFrame(columns=["ticker", "reserve_ratio"])
        ordered_rows = [{"ticker": ticker, "reserve_ratio": rows[ticker]} for ticker in tickers if ticker in rows]
        return pd.DataFrame(ordered_rows)
