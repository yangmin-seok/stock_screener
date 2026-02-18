from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import pandas as pd
from pykrx import stock


logger = logging.getLogger(__name__)


@dataclass
class PykrxCollector:
    retries: int = 3
    sleep_seconds: float = 0.5

    @staticmethod
    def fmt(dt: date | datetime) -> str:
        return dt.strftime("%Y%m%d")

    @staticmethod
    def _pick_column(frame: pd.DataFrame, candidates: list[str]) -> str | None:
        for name in candidates:
            if name in frame.columns:
                return name
        return None

    @classmethod
    def _normalize_ohlcv(cls, frame: pd.DataFrame) -> pd.DataFrame:
        colmap: dict[str, list[str]] = {
            "open": ["시가", "Open", "open"],
            "high": ["고가", "High", "high"],
            "low": ["저가", "Low", "low"],
            "close": ["종가", "Close", "close"],
            "volume": ["거래량", "Volume", "volume"],
            "value": ["거래대금", "거래대금(원)", "거래대금(백만원)", "Value", "value"],
        }

        out = pd.DataFrame(index=frame.index)
        missing_required: list[str] = []

        for target in ("open", "high", "low", "close", "volume"):
            src = cls._pick_column(frame, colmap[target])
            if src is None:
                missing_required.append(target)
            else:
                out[target] = pd.to_numeric(frame[src], errors="coerce")

        if missing_required:
            raise KeyError(f"Missing required OHLCV columns: {missing_required}. available={list(frame.columns)}")

        value_src = cls._pick_column(frame, colmap["value"])
        out["value"] = pd.to_numeric(frame[value_src], errors="coerce") if value_src is not None else pd.NA
        return out

    def _retry(self, fn, *args, **kwargs):
        last_error = None
        for idx in range(self.retries):
            try:
                return fn(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                logger.warning(
                    "pykrx call failed (attempt %s/%s): fn=%s, error=%s",
                    idx + 1,
                    self.retries,
                    getattr(fn, "__name__", repr(fn)),
                    exc,
                )
                if idx + 1 < self.retries:
                    time.sleep(self.sleep_seconds * (2**idx))
        raise RuntimeError(f"pykrx call failed after retries: {last_error}") from last_error

    def recent_business_day(self) -> date:
        candidate = datetime.now().date()
        for _ in range(10):
            frame = self._retry(stock.get_market_ohlcv_by_date, self.fmt(candidate), self.fmt(candidate), "005930")
            if not frame.empty:
                return candidate
            candidate -= timedelta(days=1)
        raise RuntimeError("Could not determine business day")

    def trading_dates(self, from_dt: date, to_dt: date) -> list[date]:
        frame = self._retry(stock.get_market_ohlcv_by_date, self.fmt(from_dt), self.fmt(to_dt), "005930")
        if frame.empty:
            logger.warning("No trading dates found between %s and %s", from_dt, to_dt)
            return []
        trading_days = [ts.date() for ts in pd.to_datetime(frame.index).sort_values()]
        logger.info("Resolved %s trading dates between %s and %s", len(trading_days), from_dt, to_dt)
        return trading_days

    def tickers(self) -> pd.DataFrame:
        rows: list[dict] = []
        for market in ("KOSPI", "KOSDAQ"):
            for ticker in self._retry(stock.get_market_ticker_list, market=market):
                name = self._retry(stock.get_market_ticker_name, ticker)
                rows.append({"ticker": ticker, "name": name, "market": market, "active_flag": 1})
        logger.info("Collected %s tickers from KOSPI/KOSDAQ", len(rows))
        return pd.DataFrame(rows)

    def ohlcv(self, from_dt: date, to_dt: date, ticker: str) -> pd.DataFrame:
        frame = self._retry(stock.get_market_ohlcv_by_date, self.fmt(from_dt), self.fmt(to_dt), ticker)
        if frame.empty:
            logger.debug("No OHLCV rows for ticker=%s in %s~%s", ticker, from_dt, to_dt)
            return pd.DataFrame()

        frame.index = pd.to_datetime(frame.index)
        norm = self._normalize_ohlcv(frame)
        norm["date"] = norm.index.strftime("%Y-%m-%d")
        norm["ticker"] = ticker
        out = norm.reset_index(drop=True)
        logger.debug("Collected OHLCV rows=%s for ticker=%s", len(out), ticker)
        return out

    def market_cap(self, dt: date) -> pd.DataFrame:
        frame = self._retry(stock.get_market_cap, self.fmt(dt))
        if frame.empty:
            logger.warning("No market cap data for date=%s", dt)
            return pd.DataFrame()
        frame = frame.rename(
            columns={
                "시가총액": "mcap",
                "상장주식수": "shares",
                "거래량": "volume",
                "거래대금": "value",
            }
        )
        for col in ("mcap", "shares", "volume", "value"):
            if col not in frame.columns:
                frame[col] = pd.NA
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
        frame.index.name = "ticker"
        frame = frame.reset_index()[["ticker", "mcap", "shares", "volume", "value"]]
        frame["date"] = dt.strftime("%Y-%m-%d")
        logger.debug("Collected market cap rows=%s for date=%s", len(frame), dt)
        return frame

    def fundamental(self, dt: date) -> pd.DataFrame:
        query_dt = dt
        frame = self._retry(stock.get_market_fundamental, self.fmt(query_dt), market="ALL")
        if frame.empty:
            # Quarter/month end can fall on non-trading day; fallback to nearest prior business day.
            for offset in range(1, 8):
                candidate = dt - timedelta(days=offset)
                frame = self._retry(stock.get_market_fundamental, self.fmt(candidate), market="ALL")
                if not frame.empty:
                    query_dt = candidate
                    logger.debug("Fundamental fallback date used: target=%s -> actual=%s", dt, candidate)
                    break
        if frame.empty:
            logger.warning("No fundamental data for date=%s", dt)
            return pd.DataFrame()
        frame = frame.rename(
            columns={
                "PER": "per",
                "PBR": "pbr",
                "EPS": "eps",
                "BPS": "bps",
                "DIV": "div",
                "DPS": "dps",
            }
        )
        for col in ("per", "pbr", "eps", "bps", "div", "dps"):
            if col not in frame.columns:
                frame[col] = pd.NA
        frame.index.name = "ticker"
        frame = frame.reset_index()[["ticker", "per", "pbr", "eps", "bps", "div", "dps"]]
        frame["date"] = query_dt.strftime("%Y-%m-%d")
        logger.debug("Collected fundamental rows=%s for date=%s", len(frame), query_dt)
        return frame
