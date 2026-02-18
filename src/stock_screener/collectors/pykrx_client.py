from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import pandas as pd
from pykrx import stock


@dataclass
class PykrxCollector:
    retries: int = 3
    sleep_seconds: float = 0.5

    @staticmethod
    def fmt(dt: date | datetime) -> str:
        return dt.strftime("%Y%m%d")

    def _retry(self, fn, *args, **kwargs):
        last_error = None
        for idx in range(self.retries):
            try:
                return fn(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
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

    def tickers(self) -> pd.DataFrame:
        rows: list[dict] = []
        for market in ("KOSPI", "KOSDAQ"):
            for ticker in self._retry(stock.get_market_ticker_list, market=market):
                name = self._retry(stock.get_market_ticker_name, ticker)
                rows.append({"ticker": ticker, "name": name, "market": market, "active_flag": 1})
        return pd.DataFrame(rows)

    def ohlcv(self, from_dt: date, to_dt: date, ticker: str) -> pd.DataFrame:
        frame = self._retry(stock.get_market_ohlcv_by_date, self.fmt(from_dt), self.fmt(to_dt), ticker)
        if frame.empty:
            return pd.DataFrame()
        frame = frame.rename(
            columns={
                "시가": "open",
                "고가": "high",
                "저가": "low",
                "종가": "close",
                "거래량": "volume",
                "거래대금": "value",
            }
        )
        frame.index = pd.to_datetime(frame.index)
        frame = frame[["open", "high", "low", "close", "volume", "value"]]
        frame["date"] = frame.index.strftime("%Y-%m-%d")
        frame["ticker"] = ticker
        return frame.reset_index(drop=True)

    def market_cap(self, dt: date) -> pd.DataFrame:
        frame = self._retry(stock.get_market_cap, self.fmt(dt))
        if frame.empty:
            return pd.DataFrame()
        frame = frame.rename(
            columns={
                "시가총액": "mcap",
                "상장주식수": "shares",
                "거래량": "volume",
                "거래대금": "value",
            }
        )
        frame.index.name = "ticker"
        frame = frame.reset_index()[["ticker", "mcap", "shares", "volume", "value"]]
        frame["date"] = dt.strftime("%Y-%m-%d")
        return frame

    def fundamental(self, dt: date) -> pd.DataFrame:
        frame = self._retry(stock.get_market_fundamental, self.fmt(dt), market="ALL")
        if frame.empty:
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
        frame.index.name = "ticker"
        frame = frame.reset_index()[["ticker", "per", "pbr", "eps", "bps", "div", "dps"]]
        frame["date"] = dt.strftime("%Y-%m-%d")
        return frame
