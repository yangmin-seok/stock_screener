from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import pandas as pd

from stock_screener.collectors.pykrx_client import PykrxCollector
from stock_screener.features.metrics import build_snapshot
from stock_screener.storage.db import init_db
from stock_screener.storage.repository import Repository


@dataclass
class BatchResult:
    asof_date: str
    tickers: int
    prices: int
    cap: int
    fundamental: int
    snapshot: int


class DailyBatchPipeline:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)
        init_db(self.db_path)
        self.repo = Repository(self.db_path)
        self.collector = PykrxCollector()

    def run(self, asof_date: str | None = None, lookback_days: int = 400) -> BatchResult:
        dt = pd.to_datetime(asof_date).date() if asof_date else self.collector.recent_business_day()
        asof_str = dt.strftime("%Y-%m-%d")

        tickers = self.collector.tickers()
        ticker_count = self.repo.upsert_tickers(tickers)

        from_dt = dt - timedelta(days=lookback_days * 2)
        price_rows = 0
        for ticker in tickers["ticker"]:
            ohlcv = self.collector.ohlcv(from_dt, dt, ticker)
            price_rows += self.repo.upsert_prices(ohlcv)

        cap_rows = self.repo.upsert_cap(self.collector.market_cap(dt))
        fund_rows = self.repo.upsert_fundamental(self.collector.fundamental(dt))

        price_window = self.repo.get_price_window(asof_str, window=lookback_days)
        daily = self.repo.get_daily_join(asof_str)
        snapshot = build_snapshot(price_window, daily, asof_str)
        snap_rows = self.repo.replace_snapshot(asof_str, snapshot)

        return BatchResult(
            asof_date=asof_str,
            tickers=ticker_count,
            prices=price_rows,
            cap=cap_rows,
            fundamental=fund_rows,
            snapshot=snap_rows,
        )
