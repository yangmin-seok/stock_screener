from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import logging
from pathlib import Path

import pandas as pd

from stock_screener.collectors.pykrx_client import PykrxCollector
from stock_screener.features.metrics import build_snapshot
from stock_screener.storage.db import init_db
from stock_screener.storage.repository import Repository


logger = logging.getLogger(__name__)


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

    @staticmethod
    def _fundamental_backfill_dates(asof: date) -> list[date]:
        dates = {asof}
        for years in range(1, 6):
            dates.add(asof - timedelta(days=365 * years))
        quarter_ends = pd.period_range(end=pd.Timestamp(asof), periods=24, freq="Q")
        for q in quarter_ends:
            dates.add(q.end_time.date())
        return sorted(dates)

    def run(self, asof_date: str | None = None, lookback_days: int = 400) -> BatchResult:
        dt = pd.to_datetime(asof_date).date() if asof_date else self.collector.recent_business_day()
        asof_str = dt.strftime("%Y-%m-%d")
        logger.info("Starting daily batch: asof=%s, lookback_days=%s", asof_str, lookback_days)

        tickers = self.collector.tickers()
        ticker_count = self.repo.upsert_tickers(tickers)
        logger.info("Tickers upserted: %s", ticker_count)

        from_dt = dt - timedelta(days=lookback_days * 2)
        logger.info("Price/cap fetch window: %s ~ %s", from_dt, dt)

        price_rows = 0
        for idx, ticker in enumerate(tickers["ticker"], start=1):
            ohlcv = self.collector.ohlcv(from_dt, dt, ticker)
            price_rows += self.repo.upsert_prices(ohlcv)
            if idx % 200 == 0 or idx == ticker_count:
                logger.info("Price progress: %s/%s tickers, rows=%s", idx, ticker_count, price_rows)

        # Root fix: trade value time-series is sourced from cap_daily (KRX 공식 거래대금)
        cap_rows = 0
        trading_dates = self.collector.trading_dates(from_dt, dt)
        for idx, trading_dt in enumerate(trading_dates, start=1):
            cap_rows += self.repo.upsert_cap(self.collector.market_cap(trading_dt))
            if idx % 30 == 0 or idx == len(trading_dates):
                logger.info("Cap progress: %s/%s dates, rows=%s", idx, len(trading_dates), cap_rows)

        fund_rows = 0
        fund_dates = self._fundamental_backfill_dates(dt)
        for idx, fdt in enumerate(fund_dates, start=1):
            fund_rows += self.repo.upsert_fundamental(self.collector.fundamental(fdt))
            if idx % 10 == 0 or idx == len(fund_dates):
                logger.info("Fundamental progress: %s/%s dates, rows=%s", idx, len(fund_dates), fund_rows)

        price_window = self.repo.get_price_window(asof_str, window=lookback_days)
        daily = self.repo.get_daily_join(asof_str)
        fund_hist = self.repo.get_fundamental_window(asof_str, years=6)
        snapshot = build_snapshot(price_window, daily, fund_hist, asof_str)
        snap_rows = self.repo.replace_snapshot(asof_str, snapshot)

        logger.info(
            "Daily batch completed: asof=%s, tickers=%s, prices=%s, cap=%s, fundamental=%s, snapshot=%s",
            asof_str,
            ticker_count,
            price_rows,
            cap_rows,
            fund_rows,
            snap_rows,
        )

        return BatchResult(
            asof_date=asof_str,
            tickers=ticker_count,
            prices=price_rows,
            cap=cap_rows,
            fundamental=fund_rows,
            snapshot=snap_rows,
        )

    def rebuild_snapshot_only(self, asof_date: str | None = None, lookback_days: int = 400) -> BatchResult:
        dt = pd.to_datetime(asof_date).date() if asof_date else self.collector.recent_business_day()
        asof_str = dt.strftime("%Y-%m-%d")
        logger.info("Starting snapshot-only rebuild: asof=%s, lookback_days=%s", asof_str, lookback_days)

        ticker_count = self.repo.upsert_tickers(self.collector.tickers())

        price_window = self.repo.get_price_window(asof_str, window=lookback_days)
        daily = self.repo.get_daily_join(asof_str)
        fund_hist = self.repo.get_fundamental_window(asof_str, years=6)
        snapshot = build_snapshot(price_window, daily, fund_hist, asof_str)
        snap_rows = self.repo.replace_snapshot(asof_str, snapshot)

        logger.info(
            "Snapshot-only rebuild completed: asof=%s, tickers=%s, snapshot=%s",
            asof_str,
            ticker_count,
            snap_rows,
        )

        return BatchResult(
            asof_date=asof_str,
            tickers=ticker_count,
            prices=0,
            cap=0,
            fundamental=0,
            snapshot=snap_rows,
        )
