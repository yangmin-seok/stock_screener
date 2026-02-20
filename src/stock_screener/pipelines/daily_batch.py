from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import logging
from pathlib import Path

import pandas as pd

from stock_screener.collectors.pykrx_client import PykrxCollector
from stock_screener.collectors.naver_ratio_client import NaverRatioCollector
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
        self.ratio_collector = NaverRatioCollector()

    def update_reserve_ratio_only(self, asof_date: str | None = None) -> tuple[str, int]:
        if asof_date:
            dt = pd.to_datetime(asof_date).date()
            asof_str = dt.strftime("%Y-%m-%d")
        else:
            latest_price_date = self.repo.get_latest_price_date()
            latest_snapshot_date = self.repo.get_latest_snapshot_date()
            asof_str = latest_price_date or latest_snapshot_date
            if not asof_str:
                asof_str = self.collector.recent_business_day().strftime("%Y-%m-%d")

        tickers = self.repo.get_active_tickers()
        if not tickers:
            tickers_frame = self.collector.tickers()
            self.repo.upsert_tickers(tickers_frame)
            tickers = tickers_frame["ticker"].tolist()

        logger.info("Starting reserve ratio update: asof=%s, tickers=%s", asof_str, len(tickers))
        ratio_frame = self.ratio_collector.latest_reserve_ratio(tickers)
        rows = self.repo.upsert_reserve_ratio(asof_str, ratio_frame)
        logger.info("Reserve ratio update completed: asof=%s, rows=%s", asof_str, rows)
        return asof_str, rows

    @staticmethod
    def _fundamental_backfill_dates(asof: date, trading_dates: list[date]) -> list[date]:
        if not trading_dates:
            return [asof]

        ts = pd.Series(pd.to_datetime(sorted(trading_dates)))
        dates: set[date] = set()

        # Use last trading day of each month/quarter to improve EPS history coverage.
        month_last = ts.groupby(ts.dt.to_period("M")).max()
        quarter_last = ts.groupby(ts.dt.to_period("Q")).max()
        dates.update(dt.date() for dt in month_last.tolist())
        dates.update(dt.date() for dt in quarter_last.tolist())

        asof_ts = pd.Timestamp(asof)
        dates.add(ts.iloc[-1].date())
        for years in range(1, 6):
            target = asof_ts - pd.DateOffset(years=years)
            candidates = ts[ts <= target]
            if not candidates.empty:
                dates.add(candidates.iloc[-1].date())

        return sorted(dates)

    def run(self, asof_date: str | None = None, lookback_days: int = 400) -> BatchResult:
        dt = pd.to_datetime(asof_date).date() if asof_date else self.collector.recent_business_day()
        asof_str = dt.strftime("%Y-%m-%d")
        logger.info("Starting daily batch: asof=%s, lookback_days=%s", asof_str, lookback_days)

        tickers = self.collector.tickers()
        ticker_count = self.repo.upsert_tickers(tickers)
        logger.info("Tickers upserted: %s", ticker_count)

        price_from_dt = dt - timedelta(days=lookback_days * 2)
        logger.info("Price/cap fetch window: %s ~ %s", price_from_dt, dt)

        price_rows = 0
        for idx, ticker in enumerate(tickers["ticker"], start=1):
            ohlcv = self.collector.ohlcv(price_from_dt, dt, ticker)
            price_rows += self.repo.upsert_prices(ohlcv)
            if idx % 200 == 0 or idx == ticker_count:
                logger.info("Price progress: %s/%s tickers, rows=%s", idx, ticker_count, price_rows)

        # Root fix: trade value time-series is sourced from cap_daily (KRX 공식 거래대금)
        cap_rows = 0
        trading_dates = self.collector.trading_dates(price_from_dt, dt)
        for idx, trading_dt in enumerate(trading_dates, start=1):
            cap_rows += self.repo.upsert_cap(self.collector.market_cap(trading_dt))
            if idx % 30 == 0 or idx == len(trading_dates):
                logger.info("Cap progress: %s/%s dates, rows=%s", idx, len(trading_dates), cap_rows)

        fund_rows = 0
        fundamental_from_dt = dt - timedelta(days=365 * 6)
        fundamental_trading_dates = self.collector.trading_dates(fundamental_from_dt, dt)
        fund_dates = self._fundamental_backfill_dates(dt, fundamental_trading_dates)
        logger.info(
            "Fundamental fetch anchors: %s dates (window=%s~%s)",
            len(fund_dates),
            fundamental_from_dt,
            dt,
        )
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
        latest_price_date = self.repo.get_latest_price_date()
        latest_snapshot_date = self.repo.get_latest_snapshot_date()

        if asof_date:
            dt = pd.to_datetime(asof_date).date()
            asof_str = dt.strftime("%Y-%m-%d")
        else:
            asof_str = latest_price_date or latest_snapshot_date
            if not asof_str:
                raise ValueError("DB cache is empty. Run full collection first.")

        logger.info("Starting snapshot-only rebuild: asof=%s, lookback_days=%s", asof_str, lookback_days)

        ticker_count = self.repo.count_active_tickers()
        if ticker_count == 0:
            raise ValueError("ticker_master is empty. Run full collection first.")

        price_window = self.repo.get_price_window(asof_str, window=lookback_days)
        daily = self.repo.get_daily_join(asof_str)
        fund_hist = self.repo.get_fundamental_window(asof_str, years=6)
        if price_window.empty or daily.empty:
            raise ValueError(
                f"No cached rows for asof={asof_str}. Run full collection for this date or choose an existing asof date."
            )

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
