from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
import logging
from pathlib import Path
import time

import pandas as pd

from stock_screener.collectors.fundamental_provider import FundamentalProvider, merge_financial_records
from stock_screener.collectors.naver_ratio_client import NaverRatioCollector
from stock_screener.collectors.pykrx_client import PykrxCollector, PykrxFinancialFallbackProvider
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
        self.financial_providers: list[FundamentalProvider] = [
            PykrxFinancialFallbackProvider(self.collector),
        ]

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

        month_last = ts.groupby(ts.dt.to_period("M")).max()
        quarter_last = ts.groupby(ts.dt.to_period("Q")).max()
        dates.update(dt.date() for dt in month_last.tolist())
        dates.update(dt.date() for dt in quarter_last.tolist())

        asof_ts = pd.Timestamp(asof)
        dates.add(ts.iloc[-1].date())
        for years in range(1, 11):
            target = asof_ts - pd.DateOffset(years=years)
            candidates = ts[ts <= target]
            if not candidates.empty:
                dates.add(candidates.iloc[-1].date())

        return sorted(dates)

    def _collect_financials(self, dt: date) -> pd.DataFrame:
        provider_frames = [provider.fetch_financials(dt) for provider in self.financial_providers]
        merged = merge_financial_records(provider_frames)
        if merged.empty:
            logger.warning("No financial provider rows for date=%s", dt)
        return merged

    def _safe_collect(self, fn, *args, label: str, max_attempts: int = 4, **kwargs):
        last_error = None
        for attempt in range(1, max_attempts + 1):
            try:
                return fn(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                sleep_s = min(10.0, 0.8 * (2 ** (attempt - 1)))
                logger.warning("%s failed (%s/%s): %s", label, attempt, max_attempts, exc)
                if attempt < max_attempts:
                    time.sleep(sleep_s)
        raise RuntimeError(f"{label} failed after retries: {last_error}") from last_error

    def run(
        self,
        asof_date: str | None = None,
        lookback_days: int = 3650,
        initial_backfill: bool = False,
        chunk_years: int = 2,
        chunks: int = 1,
    ) -> BatchResult:
        dt = pd.to_datetime(asof_date).date() if asof_date else self.collector.recent_business_day()
        asof_str = dt.strftime("%Y-%m-%d")
        chunk_years = max(1, chunk_years)
        chunks = max(1, chunks)
        run_id = f"daily_batch:{asof_str}"
        checkpoint_key = f"fundamental_chunk:{asof_str}:{chunk_years}:{chunks}"
        logger.info(
            "Starting daily batch: asof=%s, lookback_days=%s, initial_backfill=%s, chunk_years=%s, chunks=%s",
            asof_str,
            lookback_days,
            initial_backfill,
            chunk_years,
            chunks,
        )

        tickers = self.collector.tickers()
        ticker_count = self.repo.upsert_tickers(tickers)
        logger.info("Tickers upserted: %s", ticker_count)

        default_price_from_dt = dt - timedelta(days=lookback_days * 2)
        price_rows = 0
        price_failures = 0
        for idx, ticker in enumerate(tickers["ticker"], start=1):
            if initial_backfill:
                from_dt = default_price_from_dt
            else:
                checkpoint = self.repo.get_collection_checkpoint(ticker)
                last_price_date = checkpoint.get("last_price_date")
                if last_price_date:
                    from_dt = pd.to_datetime(last_price_date).date() + timedelta(days=1)
                else:
                    from_dt = dt - timedelta(days=45)
            if from_dt > dt:
                continue
            try:
                ohlcv = self._safe_collect(
                    self.collector.ohlcv,
                    from_dt,
                    dt,
                    ticker,
                    label=f"ohlcv:{ticker}",
                )
                upserted = self.repo.upsert_prices(ohlcv)
                price_rows += upserted
                if not ohlcv.empty:
                    max_date = pd.to_datetime(ohlcv["date"]).max().strftime("%Y-%m-%d")
                    self.repo.upsert_collection_checkpoint(ticker, last_price_date=max_date)
            except Exception as exc:  # noqa: BLE001
                price_failures += 1
                logger.error("Price collection failed ticker=%s: %s", ticker, exc)
            if idx % 200 == 0 or idx == ticker_count:
                logger.info(
                    "Price progress: %s/%s tickers, rows=%s, failures=%s",
                    idx,
                    ticker_count,
                    price_rows,
                    price_failures,
                )

        cap_rows = 0
        cap_from_dt = default_price_from_dt if initial_backfill else pd.to_datetime(self.repo.get_latest_price_date() or asof_str).date() - timedelta(days=10)
        trading_dates = self.collector.trading_dates(cap_from_dt, dt)
        for idx, trading_dt in enumerate(trading_dates, start=1):
            cap_frame = self._safe_collect(self.collector.market_cap, trading_dt, label=f"market_cap:{trading_dt}")
            cap_rows += self.repo.upsert_cap(cap_frame)
            if idx % 30 == 0 or idx == len(trading_dates):
                logger.info("Cap progress: %s/%s dates, rows=%s", idx, len(trading_dates), cap_rows)

        fund_rows = 0
        latest_fundamental = self.repo.get_latest_fundamental_date()
        if initial_backfill:
            base_from_dt = dt - timedelta(days=max(lookback_days, 3650) + 365)
        elif latest_fundamental:
            base_from_dt = pd.to_datetime(latest_fundamental).date() - timedelta(days=31)
        else:
            base_from_dt = dt - timedelta(days=400)

        saved_chunk = self.repo.get_batch_checkpoint(checkpoint_key)
        start_chunk = int(saved_chunk) if saved_chunk and saved_chunk.isdigit() else 1
        start_chunk = min(max(start_chunk, 1), chunks)

        for chunk_idx in range(start_chunk, chunks + 1):
            chunk_start = dt - timedelta(days=chunk_years * 365 * chunk_idx)
            chunk_end = dt - timedelta(days=chunk_years * 365 * (chunk_idx - 1))
            chunk_from_dt = max(base_from_dt, chunk_start)
            chunk_to_dt = min(dt, chunk_end)
            if chunk_from_dt > chunk_to_dt:
                continue

            stage = f"fundamental_chunk_{chunk_idx}"
            self.repo.log_job_stage(
                run_id=run_id,
                stage=stage,
                status="running",
                message=f"range={chunk_from_dt}~{chunk_to_dt}",
            )
            try:
                fundamental_trading_dates = self.collector.trading_dates(chunk_from_dt, chunk_to_dt)
                fund_dates = self._fundamental_backfill_dates(chunk_to_dt, fundamental_trading_dates)
                chunk_rows = 0
                logger.info(
                    "Fundamental fetch anchors: chunk=%s/%s, dates=%s, window=%s~%s",
                    chunk_idx,
                    chunks,
                    len(fund_dates),
                    chunk_from_dt,
                    chunk_to_dt,
                )
                for idx, fdt in enumerate(fund_dates, start=1):
                    fund_frame = self._safe_collect(
                        self.collector.fundamental_market_metrics,
                        fdt,
                        label=f"fundamental:{fdt}",
                    )
                    financial_frame = self._safe_collect(self._collect_financials, fdt, label=f"financials:{fdt}")
                    upsert_rows = self.repo.upsert_fundamental(fund_frame)
                    upsert_rows += self.repo.upsert_financials(financial_frame, fdt.strftime("%Y-%m-%d"))
                    fund_rows += upsert_rows
                    chunk_rows += upsert_rows
                    touched = set()
                    if not fund_frame.empty:
                        touched.update(fund_frame["ticker"].astype(str).tolist())
                    if not financial_frame.empty:
                        touched.update(financial_frame["ticker"].astype(str).tolist())
                    checkpoint_date = fdt.strftime("%Y-%m-%d")
                    for ticker in touched:
                        self.repo.upsert_collection_checkpoint(ticker, last_fundamental_date=checkpoint_date)
                    if idx % 10 == 0 or idx == len(fund_dates):
                        logger.info(
                            "Fundamental progress chunk=%s/%s: %s/%s dates, chunk_rows=%s, total_rows=%s",
                            chunk_idx,
                            chunks,
                            idx,
                            len(fund_dates),
                            chunk_rows,
                            fund_rows,
                        )

                self.repo.log_job_stage(
                    run_id=run_id,
                    stage=stage,
                    status="success",
                    message=f"chunk={chunk_idx}/{chunks}, range={chunk_from_dt}~{chunk_to_dt}",
                    row_count=chunk_rows,
                )
                logger.info(
                    "Fundamental chunk done: chunk=%s/%s, range=%s~%s, upsert_rows=%s",
                    chunk_idx,
                    chunks,
                    chunk_from_dt,
                    chunk_to_dt,
                    chunk_rows,
                )
                if chunk_idx < chunks:
                    self.repo.set_batch_checkpoint(checkpoint_key, str(chunk_idx + 1))
                else:
                    self.repo.clear_batch_checkpoint(checkpoint_key)
            except Exception as exc:
                self.repo.log_job_stage(
                    run_id=run_id,
                    stage=stage,
                    status="failed",
                    message=f"chunk={chunk_idx}/{chunks}, range={chunk_from_dt}~{chunk_to_dt}, error={exc}",
                )
                self.repo.set_batch_checkpoint(checkpoint_key, str(chunk_idx))
                logger.error("Fundamental chunk failed: chunk=%s/%s, error=%s", chunk_idx, chunks, exc)
                raise

        price_window = self.repo.get_price_window(asof_str, window=lookback_days)
        daily = self.repo.get_daily_join(asof_str)
        fund_hist = self.repo.get_fundamental_window(asof_str, years=11)
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

    def rebuild_snapshot_only(self, asof_date: str | None = None, lookback_days: int = 3650) -> BatchResult:
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
        fund_hist = self.repo.get_fundamental_window(asof_str, years=11)
        if price_window.empty or daily.empty:
            raise ValueError(
                f"No cached rows for asof={asof_str}. Run full collection for this date or choose an existing asof date."
            )

        snapshot = build_snapshot(price_window, daily, fund_hist, asof_str)
        snap_rows = self.repo.replace_snapshot(asof_str, snapshot)

        logger.info("Snapshot-only rebuild completed: asof=%s, tickers=%s, snapshot=%s", asof_str, ticker_count, snap_rows)

        return BatchResult(asof_date=asof_str, tickers=ticker_count, prices=0, cap=0, fundamental=0, snapshot=snap_rows)
