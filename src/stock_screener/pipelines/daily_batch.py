from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import inspect
import logging
from pathlib import Path
import os
import time
from typing import Callable
from threading import Lock

import pandas as pd

from stock_screener.collectors.fundamental_provider import FundamentalProvider, merge_financial_records
from stock_screener.collectors.dart_client import DartClient
from stock_screener.collectors.dart_financial_provider import DartFinancialProvider
from stock_screener.collectors.naver_ratio_client import NaverRatioCollector
from stock_screener.collectors.pykrx_client import PykrxCollector, PykrxFinancialFallbackProvider
from stock_screener.config import get_required_env, load_env_file
from stock_screener.features.metrics import build_snapshot
from stock_screener.storage.db import init_db
from stock_screener.storage.repository import Repository


logger = logging.getLogger(__name__)


class BatchCancelledError(RuntimeError):
    """Raised when a batch run is cancelled at a safe checkpoint."""


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
        load_env_file()
        self.dart_client: DartClient | None = None
        self.financial_providers: list[FundamentalProvider] = []


    def _ensure_dart_client(self) -> None:
        if self.dart_client is not None:
            return
        dart_api_key = get_required_env("DART_API_KEY")
        endpoint_override = os.environ.get("DART_FINANCIALS_ENDPOINT")
        self.dart_client = DartClient(
            api_key=dart_api_key,
            endpoint=endpoint_override if endpoint_override else DartClient.endpoint,
        )
        self.financial_providers = [
            DartFinancialProvider(self.dart_client),
            PykrxFinancialFallbackProvider(self.collector),
        ]

    def update_reserve_ratio_only(self, asof_date: str | None = None) -> tuple[str, int]:
        """Update reserve ratio only.

        This mode only crawls Naver reserve ratio data and leaves
        ``investor_flow_daily`` untouched.
        """
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

        logger.info(
            "Starting reserve ratio update: asof=%s, tickers=%s (investor_flow_daily untouched)",
            asof_str,
            len(tickers),
        )
        ratio_frame = self.ratio_collector.latest_reserve_ratio(tickers)
        rows = self.repo.upsert_reserve_ratio(asof_str, ratio_frame)
        logger.info(
            "Reserve ratio update completed: asof=%s, rows=%s (investor_flow_daily untouched)",
            asof_str,
            rows,
        )
        return asof_str, rows

    @staticmethod
    def _fundamental_backfill_dates(asof: date, trading_dates: list[date], mode: str = "full") -> list[date]:
        if not trading_dates:
            return [asof]

        ts = pd.Series(pd.to_datetime(sorted(trading_dates)))
        dates: set[date] = set()

        if mode == "full":
            month_last = ts.groupby(ts.dt.to_period("M")).max()
            dates.update(dt.date() for dt in month_last.tolist())
        quarter_last = ts.groupby(ts.dt.to_period("Q")).max()
        dates.update(dt.date() for dt in quarter_last.tolist())

        asof_ts = pd.Timestamp(asof)
        dates.add(ts.iloc[-1].date())
        for years in range(1, 11):
            target = asof_ts - pd.DateOffset(years=years)
            candidates = ts[ts <= target]
            if not candidates.empty:
                dates.add(candidates.iloc[-1].date())

        return sorted(dates)

    @staticmethod
    def _frame_metric_count(frame: pd.DataFrame, metric: str) -> int:
        if frame.empty or metric not in frame.columns:
            return 0
        return int(frame[metric].notna().sum())


    @staticmethod
    def _build_financial_quality_rows(financial_frame: pd.DataFrame, *, asof: str, metric_date: str, chunk_idx: int) -> pd.DataFrame:
        if financial_frame.empty:
            return pd.DataFrame(columns=[
                "asof_date", "metric_date", "chunk_idx", "metric_scope", "provider", "source",
                "fiscal_period", "period_type", "ticker", "rows_total", "eps_null", "bps_null",
            ])

        base = financial_frame.copy()
        for col, default in (("source", ""), ("fiscal_period", ""), ("period_type", ""), ("ticker", "")):
            if col not in base.columns:
                base[col] = default
            base[col] = base[col].fillna(default).astype(str)

        base["eps_null"] = base["eps"].isna().astype(int) if "eps" in base.columns else 1
        base["bps_null"] = base["bps"].isna().astype(int) if "bps" in base.columns else 1

        def _aggregate(group_cols: list[str], scope: str) -> pd.DataFrame:
            grouped = (
                base.groupby(group_cols, dropna=False)
                .agg(rows_total=("ticker", "size"), eps_null=("eps_null", "sum"), bps_null=("bps_null", "sum"))
                .reset_index()
            )
            grouped["asof_date"] = asof
            grouped["metric_date"] = metric_date
            grouped["chunk_idx"] = chunk_idx
            grouped["metric_scope"] = scope
            grouped["provider"] = ""
            grouped["source"] = grouped.get("source", "")
            grouped["fiscal_period"] = grouped.get("fiscal_period", "")
            grouped["period_type"] = grouped.get("period_type", "")
            grouped["ticker"] = grouped.get("ticker", "")
            return grouped[
                [
                    "asof_date", "metric_date", "chunk_idx", "metric_scope", "provider", "source",
                    "fiscal_period", "period_type", "ticker", "rows_total", "eps_null", "bps_null",
                ]
            ]

        by_source = _aggregate(["source"], "source")
        by_period = _aggregate(["fiscal_period", "period_type"], "period")
        by_ticker = _aggregate(["ticker"], "ticker")
        return pd.concat([by_source, by_period, by_ticker], ignore_index=True)

    def _collect_financials(self, dt: date, *, asof: str, chunk_idx: int) -> pd.DataFrame:
        provider_frames: list[pd.DataFrame] = []
        for provider in self.financial_providers:
            frame = provider.fetch_financials(dt)
            provider_frames.append(frame)
            logger.info(
                "Financial provider output: asof=%s, chunk_idx=%s, fdt=%s, provider=%s, rows=%s, eps_non_null=%s, bps_non_null=%s",
                asof,
                chunk_idx,
                dt,
                provider.__class__.__name__,
                len(frame),
                self._frame_metric_count(frame, "eps"),
                self._frame_metric_count(frame, "bps"),
            )
        merged = merge_financial_records(provider_frames)
        logger.info(
            "Financial merge output: asof=%s, chunk_idx=%s, fdt=%s, rows=%s, eps_non_null=%s, bps_non_null=%s",
            asof,
            chunk_idx,
            dt,
            len(merged),
            self._frame_metric_count(merged, "eps"),
            self._frame_metric_count(merged, "bps"),
        )
        if merged.empty:
            logger.warning("No financial provider rows: asof=%s, chunk_idx=%s, fdt=%s", asof, chunk_idx, dt)
        return merged

    def _safe_collect(self, fn, *args, label: str, max_attempts: int = 4, **kwargs):
        call_kwargs = kwargs
        if kwargs:
            try:
                signature = inspect.signature(fn)
                accepts_var_kwargs = any(
                    parameter.kind == inspect.Parameter.VAR_KEYWORD
                    for parameter in signature.parameters.values()
                )
                if not accepts_var_kwargs:
                    allowed = {
                        name
                        for name, parameter in signature.parameters.items()
                        if parameter.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
                    }
                    call_kwargs = {name: value for name, value in kwargs.items() if name in allowed}
            except (TypeError, ValueError):
                call_kwargs = kwargs

        last_error = None
        for attempt in range(1, max_attempts + 1):
            try:
                return fn(*args, **call_kwargs)
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
        rebuild_snapshot: bool = True,
        should_cancel: Callable[[], bool] | None = None,
        fundamental_anchor_mode: str = "full",
        max_price_workers: int = 6,
    ) -> BatchResult:
        dt = pd.to_datetime(asof_date).date() if asof_date else self.collector.recent_business_day()
        asof_str = dt.strftime("%Y-%m-%d")
        chunk_years = max(1, chunk_years)
        chunks = max(1, chunks)
        max_price_workers = min(8, max(1, max_price_workers))
        if fundamental_anchor_mode not in {"minimal", "full"}:
            raise ValueError("fundamental_anchor_mode must be one of: minimal, full")
        run_id = f"daily_batch:{asof_str}"
        checkpoint_key = f"fundamental_chunk:{asof_str}:{chunk_years}:{chunks}"
        self._ensure_dart_client()

        logger.info(
            "Starting daily batch: asof=%s, lookback_days=%s, initial_backfill=%s, chunk_years=%s, chunks=%s, rebuild_snapshot=%s, fundamental_anchor_mode=%s, max_price_workers=%s",
            asof_str,
            lookback_days,
            initial_backfill,
            chunk_years,
            chunks,
            rebuild_snapshot,
            fundamental_anchor_mode,
            max_price_workers,
        )

        api_calls: dict[str, int] = {}
        api_calls_lock = Lock()

        def _inc_api(stage: str, count: int = 1) -> None:
            with api_calls_lock:
                api_calls[stage] = api_calls.get(stage, 0) + count

        def _safe_collect_stage(stage: str, fn, *args, label: str, **kwargs):
            _inc_api(stage)
            return self._safe_collect(fn, *args, label=label, **kwargs)

        def _stage_message(elapsed_s: float, stage_api_calls: int, rows_upserted: int, extra: str | None = None) -> str:
            base = f"elapsed_s={elapsed_s:.3f}, api_calls={stage_api_calls}, rows_upserted={rows_upserted}"
            return f"{base}, {extra}" if extra else base

        def _raise_if_cancelled(*, phase: str) -> None:
            if should_cancel and should_cancel():
                logger.info("Daily batch cancel requested during %s", phase)
                raise BatchCancelledError(f"Cancelled during {phase}")

        tickers = self.collector.tickers()
        ticker_count = self.repo.upsert_tickers(tickers)
        logger.info("Tickers upserted: %s", ticker_count)

        default_price_from_dt = dt - timedelta(days=lookback_days * 2)
        earliest_price_date = self.repo.get_earliest_price_date()
        latest_price_date = self.repo.get_latest_price_date()
        should_backfill_prices = initial_backfill
        if not should_backfill_prices and lookback_days >= 3650:
            if not earliest_price_date:
                should_backfill_prices = True
            else:
                earliest_dt = pd.to_datetime(earliest_price_date, errors="coerce")
                if pd.notna(earliest_dt) and earliest_dt.date() > default_price_from_dt:
                    should_backfill_prices = True
        if should_backfill_prices and not initial_backfill:
            logger.info(
                "Auto-enabling long-window price/cap/investor-flow backfill: earliest_price=%s, target_from=%s",
                earliest_price_date,
                default_price_from_dt,
            )

        price_rows = 0
        price_failures = 0
        self.repo.log_job_stage(run_id=run_id, stage="price_collection", status="running", message="started")
        price_stage_started = time.perf_counter()

        ticker_jobs: list[tuple[str, date]] = []
        for idx, ticker in enumerate(tickers["ticker"], start=1):
            _raise_if_cancelled(phase=f"price_collection_prepare:{idx}/{ticker_count}")
            if should_backfill_prices:
                from_dt = default_price_from_dt
            else:
                checkpoint = self.repo.get_collection_checkpoint(ticker)
                last_price_date = checkpoint.get("last_price_date")
                if last_price_date:
                    from_dt = pd.to_datetime(last_price_date).date() + timedelta(days=1)
                else:
                    from_dt = dt - timedelta(days=45)
            if from_dt <= dt:
                ticker_jobs.append((ticker, from_dt))

        price_checkpoint_rows: list[dict[str, str | None]] = []

        def _collect_price_ticker(ticker: str, from_dt: date) -> tuple[str, pd.DataFrame, str | None, Exception | None]:
            try:
                ohlcv = _safe_collect_stage("price_collection", self.collector.ohlcv, from_dt, dt, ticker, label=f"ohlcv:{ticker}")
                max_date = None
                if not ohlcv.empty:
                    max_date = pd.to_datetime(ohlcv["date"]).max().strftime("%Y-%m-%d")
                return ticker, ohlcv, max_date, None
            except Exception as exc:  # noqa: BLE001
                return ticker, pd.DataFrame(), None, exc

        with ThreadPoolExecutor(max_workers=max_price_workers) as executor:
            future_map = {executor.submit(_collect_price_ticker, ticker, from_dt): ticker for ticker, from_dt in ticker_jobs}
            for idx, future in enumerate(as_completed(future_map), start=1):
                _raise_if_cancelled(phase=f"price_collection:{idx}/{len(future_map)}")
                ticker, ohlcv, max_date, error = future.result()
                if error is not None:
                    price_failures += 1
                    logger.error("Price collection failed ticker=%s: %s", ticker, error)
                    continue
                upserted = self.repo.upsert_prices(ohlcv)
                price_rows += upserted
                if max_date:
                    price_checkpoint_rows.append({"ticker": ticker, "last_price_date": max_date, "last_fundamental_date": None})
                if idx % 200 == 0 or idx == len(future_map):
                    logger.info("Price progress: %s/%s tickers, rows=%s, failures=%s", idx, len(future_map), price_rows, price_failures)

        self.repo.upsert_collection_checkpoints_bulk(price_checkpoint_rows)
        price_elapsed = time.perf_counter() - price_stage_started
        price_rps = price_rows / price_elapsed if price_elapsed > 0 else 0.0
        logger.info("Stage price_collection done: elapsed_s=%.3f, rows=%s, rows_per_sec=%.2f, api_calls=%s", price_elapsed, price_rows, price_rps, api_calls.get("price_collection", 0))
        self.repo.log_job_stage(
            run_id=run_id,
            stage="price_collection",
            status="success",
            message=_stage_message(price_elapsed, api_calls.get("price_collection", 0), price_rows, extra=f"failures={price_failures}"),
            row_count=price_rows,
        )

        cap_rows = 0
        cap_from_dt = default_price_from_dt if should_backfill_prices else pd.to_datetime(latest_price_date or asof_str).date() - timedelta(days=10)
        trading_dates = self.collector.trading_dates(cap_from_dt, dt)

        self.repo.log_job_stage(run_id=run_id, stage="cap_collection", status="running", message="started")
        cap_stage_started = time.perf_counter()
        for idx, trading_dt in enumerate(trading_dates, start=1):
            _raise_if_cancelled(phase=f"cap_collection:{idx}/{len(trading_dates)}")
            cap_frame = _safe_collect_stage("cap_collection", self.collector.market_cap, trading_dt, label=f"market_cap:{trading_dt}")
            cap_rows += self.repo.upsert_cap(cap_frame)
            if idx % 30 == 0 or idx == len(trading_dates):
                logger.info("Cap progress: %s/%s dates, rows=%s", idx, len(trading_dates), cap_rows)
        cap_elapsed = time.perf_counter() - cap_stage_started
        cap_rps = cap_rows / cap_elapsed if cap_elapsed > 0 else 0.0
        logger.info("Stage cap_collection done: elapsed_s=%.3f, rows=%s, rows_per_sec=%.2f, api_calls=%s", cap_elapsed, cap_rows, cap_rps, api_calls.get("cap_collection", 0))
        self.repo.log_job_stage(run_id=run_id, stage="cap_collection", status="success", message=_stage_message(cap_elapsed, api_calls.get("cap_collection", 0), cap_rows), row_count=cap_rows)

        investor_flow_rows = 0
        self.repo.log_job_stage(run_id=run_id, stage="investor_flow_collection", status="running", message="started")
        flow_stage_started = time.perf_counter()
        for idx, trading_dt in enumerate(trading_dates, start=1):
            _raise_if_cancelled(phase=f"investor_flow_collection:{idx}/{len(trading_dates)}")
            flow_frame = _safe_collect_stage("investor_flow_collection", self.collector.foreign_investor_flow, trading_dt, label=f"investor_flow:{trading_dt}")
            investor_flow_rows += self.repo.upsert_investor_flow(flow_frame)
            if idx % 30 == 0 or idx == len(trading_dates):
                logger.info("Investor flow progress: %s/%s dates, rows=%s", idx, len(trading_dates), investor_flow_rows)
        flow_elapsed = time.perf_counter() - flow_stage_started
        flow_rps = investor_flow_rows / flow_elapsed if flow_elapsed > 0 else 0.0
        logger.info("Stage investor_flow_collection done: elapsed_s=%.3f, rows=%s, rows_per_sec=%.2f, api_calls=%s", flow_elapsed, investor_flow_rows, flow_rps, api_calls.get("investor_flow_collection", 0))
        self.repo.log_job_stage(run_id=run_id, stage="investor_flow_collection", status="success", message=_stage_message(flow_elapsed, api_calls.get("investor_flow_collection", 0), investor_flow_rows), row_count=investor_flow_rows)

        fund_rows = 0
        latest_fundamental = self.repo.get_latest_fundamental_date()
        if initial_backfill:
            base_from_dt = dt - timedelta(days=max(lookback_days, 3650) + 365)
        elif latest_fundamental:
            parsed_latest = pd.to_datetime(latest_fundamental, errors="coerce")
            if pd.isna(parsed_latest):
                logger.warning("Invalid latest fundamental checkpoint: %s. Falling back to default fundamental window.", latest_fundamental)
                base_from_dt = dt - timedelta(days=400)
            else:
                latest_fundamental_dt = parsed_latest.date()
                if latest_fundamental_dt > dt:
                    logger.warning("Latest fundamental date is in the future (latest=%s, asof=%s). Clamping to asof for chunk window.", latest_fundamental_dt, dt)
                    latest_fundamental_dt = dt
                base_from_dt = latest_fundamental_dt - timedelta(days=31)
        else:
            base_from_dt = dt - timedelta(days=400)

        saved_chunk = self.repo.get_batch_checkpoint(checkpoint_key)
        start_chunk = int(saved_chunk) if saved_chunk and saved_chunk.isdigit() else 1
        start_chunk = min(max(start_chunk, 1), chunks)

        chunks_done = 0
        for chunk_idx in range(start_chunk, chunks + 1):
            if should_cancel and should_cancel():
                logger.info("Daily batch cancel requested before chunk start: chunk=%s/%s", chunk_idx, chunks)
                raise BatchCancelledError(f"Cancelled before chunk={chunk_idx}/{chunks}")

            chunk_start = dt - timedelta(days=chunk_years * 365 * chunk_idx)
            chunk_end = dt - timedelta(days=chunk_years * 365 * (chunk_idx - 1))
            chunk_from_dt = max(base_from_dt, chunk_start)
            chunk_to_dt = min(dt, chunk_end)
            if chunk_from_dt > chunk_to_dt:
                logger.warning("Skipping fundamental chunk due to empty range: chunk=%s/%s, base_from=%s, chunk_from=%s, chunk_to=%s", chunk_idx, chunks, base_from_dt, chunk_from_dt, chunk_to_dt)
                continue

            stage = f"fundamental_chunk_{chunk_idx}"
            self.repo.log_job_stage(run_id=run_id, stage=stage, status="running", message=f"range={chunk_from_dt}~{chunk_to_dt}")
            chunk_stage_started = time.perf_counter()
            try:
                fundamental_trading_dates = self.collector.trading_dates(chunk_from_dt, chunk_to_dt)
                fund_dates = self._fundamental_backfill_dates(chunk_to_dt, fundamental_trading_dates, mode=fundamental_anchor_mode)
                logger.info("Fundamental anchors profiled: chunk=%s/%s, mode=%s, dates=%s, trading_dates=%s", chunk_idx, chunks, fundamental_anchor_mode, len(fund_dates), len(fundamental_trading_dates))
                chunk_rows = 0
                chunk_eps_non_null = 0
                chunk_bps_non_null = 0
                chunk_revenue_non_null = 0
                logger.info("Fundamental fetch anchors: chunk=%s/%s, dates=%s, window=%s~%s", chunk_idx, chunks, len(fund_dates), chunk_from_dt, chunk_to_dt)
                for idx, fdt in enumerate(fund_dates, start=1):
                    _raise_if_cancelled(phase=f"fundamental_collection:chunk={chunk_idx}/{chunks},date={idx}/{len(fund_dates)}")
                    fund_frame = _safe_collect_stage(stage, self.collector.fundamental_market_metrics, fdt, label=f"fundamental:{fdt}")
                    financial_frame = _safe_collect_stage(stage, self._collect_financials, fdt, asof=asof_str, chunk_idx=chunk_idx, label=f"financials:{fdt}")
                    eps_null = len(financial_frame) - self._frame_metric_count(financial_frame, "eps")
                    bps_null = len(financial_frame) - self._frame_metric_count(financial_frame, "bps")
                    logger.info("Financial frame before upsert: asof=%s, chunk_idx=%s, fdt=%s, rows=%s, eps_non_null=%s, bps_non_null=%s, eps_null=%s, bps_null=%s", asof_str, chunk_idx, fdt, len(financial_frame), self._frame_metric_count(financial_frame, "eps"), self._frame_metric_count(financial_frame, "bps"), eps_null, bps_null)
                    quality_rows = self._build_financial_quality_rows(financial_frame, asof=asof_str, metric_date=fdt.strftime("%Y-%m-%d"), chunk_idx=chunk_idx)
                    upsert_rows = self.repo.upsert_fundamental(fund_frame)
                    upsert_rows += self.repo.upsert_financials(financial_frame, fdt.strftime("%Y-%m-%d"))
                    upsert_rows += self.repo.upsert_financials_periodic(financial_frame)
                    upsert_rows += self.repo.upsert_financial_quality(quality_rows)
                    if not financial_frame.empty:
                        chunk_eps_non_null += int(financial_frame["eps"].notna().sum())
                        chunk_bps_non_null += int(financial_frame["bps"].notna().sum())
                        chunk_revenue_non_null += int(financial_frame["revenue"].notna().sum())
                    fund_rows += upsert_rows
                    chunk_rows += upsert_rows
                    touched = set()
                    if not fund_frame.empty:
                        touched.update(fund_frame["ticker"].astype(str).tolist())
                    if not financial_frame.empty:
                        touched.update(financial_frame["ticker"].astype(str).tolist())
                    checkpoint_date = fdt.strftime("%Y-%m-%d")
                    checkpoint_rows = [{"ticker": ticker, "last_price_date": None, "last_fundamental_date": checkpoint_date} for ticker in touched]
                    self.repo.upsert_collection_checkpoints_bulk(checkpoint_rows)
                    if idx % 10 == 0 or idx == len(fund_dates):
                        logger.info("Fundamental progress chunk=%s/%s: %s/%s dates, chunk_rows=%s, total_rows=%s", chunk_idx, chunks, idx, len(fund_dates), chunk_rows, fund_rows)

                chunk_elapsed = time.perf_counter() - chunk_stage_started
                chunk_rps = chunk_rows / chunk_elapsed if chunk_elapsed > 0 else 0.0
                quality_counts = f"eps_non_null={chunk_eps_non_null}, bps_non_null={chunk_bps_non_null}, revenue_non_null={chunk_revenue_non_null}, rows_per_sec={chunk_rps:.2f}"
                self.repo.log_job_stage(
                    run_id=run_id,
                    stage=stage,
                    status="success",
                    message=_stage_message(chunk_elapsed, api_calls.get(stage, 0), chunk_rows, extra=f"chunk={chunk_idx}/{chunks}, range={chunk_from_dt}~{chunk_to_dt}, {quality_counts}"),
                    row_count=chunk_rows,
                )
                logger.info("Stage %s done: elapsed_s=%.3f, rows=%s, rows_per_sec=%.2f, api_calls=%s", stage, chunk_elapsed, chunk_rows, chunk_rps, api_calls.get(stage, 0))
                chunks_done = chunk_idx
                if chunk_idx < chunks:
                    self.repo.set_batch_checkpoint(checkpoint_key, str(chunk_idx + 1))
                else:
                    self.repo.clear_batch_checkpoint(checkpoint_key)
            except BatchCancelledError as exc:
                chunk_elapsed = time.perf_counter() - chunk_stage_started
                self.repo.log_job_stage(run_id=run_id, stage=stage, status="cancelled", message=_stage_message(chunk_elapsed, api_calls.get(stage, 0), 0, extra=f"chunk={chunk_idx}/{chunks}, range={chunk_from_dt}~{chunk_to_dt}, cancelled={exc}"))
                self.repo.set_batch_checkpoint(checkpoint_key, str(chunk_idx))
                logger.info("Fundamental chunk cancelled: chunk=%s/%s, reason=%s", chunk_idx, chunks, exc)
                raise
            except Exception as exc:
                chunk_elapsed = time.perf_counter() - chunk_stage_started
                self.repo.log_job_stage(run_id=run_id, stage=stage, status="failed", message=_stage_message(chunk_elapsed, api_calls.get(stage, 0), 0, extra=f"chunk={chunk_idx}/{chunks}, range={chunk_from_dt}~{chunk_to_dt}, error={exc}"))
                self.repo.set_batch_checkpoint(checkpoint_key, str(chunk_idx))
                logger.error("Fundamental chunk failed: chunk=%s/%s, error=%s", chunk_idx, chunks, exc)
                raise

        snap_rows = 0
        snapshot_rebuilt = False
        if rebuild_snapshot:
            self.repo.log_job_stage(run_id=run_id, stage="snapshot_build", status="running", message="started")
            snapshot_stage_started = time.perf_counter()
            price_window = self.repo.get_price_window(asof_str, window=lookback_days)
            daily = self.repo.get_daily_join(asof_str)
            fund_hist = self.repo.get_fundamental_window_periodic(asof_str, years=11)
            snapshot = build_snapshot(price_window, daily, fund_hist, asof_str)
            snap_rows = self.repo.replace_snapshot(asof_str, snapshot)
            snapshot_rebuilt = True
            snapshot_elapsed = time.perf_counter() - snapshot_stage_started
            snapshot_rps = snap_rows / snapshot_elapsed if snapshot_elapsed > 0 else 0.0
            logger.info("Stage snapshot_build done: elapsed_s=%.3f, rows=%s, rows_per_sec=%.2f", snapshot_elapsed, snap_rows, snapshot_rps)
            self.repo.log_job_stage(run_id=run_id, stage="snapshot_build", status="success", message=_stage_message(snapshot_elapsed, api_calls.get("snapshot_build", 0), snap_rows), row_count=snap_rows)

        logger.info("Daily batch completed: asof=%s, tickers=%s, prices=%s, cap=%s, investor_flow=%s, fundamental=%s, chunks_done=%s/%s, snapshot_rebuilt=%s, snapshot_rows=%s", asof_str, ticker_count, price_rows, cap_rows, investor_flow_rows, fund_rows, chunks_done, chunks, snapshot_rebuilt, snap_rows)

        return BatchResult(asof_date=asof_str, tickers=ticker_count, prices=price_rows, cap=cap_rows, fundamental=fund_rows, snapshot=snap_rows)

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
        fund_hist = self.repo.get_fundamental_window_periodic(asof_str, years=11)
        if price_window.empty or daily.empty:
            raise ValueError(
                f"No cached rows for asof={asof_str}. Run full collection for this date or choose an existing asof date."
            )

        snapshot = build_snapshot(price_window, daily, fund_hist, asof_str)
        snap_rows = self.repo.replace_snapshot(asof_str, snapshot)

        logger.info("Snapshot-only rebuild completed: asof=%s, tickers=%s, snapshot=%s", asof_str, ticker_count, snap_rows)

        return BatchResult(asof_date=asof_str, tickers=ticker_count, prices=0, cap=0, fundamental=0, snapshot=snap_rows)
