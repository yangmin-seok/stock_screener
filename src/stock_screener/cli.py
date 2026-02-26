from __future__ import annotations

import argparse
import logging
from pathlib import Path

from stock_screener.pipelines.daily_batch import DailyBatchPipeline
from stock_screener.storage.db import init_db
from stock_screener.storage.repository import Repository


def _print_latest_batch_report(db_path: Path) -> None:
    init_db(db_path)
    repo = Repository(db_path)
    rows = repo.get_latest_batch_chunk_report("daily_batch:")
    if not rows:
        print("No daily_batch run found in job_log.")
        return

    run_id = rows[0]["run_id"]
    print(f"Latest run_id: {run_id}")
    print(
        "chunk | status  | row_count | eps_non_null          | bps_non_null          | revenue_non_null      | message"
    )
    print(
        "------|---------|-----------|------------------------|-----------------------|-----------------------|--------"
    )
    for row in rows:
        chunk_text = "-"
        if row["chunk_idx"] is not None and row["chunk_total"] is not None:
            chunk_text = f"{row['chunk_idx']}/{row['chunk_total']}"
        message = row["message"] or ""
        print(
            f"{chunk_text:<5} | "
            f"{row['status']:<7} | "
            f"{(row['row_count'] if row['row_count'] is not None else '-'):>9} | "
            f"{row['eps_ratio']:<22} | "
            f"{row['bps_ratio']:<21} | "
            f"{row['revenue_ratio']:<21} | "
            f"{message}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run daily batch for stock screener")
    parser.add_argument("--db-path", default="data/screener.db")
    parser.add_argument("--asof-date", default=None)
    parser.add_argument("--lookback-days", type=int, default=3650)
    parser.add_argument("--chunk-years", type=int, default=2, help="Fundamental backfill years per chunk")
    parser.add_argument("--chunks", type=int, default=1, help="Number of fundamental backfill chunks")
    parser.add_argument("--fundamental-anchor-mode", choices=["minimal", "full"], default="full", help="Anchor density for fundamental backfill dates")
    parser.add_argument("--max-price-workers", type=int, default=6, help="Max workers for parallel OHLCV collection (bounded at 8)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    parser.add_argument("--snapshot-only", action="store_true", help="Rebuild snapshot from cached DB data only")
    parser.add_argument("--initial-backfill", action="store_true", help="Run long-window initial backfill")
    parser.add_argument("--update-reserve-only", action="store_true", help="Update reserve ratio only (Naver crawl)")
    parser.add_argument(
        "--rebuild-snapshot",
        action="store_true",
        help="When used with --update-reserve-only, rebuild snapshot_metrics right after reserve update",
    )
    parser.add_argument(
        "--report-latest-batch",
        action="store_true",
        help="Print latest daily_batch:* chunk status report from job_log",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    db_path = Path(args.db_path)
    if args.report_latest_batch:
        _print_latest_batch_report(db_path)
        return

    pipeline = DailyBatchPipeline(db_path)
    if args.update_reserve_only:
        updated_asof, rows = pipeline.update_reserve_ratio_only(asof_date=args.asof_date)
        print(f"reserve_ratio updated: asof={updated_asof}, rows={rows}")

        investor_flow_message = (
            "ℹ️ --update-reserve-only does not collect investor flow(외국인 순매수) data; "
            "investor_flow_daily remains unchanged in this run."
        )
        logging.info(investor_flow_message)
        print(investor_flow_message)

        if args.rebuild_snapshot:
            snapshot_result = pipeline.rebuild_snapshot_only(asof_date=updated_asof, lookback_days=args.lookback_days)
            print(f"snapshot_metrics rebuilt: asof={snapshot_result.asof_date}, rows={snapshot_result.snapshot}")
        else:
            warning_message = (
                "⚠️ reserve_ratio is updated but snapshot_metrics is unchanged. "
                "Run --snapshot-only or add --rebuild-snapshot to reflect reserve_ratio changes in the UI."
            )
            logging.warning(warning_message)
            print(warning_message)
    elif args.snapshot_only:
        result = pipeline.rebuild_snapshot_only(asof_date=args.asof_date, lookback_days=args.lookback_days)
        print(result)
    else:
        result = pipeline.run(
            asof_date=args.asof_date,
            lookback_days=args.lookback_days,
            initial_backfill=args.initial_backfill,
            chunk_years=args.chunk_years,
            chunks=args.chunks,
            fundamental_anchor_mode=args.fundamental_anchor_mode,
            max_price_workers=args.max_price_workers,
        )
        print(result)


if __name__ == "__main__":
    main()
