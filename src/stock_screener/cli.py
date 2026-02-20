from __future__ import annotations

import argparse
import logging
from pathlib import Path

from stock_screener.pipelines.daily_batch import DailyBatchPipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Run daily batch for stock screener")
    parser.add_argument("--db-path", default="data/screener.db")
    parser.add_argument("--asof-date", default=None)
    parser.add_argument("--lookback-days", type=int, default=400)
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    parser.add_argument("--snapshot-only", action="store_true", help="Rebuild snapshot from cached DB data only")
    parser.add_argument("--update-reserve-only", action="store_true", help="Update reserve ratio only (Naver crawl)")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    pipeline = DailyBatchPipeline(Path(args.db_path))
    if args.update_reserve_only:
        asof, rows = pipeline.update_reserve_ratio_only(asof_date=args.asof_date)
        print(f"reserve_ratio updated: asof={asof}, rows={rows}")
    elif args.snapshot_only:
        result = pipeline.rebuild_snapshot_only(asof_date=args.asof_date, lookback_days=args.lookback_days)
        print(result)
    else:
        result = pipeline.run(asof_date=args.asof_date, lookback_days=args.lookback_days)
        print(result)


if __name__ == "__main__":
    main()
