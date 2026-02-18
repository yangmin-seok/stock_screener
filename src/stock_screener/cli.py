from __future__ import annotations

import argparse
from pathlib import Path

from stock_screener.pipelines.daily_batch import DailyBatchPipeline


def main() -> None:
    parser = argparse.ArgumentParser(description="Run daily batch for stock screener")
    parser.add_argument("--db-path", default="data/screener.db")
    parser.add_argument("--asof-date", default=None)
    parser.add_argument("--lookback-days", type=int, default=400)
    args = parser.parse_args()

    pipeline = DailyBatchPipeline(Path(args.db_path))
    result = pipeline.run(asof_date=args.asof_date, lookback_days=args.lookback_days)
    print(result)


if __name__ == "__main__":
    main()
