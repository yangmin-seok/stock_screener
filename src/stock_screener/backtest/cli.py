from __future__ import annotations

import argparse
from pathlib import Path

from stock_screener.backtest.config import load_backtest_config
from stock_screener.storage.db import init_db
from stock_screener.storage.repository import Repository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Backtest CLI bootstrap")
    parser.add_argument("--db-path", default="data/screener.db", help="SQLite path used by repository")
    parser.add_argument("--config", required=True, help="Path to backtest YAML config")
    return parser


def main() -> None:
    args = build_parser().parse_args()

    db_path = Path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    config = load_backtest_config(args.config)

    init_db(db_path)
    Repository(db_path)

    print(
        "Backtest bootstrap complete "
        f"(db_path={db_path}, run_name={config.run.get('name')}, period={config.run.get('start_date')}~{config.run.get('end_date')})"
    )


if __name__ == "__main__":
    main()
