from __future__ import annotations

import argparse
from dataclasses import asdict
from pathlib import Path

from stock_screener.backtest.config import BacktestConfig, load_backtest_config
from stock_screener.backtest.engine import run_backtest
from stock_screener.backtest.report import compute_summary, save_backtest_outputs
from stock_screener.storage.db import init_db
from stock_screener.storage.repository import Repository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Backtest CLI')
    parser.add_argument('--db-path', default='data/screener.db', help='SQLite path used by repository')
    parser.add_argument('--config', required=True, help='Path to backtest YAML config')
    parser.add_argument('--out-dir', default=None, help='Override output root directory')
    parser.add_argument('--save-trades', action='store_true', default=False, help='Save trades.csv')
    parser.add_argument('--save-positions', action='store_true', default=False, help='Save positions.csv')
    parser.add_argument('--save-daily', action='store_true', default=False, help='Save equity_curve.csv')
    parser.add_argument('--verbose', action='store_true', default=False)
    return parser


def _config_dict(config: BacktestConfig) -> dict:
    return asdict(config)


def main() -> None:
    args = build_parser().parse_args()

    db_path = Path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    config = load_backtest_config(args.config)

    init_db(db_path)
    repo = Repository(db_path)
    result = run_backtest(config, repo)

    config_dict = _config_dict(config)
    initial_capital = float(config_dict.get('run', {}).get('initial_capital', 1_000_000))
    result['summary'] = compute_summary(result, initial_capital=initial_capital)

    output_cfg = config_dict.get('output', {})
    out_dir = args.out_dir or output_cfg.get('dir', 'runs/backtests')
    run_name = config_dict.get('run', {}).get('name', 'backtest')

    run_dir = save_backtest_outputs(
        config_dict=config_dict,
        result=result,
        out_dir=out_dir,
        run_name=run_name,
        save_trades=args.save_trades or bool(output_cfg.get('save_trades', True)),
        save_positions=args.save_positions or bool(output_cfg.get('save_positions', True)),
        save_daily=args.save_daily or bool(output_cfg.get('save_daily', True)),
    )

    summary = result['summary']
    print(
        'Backtest run complete '
        f"(db_path={db_path}, run_name={run_name}, rebalances={summary.get('rebalances')}, "
        f"final_equity={summary.get('final_equity')}, output_dir={run_dir})"
    )
    if args.verbose:
        print(summary)


if __name__ == '__main__':
    main()
