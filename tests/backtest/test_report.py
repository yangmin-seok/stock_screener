from __future__ import annotations

import json

import pandas as pd

from stock_screener.backtest.report import compute_summary, save_backtest_outputs


def _sample_result() -> dict:
    return {
        'equity_curve': pd.DataFrame(
            [
                {'date': '2025-01-02', 'equity_close': 1_000_000.0, 'return': 0.0},
                {'date': '2025-01-03', 'equity_close': 1_010_000.0, 'return': 0.01},
            ]
        ),
        'rebalance_log': pd.DataFrame(
            [{'signal_date': '2025-01-02', 'exec_date': '2025-01-03', 'turnover_notional': 100_000.0, 'costs': 100.0}]
        ),
        'positions': pd.DataFrame([{'date': '2025-01-03', 'ticker': 'AAA', 'weight': 1.0, 'shares': 100, 'open_price': 100.0}]),
        'trades': pd.DataFrame([{'exec_date': '2025-01-03', 'ticker': 'AAA', 'delta_shares': 100.0, 'price_open': 100.0, 'notional': 10_000.0, 'cost': 10.0}]),
    }


def test_compute_summary_basic_metrics():
    summary = compute_summary(_sample_result(), initial_capital=1_000_000.0)

    assert summary['final_equity'] == 1_010_000.0
    assert summary['number_of_trades'] == 1
    assert summary['turnover'] == 100_000.0
    assert summary['total_costs'] == 100.0


def test_save_backtest_outputs_writes_expected_files(tmp_path):
    config = {
        'run': {'name': 'unit', 'initial_capital': 1_000_000.0},
        'output': {'dir': str(tmp_path), 'save_trades': True, 'save_positions': True, 'save_daily': True},
    }
    result = _sample_result()
    result['summary'] = compute_summary(result, initial_capital=1_000_000.0)

    run_dir = save_backtest_outputs(
        config_dict=config,
        result=result,
        out_dir=tmp_path,
        run_name='unit',
        save_trades=True,
        save_positions=True,
        save_daily=True,
    )

    assert (run_dir / 'config_used.yaml').exists()
    assert (run_dir / 'equity_curve.csv').exists()
    assert (run_dir / 'rebalance_log.csv').exists()
    assert (run_dir / 'positions.csv').exists()
    assert (run_dir / 'trades.csv').exists()
    assert (run_dir / 'summary.json').exists()

    summary = json.loads((run_dir / 'summary.json').read_text(encoding='utf-8'))
    assert summary['final_equity'] == 1_010_000.0
