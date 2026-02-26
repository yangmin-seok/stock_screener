from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any

import pandas as pd

from stock_screener.backtest.calendar import make_rebalance_signal_dates, next_trading_day
from stock_screener.backtest.filters import apply_filters
from stock_screener.backtest.portfolio import PortfolioState, mark_to_market_close, rebalance_at_open
from stock_screener.backtest.selection import build_target_weights, select_tickers


def _cfg_section(section: Any) -> dict[str, Any]:
    if section is None:
        return {}
    if isinstance(section, dict):
        return section
    if is_dataclass(section):
        return asdict(section)
    return dict(section)


def _pick_run_value(run_cfg: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in run_cfg:
            return run_cfg[key]
    return default


def run_backtest(config: Any, repo: Any) -> dict[str, pd.DataFrame | dict[str, Any]]:
    run_cfg = _cfg_section(getattr(config, 'run', {}))
    filters_cfg = _cfg_section(getattr(config, 'filters', {}))
    selection_cfg = _cfg_section(getattr(config, 'selection', {}))
    portfolio_cfg = _cfg_section(getattr(config, 'portfolio', {}))
    costs_cfg = _cfg_section(getattr(config, 'costs', {}))

    start = _pick_run_value(run_cfg, 'start', 'start_date')
    end = _pick_run_value(run_cfg, 'end', 'end_date')
    initial_capital = float(_pick_run_value(run_cfg, 'initial_capital', default=1_000_000.0))
    if not start or not end:
        raise ValueError('run.start/start_date and run.end/end_date are required')

    trading_dates_all = [d for d in repo.get_trading_dates() if start <= d <= end]
    if len(trading_dates_all) < 2:
        return {
            'equity_curve': pd.DataFrame(columns=['date', 'equity_close', 'return']),
            'rebalance_log': pd.DataFrame(columns=['signal_date', 'exec_date', 'selected_count', 'turnover_notional', 'costs']),
            'positions': pd.DataFrame(columns=['date', 'ticker', 'weight', 'shares', 'open_price']),
            'trades': pd.DataFrame(columns=['exec_date', 'ticker', 'delta_shares', 'price_open', 'notional', 'cost']),
            'summary': {'rebalances': 0, 'final_equity': initial_capital, 'turnover': 0.0, 'number_of_trades': 0, 'total_costs': 0.0},
        }

    rule = _pick_run_value(run_cfg, 'rebalance', 'rebalance_rule', default='M')
    anchor = _pick_run_value(run_cfg, 'anchor', default='month_end')
    signal_dates = make_rebalance_signal_dates(trading_dates_all, rule=rule, anchor=anchor)

    state = PortfolioState(cash=initial_capital)
    fee_bps = float(costs_cfg.get('fee_bps', costs_cfg.get('commission_bps', 0.0)) or 0.0)
    slippage_bps = float(costs_cfg.get('slippage_bps', 0.0) or 0.0)

    equity_records: list[dict[str, Any]] = []
    rebalance_records: list[dict[str, Any]] = []
    positions_records: list[dict[str, Any]] = []
    trades_records: list[dict[str, Any]] = []

    for signal_date in signal_dates:
        exec_date = next_trading_day(trading_dates_all, signal_date)
        if not exec_date:
            continue

        frame = repo.get_asof_frame(signal_date, foreign_window=int(filters_cfg.get('foreign_window', 20)))
        filtered, diagnostics = apply_filters(frame, filters_cfg)
        selected, selection_meta = select_tickers(filtered, selection_cfg)

        empty_policy = str(selection_cfg.get('empty_selection_policy', 'cash'))
        if not selected and empty_policy == 'keep_prev':
            selected = sorted(state.shares.keys())

        weights = build_target_weights(
            selected,
            weighting=str(portfolio_cfg.get('weighting', 'equal')),
            max_weight=portfolio_cfg.get('max_weight'),
        )

        next_exec = next_trading_day(trading_dates_all, exec_date)
        if next_exec and next_exec in trading_dates_all:
            next_idx = trading_dates_all.index(next_exec)
            segment_end = trading_dates_all[next_idx - 1] if next_idx > 0 else exec_date
        else:
            segment_end = trading_dates_all[-1]

        held_tickers = sorted(set(state.shares) | set(selected))
        panel = repo.get_price_panel(held_tickers, exec_date, segment_end)
        open_slice = panel[panel['date'] == exec_date]
        rebalance_metrics = rebalance_at_open(
            state,
            open_slice,
            weights,
            fee_bps=fee_bps,
            slippage_bps=slippage_bps,
        )

        open_map = {
            str(row['ticker']): float(row['open'])
            for _, row in open_slice.dropna(subset=['open']).iterrows()
        }
        equity_open = float(rebalance_metrics['equity_open']) if rebalance_metrics.get('equity_open') else 0.0
        for ticker, shares in state.shares.items():
            open_px = open_map.get(ticker)
            if open_px is None:
                continue
            weight = (shares * open_px / equity_open) if equity_open > 0 else 0.0
            positions_records.append(
                {
                    'date': exec_date,
                    'ticker': ticker,
                    'weight': float(weight),
                    'shares': float(shares),
                    'open_price': float(open_px),
                }
            )

        for trade_row in rebalance_metrics.get('trade_rows', []):
            trades_records.append({'exec_date': exec_date, **trade_row})

        for dt in sorted(panel['date'].unique().tolist()):
            close_slice = panel[panel['date'] == dt]
            equity_records.append(mark_to_market_close(state, close_slice, dt))

        rebalance_records.append(
            {
                'signal_date': signal_date,
                'exec_date': exec_date,
                'selected_count': len(selected),
                'selected_tickers': ','.join(selected),
                'diagnostics': str(diagnostics),
                'below_min_holdings': bool(selection_meta.get('below_min_holdings', False)),
                'turnover_notional': rebalance_metrics['turnover_notional'],
                'costs': rebalance_metrics['costs'],
            }
        )

    if not equity_records:
        equity_curve = pd.DataFrame(columns=['date', 'equity_close', 'return'])
    else:
        equity_curve = pd.DataFrame(equity_records).drop_duplicates(subset=['date'], keep='last').sort_values('date')
        equity_curve['return'] = equity_curve['equity_close'].pct_change().fillna(0.0)

    rebalance_log = pd.DataFrame(rebalance_records)
    trades = pd.DataFrame(trades_records)

    final_equity = float(state.cash) if state.cash is not None else float(initial_capital)
    if not equity_curve.empty:
        final_equity = float(equity_curve['equity_close'].iloc[-1])

    summary = {
        'rebalances': len(rebalance_records),
        'final_equity': final_equity,
        'total_costs': float(rebalance_log['costs'].sum()) if not rebalance_log.empty else 0.0,
        'turnover': float(rebalance_log['turnover_notional'].sum()) if not rebalance_log.empty else 0.0,
        'number_of_trades': int(len(trades)),
    }

    return {
        'equity_curve': equity_curve,
        'rebalance_log': rebalance_log,
        'positions': pd.DataFrame(positions_records),
        'trades': trades,
        'summary': summary,
    }
