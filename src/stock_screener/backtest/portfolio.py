from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd


@dataclass(slots=True)
class PortfolioState:
    cash: float
    shares: dict[str, float] = field(default_factory=dict)


def _price_map(frame: pd.DataFrame, column: str) -> dict[str, float]:
    valid = frame.dropna(subset=[column])[['ticker', column]]
    return {str(row['ticker']): float(row[column]) for _, row in valid.iterrows()}


def rebalance_at_open(
    state: PortfolioState,
    open_prices: pd.DataFrame,
    target_weights: dict[str, float],
    *,
    fee_bps: float = 0.0,
    slippage_bps: float = 0.0,
) -> dict[str, float | list[dict[str, float | str]]]:
    open_map = _price_map(open_prices, 'open')
    tradable_targets = {t: w for t, w in target_weights.items() if t in open_map}

    if tradable_targets:
        total = sum(tradable_targets.values())
        tradable_targets = {k: v / total for k, v in tradable_targets.items()}

    equity_open = state.cash + sum(state.shares.get(t, 0.0) * px for t, px in open_map.items())

    current_tickers = set(state.shares)
    target_tickers = set(tradable_targets)
    all_tickers = current_tickers | target_tickers

    turnover_notional = 0.0
    staged_shares = dict(state.shares)
    trade_rows: list[dict[str, float | str]] = []

    for ticker in all_tickers:
        px = open_map.get(ticker)
        if px is None or px <= 0:
            continue

        current_shares = float(state.shares.get(ticker, 0.0))
        target_value = equity_open * tradable_targets.get(ticker, 0.0)
        target_shares = target_value / px
        delta = target_shares - current_shares
        notional = abs(delta) * px
        turnover_notional += notional
        staged_shares[ticker] = target_shares

        if abs(delta) > 0:
            trade_rows.append(
                {
                    'ticker': ticker,
                    'delta_shares': float(delta),
                    'price_open': float(px),
                    'notional': float(notional),
                }
            )

    cost_rate = (fee_bps + slippage_bps) / 10000.0
    costs = turnover_notional * cost_rate

    for row in trade_rows:
        row['cost'] = float(row['notional']) * cost_rate

    post_trade_value = sum(staged_shares.get(t, 0.0) * px for t, px in open_map.items())
    state.shares = staged_shares
    state.cash = equity_open - post_trade_value - costs

    return {
        'equity_open': equity_open,
        'turnover_notional': turnover_notional,
        'costs': costs,
        'trade_rows': trade_rows,
    }


def mark_to_market_close(state: PortfolioState, close_prices: pd.DataFrame, date: str) -> dict[str, float | str]:
    close_map = _price_map(close_prices, 'close')
    equity_close = state.cash + sum(state.shares.get(t, 0.0) * px for t, px in close_map.items())
    return {'date': date, 'equity_close': equity_close}
