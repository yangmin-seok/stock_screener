from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict
from itertools import product
from typing import Any

import pandas as pd

from stock_screener.backtest.config import BacktestConfig
from stock_screener.backtest.engine import run_backtest
from stock_screener.backtest.report import compute_summary


def _config_to_dict(config: BacktestConfig) -> dict[str, Any]:
    return asdict(config)


def _make_config(config_dict: dict[str, Any]) -> BacktestConfig:
    return BacktestConfig(
        run=config_dict["run"],
        universe=config_dict["universe"],
        filters=config_dict["filters"],
        selection=config_dict["selection"],
        portfolio=config_dict["portfolio"],
        costs=config_dict["costs"],
        output=config_dict["output"],
    )


def _split_dates(trading_dates: list[str], train_ratio: float | None, split_date: str | None) -> tuple[str, str] | None:
    if not trading_dates:
        return None
    if split_date:
        if split_date <= trading_dates[0] or split_date > trading_dates[-1]:
            return None
        return trading_dates[0], split_date
    if train_ratio is None:
        return None
    if train_ratio <= 0.0 or train_ratio >= 1.0:
        return None
    split_idx = int(len(trading_dates) * train_ratio)
    split_idx = max(1, min(split_idx, len(trading_dates) - 1))
    return trading_dates[0], trading_dates[split_idx - 1]


def _next_date(trading_dates: list[str], date: str) -> str | None:
    for dt in trading_dates:
        if dt > date:
            return dt
    return None


def _run_once(base_config: dict[str, Any], repo: Any, rebalance: str, foreign_window: int, cap_n: int, start_date: str, end_date: str) -> dict[str, Any]:
    cfg_dict = deepcopy(base_config)
    cfg_dict["run"]["rebalance"] = rebalance
    cfg_dict["run"]["start_date"] = start_date
    cfg_dict["run"]["end_date"] = end_date
    cfg_dict["filters"]["foreign_window"] = int(foreign_window)
    cfg_dict["selection"]["cap_n"] = int(cap_n)

    cfg = _make_config(cfg_dict)
    result = run_backtest(cfg, repo)
    initial_capital = float(cfg.run.get("initial_capital", 1_000_000))
    return compute_summary(result, initial_capital=initial_capital)


def run_parameter_sweep(
    config: BacktestConfig,
    repo: Any,
    *,
    rebalance_values: list[str],
    foreign_window_values: list[int],
    cap_n_values: list[int],
    train_ratio: float | None = None,
    split_date: str | None = None,
) -> pd.DataFrame:
    config_dict = _config_to_dict(config)
    start_date = str(config_dict["run"].get("start_date") or config_dict["run"].get("start"))
    end_date = str(config_dict["run"].get("end_date") or config_dict["run"].get("end"))
    trading_dates = [d for d in repo.get_trading_dates() if start_date <= d <= end_date]

    in_sample = _split_dates(trading_dates, train_ratio=train_ratio, split_date=split_date)
    out_sample: tuple[str, str] | None = None
    if in_sample is not None:
        out_start = _next_date(trading_dates, in_sample[1])
        if out_start is not None:
            out_sample = (out_start, trading_dates[-1])

    rows: list[dict[str, Any]] = []
    for rebalance, foreign_window, cap_n in product(rebalance_values, foreign_window_values, cap_n_values):
        full_summary = _run_once(
            config_dict,
            repo,
            rebalance=rebalance,
            foreign_window=foreign_window,
            cap_n=cap_n,
            start_date=start_date,
            end_date=end_date,
        )
        row = {
            "rebalance": rebalance,
            "foreign_window": int(foreign_window),
            "cap_n": int(cap_n),
            "final_equity": float(full_summary.get("final_equity", 0.0)),
            "cagr": float(full_summary.get("cagr", 0.0)),
            "mdd": float(full_summary.get("mdd", 0.0)),
            "turnover": float(full_summary.get("turnover", 0.0)),
            "total_costs": float(full_summary.get("total_costs", 0.0)),
            "is_start_date": None,
            "is_end_date": None,
            "is_final_equity": None,
            "is_cagr": None,
            "is_mdd": None,
            "is_turnover": None,
            "is_total_costs": None,
            "oos_start_date": None,
            "oos_end_date": None,
            "oos_final_equity": None,
            "oos_cagr": None,
            "oos_mdd": None,
            "oos_turnover": None,
            "oos_total_costs": None,
        }
        if in_sample is not None:
            is_summary = _run_once(
                config_dict,
                repo,
                rebalance=rebalance,
                foreign_window=foreign_window,
                cap_n=cap_n,
                start_date=in_sample[0],
                end_date=in_sample[1],
            )
            row.update(
                {
                    "is_start_date": in_sample[0],
                    "is_end_date": in_sample[1],
                    "is_final_equity": float(is_summary.get("final_equity", 0.0)),
                    "is_cagr": float(is_summary.get("cagr", 0.0)),
                    "is_mdd": float(is_summary.get("mdd", 0.0)),
                    "is_turnover": float(is_summary.get("turnover", 0.0)),
                    "is_total_costs": float(is_summary.get("total_costs", 0.0)),
                }
            )
        if out_sample is not None and out_sample[0] <= out_sample[1]:
            oos_summary = _run_once(
                config_dict,
                repo,
                rebalance=rebalance,
                foreign_window=foreign_window,
                cap_n=cap_n,
                start_date=out_sample[0],
                end_date=out_sample[1],
            )
            row.update(
                {
                    "oos_start_date": out_sample[0],
                    "oos_end_date": out_sample[1],
                    "oos_final_equity": float(oos_summary.get("final_equity", 0.0)),
                    "oos_cagr": float(oos_summary.get("cagr", 0.0)),
                    "oos_mdd": float(oos_summary.get("mdd", 0.0)),
                    "oos_turnover": float(oos_summary.get("turnover", 0.0)),
                    "oos_total_costs": float(oos_summary.get("total_costs", 0.0)),
                }
            )
        rows.append(row)

    return pd.DataFrame(rows)

