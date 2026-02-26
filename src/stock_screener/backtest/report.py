from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from typing import Any

import pandas as pd


def _serialize_yaml_value(value: Any, indent: int = 0) -> list[str]:
    pad = " " * indent
    if isinstance(value, dict):
        lines: list[str] = []
        for key, val in value.items():
            if isinstance(val, dict):
                lines.append(f"{pad}{key}:")
                lines.extend(_serialize_yaml_value(val, indent + 2))
            else:
                lines.append(f"{pad}{key}: {json.dumps(val, ensure_ascii=False)}")
        return lines
    return [f"{pad}{json.dumps(value, ensure_ascii=False)}"]


def _max_drawdown(equity: pd.Series) -> float:
    if equity.empty:
        return 0.0
    running_max = equity.cummax()
    drawdown = (equity / running_max) - 1.0
    return float(drawdown.min())


def _cagr(equity_curve: pd.DataFrame, initial_capital: float) -> float:
    if equity_curve.empty or initial_capital <= 0:
        return 0.0
    first = equity_curve["date"].iloc[0]
    last = equity_curve["date"].iloc[-1]
    days = (pd.to_datetime(last) - pd.to_datetime(first)).days
    if days <= 0:
        return 0.0
    final_equity = float(equity_curve["equity_close"].iloc[-1])
    years = days / 365.25
    if years <= 0:
        return 0.0
    return float((final_equity / initial_capital) ** (1.0 / years) - 1.0)


def compute_summary(result: dict[str, Any], *, initial_capital: float) -> dict[str, Any]:
    equity_curve = result.get("equity_curve", pd.DataFrame())
    rebalance_log = result.get("rebalance_log", pd.DataFrame())
    trades = result.get("trades", pd.DataFrame())

    final_equity = float(equity_curve["equity_close"].iloc[-1]) if not equity_curve.empty else float(initial_capital)
    total_turnover = float(rebalance_log["turnover_notional"].sum()) if not rebalance_log.empty and "turnover_notional" in rebalance_log else 0.0
    total_costs = float(rebalance_log["costs"].sum()) if not rebalance_log.empty and "costs" in rebalance_log else 0.0

    return {
        "final_equity": final_equity,
        "cagr": _cagr(equity_curve, initial_capital=initial_capital),
        "mdd": _max_drawdown(equity_curve["equity_close"]) if not equity_curve.empty else 0.0,
        "turnover": total_turnover,
        "number_of_trades": int(len(trades)) if isinstance(trades, pd.DataFrame) else 0,
        "total_costs": total_costs,
        "rebalances": int(len(rebalance_log)) if isinstance(rebalance_log, pd.DataFrame) else 0,
    }


def save_backtest_outputs(
    *,
    config_dict: dict[str, Any],
    result: dict[str, Any],
    out_dir: str | Path,
    run_name: str,
    save_trades: bool = True,
    save_positions: bool = True,
    save_daily: bool = True,
) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = Path(out_dir) / f"{run_name}_{ts}"
    run_dir.mkdir(parents=True, exist_ok=True)

    config_lines = _serialize_yaml_value(config_dict)
    (run_dir / "config_used.yaml").write_text("\n".join(config_lines) + "\n", encoding="utf-8")

    equity_curve = result.get("equity_curve", pd.DataFrame())
    if save_daily:
        if not equity_curve.empty and "return" not in equity_curve.columns:
            out = equity_curve.copy()
            out["return"] = out["equity_close"].pct_change().fillna(0.0)
            out.to_csv(run_dir / "equity_curve.csv", index=False)
        else:
            equity_curve.to_csv(run_dir / "equity_curve.csv", index=False)

    rebalance_log = result.get("rebalance_log", pd.DataFrame())
    rebalance_log.to_csv(run_dir / "rebalance_log.csv", index=False)

    if save_positions:
        positions = result.get("positions", pd.DataFrame())
        positions.to_csv(run_dir / "positions.csv", index=False)

    if save_trades:
        trades = result.get("trades", pd.DataFrame())
        trades.to_csv(run_dir / "trades.csv", index=False)

    summary = result.get("summary") or compute_summary(result, initial_capital=float(config_dict.get("run", {}).get("initial_capital", 1_000_000)))
    (run_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    return run_dir
