from __future__ import annotations

from datetime import datetime
from typing import Literal


def _month_key(date_text: str) -> tuple[int, int]:
    dt = datetime.strptime(date_text, "%Y-%m-%d")
    return dt.year, dt.month


def make_rebalance_signal_dates(
    trading_dates: list[str],
    rule: str,
    anchor: Literal["month_end", "month_start"] = "month_end",
) -> list[str]:
    if not trading_dates:
        return []

    rule_norm = rule.strip().upper()
    if rule_norm in {"W", "WEEKLY", "WEEK"}:
        return trading_dates[::5]

    if rule_norm in {"M", "MONTHLY", "MONTH"}:
        grouped: dict[tuple[int, int], list[str]] = {}
        for dt in trading_dates:
            grouped.setdefault(_month_key(dt), []).append(dt)
        out: list[str] = []
        for _, dates in sorted(grouped.items()):
            out.append(dates[0] if anchor == "month_start" else dates[-1])
        return out

    raise ValueError(f"Unsupported rebalance rule: {rule}")


def next_trading_day(trading_dates: list[str], dt: str) -> str | None:
    for idx, date in enumerate(trading_dates):
        if date == dt:
            return trading_dates[idx + 1] if idx + 1 < len(trading_dates) else None
    return None
