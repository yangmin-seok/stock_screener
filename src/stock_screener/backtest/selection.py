from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from typing import Any

import pandas as pd


def _cfg_to_dict(selection_cfg: Any) -> dict[str, Any]:
    if isinstance(selection_cfg, Mapping):
        return dict(selection_cfg)
    if is_dataclass(selection_cfg):
        return asdict(selection_cfg)
    raise TypeError(f"Unsupported selection cfg type: {type(selection_cfg)!r}")


def select_tickers(df: pd.DataFrame, selection_cfg: Any) -> tuple[list[str], dict[str, Any]]:
    cfg = _cfg_to_dict(selection_cfg)
    mode = str(cfg.get("mode", "all"))
    min_holdings = int(cfg.get("min_holdings", 0) or 0)

    if df.empty:
        selected: list[str] = []
    elif mode == "all":
        selected = [str(t) for t in df["ticker"].dropna().tolist()]
    elif mode == "cap_n":
        cap_n = int(cfg.get("cap_n", 0) or 0)
        sort_by = str(cfg.get("sort_by", "ticker"))
        ascending = str(cfg.get("sort_direction", "desc")).lower() == "asc"
        if sort_by not in df.columns:
            raise KeyError(f"sort_by column not found: {sort_by}")
        ranked = df.sort_values(by=[sort_by, "ticker"], ascending=[ascending, True], na_position="last")
        selected = [str(t) for t in ranked["ticker"].head(cap_n).tolist()]
    else:
        raise ValueError(f"Unsupported selection mode: {mode}")

    metadata = {
        "count": len(selected),
        "below_min_holdings": len(selected) < min_holdings,
        "min_holdings": min_holdings,
        "mode": mode,
    }
    return selected, metadata


def build_target_weights(
    tickers: list[str],
    *,
    weighting: str = "equal",
    max_weight: float | None = None,
) -> dict[str, float]:
    if not tickers:
        return {}
    if weighting != "equal":
        raise ValueError(f"Unsupported weighting: {weighting}")

    n = len(tickers)
    weights = {ticker: 1.0 / n for ticker in tickers}

    if max_weight is None:
        return weights
    if max_weight <= 0:
        raise ValueError("max_weight must be positive")

    capped = {ticker: min(weight, max_weight) for ticker, weight in weights.items()}
    remainder = 1.0 - sum(capped.values())

    if remainder <= 0:
        total = sum(capped.values())
        return {k: v / total for k, v in capped.items()} if total > 0 else {}

    under_cap = [ticker for ticker, weight in capped.items() if weight < max_weight]
    if not under_cap:
        total = sum(capped.values())
        return {k: v / total for k, v in capped.items()} if total > 0 else {}

    add = remainder / len(under_cap)
    for ticker in under_cap:
        capped[ticker] += add

    total = sum(capped.values())
    return {k: v / total for k, v in capped.items()} if total > 0 else {}
