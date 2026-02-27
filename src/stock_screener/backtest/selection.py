from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
import math
from typing import Any

import pandas as pd


def _cfg_to_dict(selection_cfg: Any) -> dict[str, Any]:
    if isinstance(selection_cfg, Mapping):
        return dict(selection_cfg)
    if is_dataclass(selection_cfg):
        return asdict(selection_cfg)
    raise TypeError(f"Unsupported selection cfg type: {type(selection_cfg)!r}")


def _resolve_threshold_count(cfg: dict[str, Any], *, n_universe: int, rank_key: str, pct_key: str, default: int) -> int:
    rank_raw = cfg.get(rank_key)
    if rank_raw is not None:
        return max(0, int(rank_raw))

    pct_raw = cfg.get(pct_key)
    if pct_raw is not None:
        pct = float(pct_raw)
        if pct > 1.0:
            pct = pct / 100.0
        return max(0, int(math.ceil(n_universe * pct)))

    return max(0, int(default))


def _select_with_buffer(
    ranked: pd.DataFrame,
    cfg: dict[str, Any],
    *,
    current_holdings: set[str],
    cap_n: int,
    sort_by: str,
    ascending: bool,
) -> tuple[list[str], dict[str, Any]]:
    ranked = ranked.dropna(subset=["ticker"]).copy()
    tickers = [str(t) for t in ranked["ticker"].tolist()]
    n_universe = len(tickers)

    entry_count = _resolve_threshold_count(cfg, n_universe=n_universe, rank_key="entry_rank", pct_key="entry_pct", default=cap_n)
    exit_count = _resolve_threshold_count(cfg, n_universe=n_universe, rank_key="exit_rank", pct_key="exit_pct", default=cap_n)
    entry_count = min(entry_count, n_universe)
    exit_count = min(exit_count, n_universe)

    if entry_count == 0 and exit_count == 0:
        return tickers[:cap_n], {
            "buffer_enabled": False,
            "entry_count": 0,
            "exit_count": 0,
            "entry_added": 0,
            "exit_removed": 0,
        }

    def _eligible(count: int) -> set[str]:
        if count <= 0:
            return set()
        if count >= n_universe:
            return set(tickers)
        cutoff_val = ranked.iloc[count - 1][sort_by]
        if ascending:
            allowed = ranked[ranked[sort_by] <= cutoff_val]
        else:
            allowed = ranked[ranked[sort_by] >= cutoff_val]
        return {str(t) for t in allowed["ticker"].tolist()}

    entry_eligible = _eligible(entry_count)
    exit_eligible = _eligible(exit_count)

    selected: list[str] = []
    for ticker in tickers:
        if ticker in current_holdings:
            if ticker in exit_eligible:
                selected.append(ticker)
        elif ticker in entry_eligible:
            selected.append(ticker)

    entry_added = len([t for t in selected if t not in current_holdings])
    exit_removed = len([t for t in current_holdings if t not in selected])
    return selected, {
        "buffer_enabled": True,
        "entry_count": entry_count,
        "exit_count": exit_count,
        "entry_added": entry_added,
        "exit_removed": exit_removed,
    }


def select_tickers(df: pd.DataFrame, selection_cfg: Any, *, current_holdings: set[str] | None = None) -> tuple[list[str], dict[str, Any]]:
    cfg = _cfg_to_dict(selection_cfg)
    mode = str(cfg.get("mode", "all"))
    min_holdings = int(cfg.get("min_holdings", 0) or 0)
    holdings = {str(t) for t in (current_holdings or set())}
    buffer_meta = {
        "buffer_enabled": False,
        "entry_count": 0,
        "exit_count": 0,
        "entry_added": 0,
        "exit_removed": 0,
    }

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
        has_buffer_cfg = any(cfg.get(key) is not None for key in ("entry_rank", "exit_rank", "entry_pct", "exit_pct"))
        if has_buffer_cfg:
            selected, buffer_meta = _select_with_buffer(
                ranked,
                cfg,
                current_holdings=holdings,
                cap_n=cap_n,
                sort_by=sort_by,
                ascending=ascending,
            )
        else:
            selected = [str(t) for t in ranked["ticker"].head(cap_n).tolist()]

        if len(selected) < min_holdings:
            top_tickers = [str(t) for t in ranked["ticker"].tolist()]
            for ticker in top_tickers:
                if ticker in selected:
                    continue
                selected.append(ticker)
                if len(selected) >= min_holdings:
                    break
    else:
        raise ValueError(f"Unsupported selection mode: {mode}")

    metadata = {
        "count": len(selected),
        "below_min_holdings": len(selected) < min_holdings,
        "min_holdings": min_holdings,
        "mode": mode,
    }
    metadata.update(buffer_meta)
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
