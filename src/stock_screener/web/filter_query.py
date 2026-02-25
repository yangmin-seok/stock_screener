from __future__ import annotations

from typing import Any, Mapping


def prune_query_filter_state(query_filter_state: dict[str, Any], state: Mapping[str, Any]) -> dict[str, Any]:
    """Drop query keys that are incompatible with current filter modes."""
    pruned = dict(query_filter_state)

    for prefix in ("mcap", "price", "div", "value", "relvol", "momentum", "ev_ebitda"):
        mode = state.get(f"{prefix}_filter_mode", "Any")
        if mode != "직접 입력":
            pruned.pop(f"{prefix}_min_custom", None)
            pruned.pop(f"{prefix}_max_custom", None)
        if mode != "구간 선택":
            pruned.pop(f"{prefix}_bucket", None)

    if state.get("momentum_filter_mode", "Any") == "Any":
        pruned.pop("momentum_metric", None)

    for prefix in ("rsi", "atr", "gap", "chg_open", "volatility", "foreign_buy"):
        mode = state.get(f"{prefix}_filter_mode", "Any")
        if mode != "직접 입력":
            pruned.pop(f"{prefix}_min_custom", None)
            pruned.pop(f"{prefix}_max_custom", None)
        if mode != "구간 선택":
            pruned.pop(f"{prefix}_bucket", None)


    for key in ("foreign_buy2_filter_mode", "foreign_buy2_metric", "foreign_buy2_bucket", "foreign_buy2_min_custom", "foreign_buy2_max_custom"):
        pruned.pop(key, None)

    for prefix in ("dist_sma20", "dist_sma50", "dist_sma200", "near_high", "near_low"):
        mode = state.get(f"{prefix}_filter_mode", "Any")
        if mode != "직접 입력":
            pruned.pop(f"{prefix}_min_custom", None)
            pruned.pop(f"{prefix}_max_custom", None)
        if mode != "구간 선택":
            pruned.pop(f"{prefix}_bucket", None)

    return pruned
