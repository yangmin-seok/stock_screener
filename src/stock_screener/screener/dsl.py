from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass
class FilterCondition:
    field: str
    op: str
    value: object


def apply_filters(frame: pd.DataFrame, conditions: list[FilterCondition]) -> pd.DataFrame:
    out = frame.copy()
    for cond in conditions:
        if cond.field not in out.columns:
            raise ValueError(f"Unknown field: {cond.field}")
        series = out[cond.field]
        match cond.op:
            case "gte":
                out = out[series >= cond.value]
            case "lte":
                out = out[series <= cond.value]
            case "eq":
                out = out[series == cond.value]
            case "in":
                out = out[series.isin(cond.value)]
            case "range":
                lo, hi = cond.value
                out = out[(series >= lo) & (series <= hi)]
            case "bool":
                out = out[series.astype(bool) == bool(cond.value)]
            case _:
                raise ValueError(f"Unsupported op: {cond.op}")
    return out


def preset_conditions(name: str) -> list[FilterCondition]:
    presets: dict[str, list[FilterCondition]] = {
        "deep_value": [
            FilterCondition("pbr", "lte", 0.8),
            FilterCondition("eps_positive", "eq", 1),
            FilterCondition("avg_value_20d", "gte", 500_000_000),
        ],
        "rerating": [
            FilterCondition("pbr", "lte", 1.0),
            FilterCondition("pos_52w", "lte", 0.25),
            FilterCondition("roe_proxy", "gte", 0.10),
        ],
        "dividend_lowvol": [
            FilterCondition("div", "gte", 3.0),
            FilterCondition("vol_20d", "lte", 0.025),
            FilterCondition("avg_value_20d", "gte", 300_000_000),
        ],
        "momentum": [
            FilterCondition("dist_sma200", "gte", 0.0),
            FilterCondition("ret_3m", "gte", 0.0),
            FilterCondition("avg_value_20d", "gte", 500_000_000),
        ],
        "eps_growth_breakout": [
            FilterCondition("eps_cagr_5y", "gte", 0.15),
            FilterCondition("eps_yoy_q", "gte", 0.25),
            FilterCondition("near_52w_high_ratio", "gte", 0.90),
        ],
    }
    if name not in presets:
        raise ValueError(f"Unknown preset: {name}")
    return presets[name]
