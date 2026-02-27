from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, is_dataclass
from typing import Any

import pandas as pd


def _spec_to_dict(spec: Any) -> dict[str, Any]:
    if isinstance(spec, Mapping):
        return dict(spec)
    if is_dataclass(spec):
        return asdict(spec)
    raise TypeError(f"Unsupported filter spec type: {type(spec)!r}")


def _resolve_foreign_column(spec: dict[str, Any]) -> str:
    unit = str(spec.get("unit", "value"))
    normalize = str(spec.get("normalize", "none"))

    if unit == "value" and normalize == "none":
        return "foreign_cum_value_20d"
    if unit == "volume" and normalize == "none":
        return "foreign_cum_volume_20d"
    if unit == "value" and normalize == "by_avg_value":
        return "foreign_pressure_by_avg_value"
    if unit == "value" and normalize == "by_mcap":
        return "foreign_pressure_by_mcap"

    raise ValueError(
        "Unsupported foreign filter mapping "
        f"(unit={unit!r}, normalize={normalize!r})"
    )


def _column_for_filter(filter_name: str, spec: dict[str, Any]) -> str:
    field = str(spec.get("field") or filter_name)
    if field == "foreign_cum":
        return _resolve_foreign_column(spec)
    return field


def _build_condition_text(column: str, spec: dict[str, Any]) -> str:
    op = str(spec.get("op", "eq"))
    if op == "range":
        return (
            f"{column} in [{spec.get('min')}, {spec.get('max')}]"
            if bool(spec.get("inclusive", True))
            else f"{column} in ({spec.get('min')}, {spec.get('max')})"
        )
    if op in {"gte", "lte", "eq"}:
        return f"{column} {op} {spec.get('value')}"
    if op == "in":
        return f"{column} in {spec.get('values', [])}"
    return f"{column} ({op})"


def _apply_condition(series: pd.Series, spec: dict[str, Any]) -> pd.Series:
    op = str(spec.get("op", "eq"))
    inclusive = bool(spec.get("inclusive", True))

    if op == "range":
        min_v = spec.get("min")
        max_v = spec.get("max")
        if min_v is None or max_v is None:
            raise ValueError("range filter requires both min and max")
        if inclusive:
            return (series >= min_v) & (series <= max_v)
        return (series > min_v) & (series < max_v)

    if op == "gte":
        return series >= spec.get("value", spec.get("min"))
    if op == "lte":
        return series <= spec.get("value", spec.get("max"))
    if op == "eq":
        return series == spec.get("value")
    if op == "in":
        return series.isin(spec.get("values", []))

    raise ValueError(f"Unsupported op: {op}")


def _validate_required_columns(df: pd.DataFrame, filters: Mapping[str, Any]) -> None:
    missing_messages: list[str] = []

    for filter_name, raw_spec in filters.items():
        if not isinstance(raw_spec, Mapping) and not is_dataclass(raw_spec):
            continue
        spec = _spec_to_dict(raw_spec)
        if not bool(spec.get("enabled", False)):
            continue

        field = str(spec.get("field") or filter_name)
        if field != "foreign_cum":
            continue

        expected_column = _resolve_foreign_column(spec)
        if expected_column in df.columns:
            continue

        available_foreign_cols = sorted(c for c in df.columns if c.startswith("foreign_"))
        missing_messages.append(
            "외국인 필터 컬럼이 데이터프레임에 없습니다: "
            f"filter={filter_name}, normalize={spec.get('normalize', 'none')}, expected={expected_column}, "
            f"available_foreign_columns={available_foreign_cols}"
        )

    if missing_messages:
        raise KeyError("; ".join(missing_messages))


def apply_filters(
    df: pd.DataFrame,
    filters: Mapping[str, Any],
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    _validate_required_columns(df, filters)
    filtered = df.copy()
    diagnostics: list[dict[str, Any]] = []

    for filter_name, raw_spec in filters.items():
        if not isinstance(raw_spec, Mapping) and not is_dataclass(raw_spec):
            continue
        spec = _spec_to_dict(raw_spec)
        if not bool(spec.get("enabled", False)):
            continue

        column = _column_for_filter(filter_name, spec)
        if column not in filtered.columns:
            raise KeyError(f"Filter column not found: {column}")

        working = filtered.copy()

        if filter_name == "per":
            if bool(spec.get("drop_nonpositive", False)):
                working = working[working[column].notna() & (working[column] > 0)]
            clip_min = spec.get("clip_min")
            clip_max = spec.get("clip_max")
            if clip_min is not None or clip_max is not None:
                working.loc[:, column] = working[column].clip(lower=clip_min, upper=clip_max)

        before_count = int(len(working))
        is_na = working[column].isna()
        dropped_nan = int(is_na.sum()) if str(spec.get("missing_policy", "drop")) == "drop" else 0

        condition = _apply_condition(working[column], spec)
        if str(spec.get("missing_policy", "drop")) == "drop":
            pass_mask = (~is_na) & condition
        elif str(spec.get("missing_policy", "drop")) == "ignore":
            pass_mask = is_na | condition
        else:
            raise ValueError(f"Unsupported missing_policy: {spec.get('missing_policy')}")

        filtered = working[pass_mask].copy()
        after_count = int(len(filtered))

        diagnostics.append(
            {
                "filter": filter_name,
                "field": column,
                "before_count": before_count,
                "after_count": after_count,
                "dropped_nan": dropped_nan,
                "condition": _build_condition_text(column, spec),
            }
        )

    return filtered, diagnostics
