from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

WINSORIZE_LIMITS: tuple[float, float] = (-5.0, 5.0)


@dataclass(frozen=True)
class GrowthResult:
    value: float | None
    window_years: int | None
    asof: str
    sample_count: int


def _to_numeric_series(frame: pd.DataFrame, value_col: str) -> pd.Series:
    if frame.empty:
        return pd.Series(dtype=float)
    ordered = frame[["date", value_col]].copy()
    ordered["date"] = pd.to_datetime(ordered["date"])
    ordered[value_col] = pd.to_numeric(ordered[value_col], errors="coerce")
    ordered = ordered.dropna(subset=[value_col]).sort_values("date")
    if ordered.empty:
        return pd.Series(dtype=float)
    return pd.Series(ordered[value_col].values, index=ordered["date"])


def _nearest_on_or_before(series: pd.Series, target: pd.Timestamp) -> float:
    subset = series[series.index <= target]
    if subset.empty:
        return np.nan
    return subset.iloc[-1]


def _normalize_growth(value: float) -> float | None:
    if pd.isna(value) or not np.isfinite(value):
        return None
    low, high = WINSORIZE_LIMITS
    return float(np.clip(value, low, high))


def calc_cagr(series: pd.Series, asof: str, window_years: int) -> GrowthResult:
    """Sample requirements: at least window_years + 1 annual points and positive start/end values."""
    asof_ts = pd.Timestamp(asof)
    valid = series[series.index <= asof_ts]
    sample_count = int(valid.shape[0])
    min_samples = window_years + 1
    if sample_count < min_samples:
        return GrowthResult(None, window_years, asof, sample_count)

    end = _nearest_on_or_before(valid, asof_ts)
    start = _nearest_on_or_before(valid, asof_ts - pd.DateOffset(years=window_years))
    if pd.isna(end) or pd.isna(start) or end <= 0 or start <= 0:
        return GrowthResult(None, window_years, asof, sample_count)

    value = (end / start) ** (1 / window_years) - 1
    return GrowthResult(_normalize_growth(value), window_years, asof, sample_count)


def calc_yoy(series: pd.Series, asof: str) -> GrowthResult:
    """Sample requirements: at least 2 points; prior-year denominator must be non-zero."""
    asof_ts = pd.Timestamp(asof)
    valid = series[series.index <= asof_ts]
    sample_count = int(valid.shape[0])
    if sample_count < 2:
        return GrowthResult(None, 1, asof, sample_count)

    latest_dt = valid.index[-1]
    current = _nearest_on_or_before(valid, latest_dt)
    prev = _nearest_on_or_before(valid, latest_dt - pd.DateOffset(years=1))
    if pd.isna(current) or pd.isna(prev) or prev == 0:
        return GrowthResult(None, 1, asof, sample_count)

    return GrowthResult(_normalize_growth(current / prev - 1), 1, asof, sample_count)


def calc_qoq(series: pd.Series, asof: str) -> GrowthResult:
    """Sample requirements: at least 2 quarterly points; prior-quarter denominator must be non-zero."""
    asof_ts = pd.Timestamp(asof)
    valid = series[series.index <= asof_ts]
    sample_count = int(valid.shape[0])
    if sample_count < 2:
        return GrowthResult(None, None, asof, sample_count)

    current = valid.iloc[-1]
    prev = valid.iloc[-2]
    if pd.isna(current) or pd.isna(prev) or prev == 0:
        return GrowthResult(None, None, asof, sample_count)

    days = (valid.index[-1] - valid.index[-2]).days
    window_years = 1 if days >= 365 else 0
    return GrowthResult(_normalize_growth(current / prev - 1), window_years, asof, sample_count)


def calc_ttm_growth(series: pd.Series, asof: str) -> GrowthResult:
    """Sample requirements: >=8 quarterly points (4 current + 4 prior); prior TTM denominator must be non-zero."""
    asof_ts = pd.Timestamp(asof)
    valid = series[series.index <= asof_ts]
    sample_count = int(valid.shape[0])
    if sample_count < 8:
        return GrowthResult(None, 1, asof, sample_count)

    current_ttm = valid.iloc[-4:].sum()
    prev_ttm = valid.iloc[-8:-4].sum()
    if pd.isna(current_ttm) or pd.isna(prev_ttm) or prev_ttm == 0:
        return GrowthResult(None, 1, asof, sample_count)

    return GrowthResult(_normalize_growth(current_ttm / prev_ttm - 1), 1, asof, sample_count)


FINVIZ_GROWTH_FORMULAS = {
    "eps_growth_past_5y": lambda series, asof: calc_cagr(series, asof=asof, window_years=5),
    "sales_growth_qtr_over_qtr": lambda series, asof: calc_qoq(series, asof=asof),
    "eps_growth_ttm": lambda series, asof: calc_ttm_growth(series, asof=asof),
    "eps_growth_this_year_over_year": lambda series, asof: calc_yoy(series, asof=asof),
}


def compute_growth_bundle(fund_hist: pd.DataFrame, ticker: str, asof: str) -> dict[str, GrowthResult]:
    subset = fund_hist[fund_hist["ticker"] == ticker].copy()
    if subset.empty:
        return {
            "eps_growth_past_5y": GrowthResult(None, 5, asof, 0),
            "sales_growth_qtr_over_qtr": GrowthResult(None, None, asof, 0),
            "eps_growth_ttm": GrowthResult(None, 1, asof, 0),
            "eps_growth_this_year_over_year": GrowthResult(None, 1, asof, 0),
        }

    eps_series = _to_numeric_series(subset, "eps")
    rev_series = _to_numeric_series(subset, "revenue")

    return {
        "eps_growth_past_5y": FINVIZ_GROWTH_FORMULAS["eps_growth_past_5y"](eps_series, asof),
        "sales_growth_qtr_over_qtr": FINVIZ_GROWTH_FORMULAS["sales_growth_qtr_over_qtr"](rev_series, asof),
        "eps_growth_ttm": FINVIZ_GROWTH_FORMULAS["eps_growth_ttm"](eps_series, asof),
        "eps_growth_this_year_over_year": FINVIZ_GROWTH_FORMULAS["eps_growth_this_year_over_year"](eps_series, asof),
    }
