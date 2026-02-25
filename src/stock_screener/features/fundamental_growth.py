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
        return pd.Series(dtype=float, index=pd.DatetimeIndex([]))
    ordered = frame[["fiscal_period", "period_type", value_col]].copy()
    ordered[value_col] = pd.to_numeric(ordered[value_col], errors="coerce")
    ordered["fiscal_period_ts"] = _fiscal_period_to_timestamp(ordered["fiscal_period"], ordered["period_type"])
    ordered = ordered.dropna(subset=[value_col, "fiscal_period_ts"]).sort_values("fiscal_period_ts")
    if ordered.empty:
        return pd.Series(dtype=float, index=pd.DatetimeIndex([]))
    return pd.Series(ordered[value_col].values, index=ordered["fiscal_period_ts"])


def _fiscal_period_to_timestamp(fiscal_period: pd.Series, period_type: pd.Series) -> pd.Series:
    fiscal_period = fiscal_period.astype(str).str.strip()
    period_type = period_type.astype(str).str.strip().str.lower()
    out = pd.Series(pd.NaT, index=fiscal_period.index, dtype="datetime64[ns]")

    quarterly_mask = period_type.isin({"quarterly", "q", "quarter"})
    if quarterly_mask.any():
        q_series = fiscal_period[quarterly_mask]
        extracted = q_series.str.extract(r"(?P<year>\d{4}).*?[Qq](?P<q>[1-4])")
        q_ts = pd.Series(pd.NaT, index=q_series.index, dtype="datetime64[ns]")
        valid = extracted["year"].notna() & extracted["q"].notna()
        if valid.any():
            periods = pd.PeriodIndex.from_fields(
                year=extracted.loc[valid, "year"].astype(int),
                quarter=extracted.loc[valid, "q"].astype(int),
                freq="Q",
            )
            q_ts.loc[valid] = periods.to_timestamp(how="end")
        q_ts.loc[~valid] = pd.to_datetime(q_series.loc[~valid], errors="coerce")
        out.loc[quarterly_mask] = q_ts

    annual_mask = period_type.isin({"annual", "yearly", "y", "year"})
    if annual_mask.any():
        y_series = fiscal_period[annual_mask]
        years = y_series.str.extract(r"(?P<year>\d{4})")["year"]
        y_ts = pd.Series(pd.NaT, index=y_series.index, dtype="datetime64[ns]")
        valid = years.notna()
        if valid.any():
            y_ts.loc[valid] = pd.to_datetime(years.loc[valid].astype(str) + "-12-31", errors="coerce")
        y_ts.loc[~valid] = pd.to_datetime(y_series.loc[~valid], errors="coerce")
        out.loc[annual_mask] = y_ts

    other_mask = ~(quarterly_mask | annual_mask)
    if other_mask.any():
        out.loc[other_mask] = pd.to_datetime(fiscal_period.loc[other_mask], errors="coerce")
    return out


def _preprocess_periodic_history(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    prepared = frame.copy()
    for col, default in (("source_priority", 0), ("is_correction", 0)):
        if col not in prepared.columns:
            prepared[col] = default
    prepared["source_priority"] = pd.to_numeric(prepared["source_priority"], errors="coerce").fillna(0)
    prepared["is_correction"] = pd.to_numeric(prepared["is_correction"], errors="coerce").fillna(0)
    prepared["reported_date"] = pd.to_datetime(prepared.get("reported_date"), errors="coerce")
    prepared["date"] = pd.to_datetime(prepared.get("date"), errors="coerce")
    prepared["source_ts"] = pd.to_datetime(prepared.get("source_ts"), errors="coerce")

    dedupe_keys = ["ticker", "fiscal_period", "period_type", "consolidation_type"]
    for key in dedupe_keys:
        if key not in prepared.columns:
            prepared[key] = None

    prepared = prepared.sort_values(
        by=[*dedupe_keys, "is_correction", "reported_date", "date", "source_priority", "source_ts"],
        ascending=[True, True, True, True, False, False, False, False, False],
    )
    return prepared.drop_duplicates(subset=dedupe_keys, keep="first")


def _filter_period_type(frame: pd.DataFrame, allowed: set[str]) -> pd.DataFrame:
    if frame.empty:
        return frame
    normalized = frame["period_type"].astype(str).str.lower().str.strip()
    return frame[normalized.isin(allowed)].copy()


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


def calc_cagr(series: pd.Series, asof: str, window_years: int, *, period_type: str | None = None) -> GrowthResult:
    """Sample requirements: at least window_years + 1 annual points and positive start/end values."""
    if period_type and period_type.lower() not in {"annual", "yearly", "y", "year"}:
        return GrowthResult(None, window_years, asof, 0)
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


def calc_yoy(series: pd.Series, asof: str, *, period_type: str | None = None) -> GrowthResult:
    """Sample requirements: at least 2 points; prior-year denominator must be non-zero."""
    if period_type and period_type.lower() not in {"annual", "yearly", "y", "year"}:
        return GrowthResult(None, 1, asof, 0)
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


def calc_qoq(series: pd.Series, asof: str, *, period_type: str | None = None) -> GrowthResult:
    """Sample requirements: at least 2 quarterly points; prior-quarter denominator must be non-zero."""
    if period_type and period_type.lower() not in {"quarterly", "q", "quarter"}:
        return GrowthResult(None, None, asof, 0)
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


def calc_ttm_growth(series: pd.Series, asof: str, *, period_type: str | None = None) -> GrowthResult:
    """Sample requirements: >=8 quarterly points (4 current + 4 prior); prior TTM denominator must be non-zero."""
    if period_type and period_type.lower() not in {"quarterly", "q", "quarter"}:
        return GrowthResult(None, 1, asof, 0)
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


def calc_eps_growth_qoq(series: pd.Series, asof: str) -> GrowthResult:
    return calc_qoq(series, asof=asof, period_type="quarterly")


def calc_sales_growth_qoq(series: pd.Series, asof: str) -> GrowthResult:
    return calc_qoq(series, asof=asof, period_type="quarterly")


def calc_eps_growth_ttm(series: pd.Series, asof: str) -> GrowthResult:
    return calc_ttm_growth(series, asof=asof, period_type="quarterly")


def calc_sales_growth_ttm(series: pd.Series, asof: str) -> GrowthResult:
    return calc_ttm_growth(series, asof=asof, period_type="quarterly")


def calc_eps_growth_cagr(series: pd.Series, asof: str, *, window_years: int) -> GrowthResult:
    return calc_cagr(series, asof=asof, window_years=window_years, period_type="annual")


def calc_sales_growth_cagr(series: pd.Series, asof: str, *, window_years: int) -> GrowthResult:
    return calc_cagr(series, asof=asof, window_years=window_years, period_type="annual")


def compute_growth_bundle(fund_hist: pd.DataFrame, ticker: str, asof: str) -> dict[str, GrowthResult]:
    subset = fund_hist[fund_hist["ticker"] == ticker].copy()
    empty_result = {
        "eps_growth_qtr_over_qtr": GrowthResult(None, None, asof, 0),
        "eps_growth_ttm": GrowthResult(None, 1, asof, 0),
        "eps_growth_past_3y": GrowthResult(None, 3, asof, 0),
        "eps_growth_past_5y": GrowthResult(None, 5, asof, 0),
        "sales_growth_qtr_over_qtr": GrowthResult(None, None, asof, 0),
        "sales_growth_ttm": GrowthResult(None, 1, asof, 0),
        "sales_growth_past_3y": GrowthResult(None, 3, asof, 0),
        "sales_growth_past_5y": GrowthResult(None, 5, asof, 0),
    }
    if subset.empty:
        empty_result["eps_growth_this_year_over_year"] = empty_result["eps_growth_qtr_over_qtr"]
        return empty_result

    deduped = _preprocess_periodic_history(subset)
    annual = _filter_period_type(deduped, {"annual", "yearly", "y", "year"})
    quarterly = _filter_period_type(deduped, {"quarterly", "q", "quarter"})

    eps_annual_series = _to_numeric_series(annual, "eps")
    eps_quarterly_series = _to_numeric_series(quarterly, "eps")
    rev_annual_series = _to_numeric_series(annual, "revenue")
    rev_quarterly_series = _to_numeric_series(quarterly, "revenue")

    eps_qoq = calc_eps_growth_qoq(eps_quarterly_series, asof=asof)
    bundle = {
        "eps_growth_qtr_over_qtr": eps_qoq,
        "eps_growth_ttm": calc_eps_growth_ttm(eps_quarterly_series, asof=asof),
        "eps_growth_past_3y": calc_eps_growth_cagr(eps_annual_series, asof=asof, window_years=3),
        "eps_growth_past_5y": calc_eps_growth_cagr(eps_annual_series, asof=asof, window_years=5),
        "sales_growth_qtr_over_qtr": calc_sales_growth_qoq(rev_quarterly_series, asof=asof),
        "sales_growth_ttm": calc_sales_growth_ttm(rev_quarterly_series, asof=asof),
        "sales_growth_past_3y": calc_sales_growth_cagr(rev_annual_series, asof=asof, window_years=3),
        "sales_growth_past_5y": calc_sales_growth_cagr(rev_annual_series, asof=asof, window_years=5),
    }
    # Backward-compatibility alias for existing snapshot/query/session keys.
    bundle["eps_growth_this_year_over_year"] = bundle["eps_growth_qtr_over_qtr"]
    return bundle
