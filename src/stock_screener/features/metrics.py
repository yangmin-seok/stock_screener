from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from stock_screener.features.fundamental_growth import (
    GrowthResult,
    _fiscal_period_to_timestamp,
    _preprocess_periodic_history,
    compute_growth_bundle,
)

CALC_VERSION = "v1.4"
logger = logging.getLogger(__name__)


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    num = pd.to_numeric(numerator, errors="coerce")
    den = pd.to_numeric(denominator, errors="coerce")
    valid = num.notna() & den.notna() & (den > 0)
    result = pd.Series(np.nan, index=num.index, dtype=float)
    result.loc[valid] = num.loc[valid] / den.loc[valid]
    return result



def _result_to_row(result: GrowthResult) -> dict[str, object]:
    return {
        "value": np.nan if result.value is None else result.value,
        "window_years": result.window_years,
        "asof": result.asof,
        "sample_count": result.sample_count,
    }


def _latest_periodic_row(fund_hist: pd.DataFrame, ticker: str, asof_date: str) -> pd.Series | None:
    subset = fund_hist[fund_hist["ticker"] == ticker].copy()
    if subset.empty:
        return None
    deduped = _preprocess_periodic_history(subset)
    if deduped.empty:
        return None
    deduped["period_ts"] = _fiscal_period_to_timestamp(deduped["fiscal_period"], deduped["period_type"])
    asof_ts = pd.Timestamp(asof_date)
    deduped = deduped[deduped["period_ts"].notna() & (deduped["period_ts"] <= asof_ts)]
    if deduped.empty:
        return None
    deduped = deduped.sort_values("period_ts")
    return deduped.iloc[-1]


def _ttm_sum_from_quarterly(fund_hist: pd.DataFrame, ticker: str, asof_date: str, value_col: str) -> float:
    subset = fund_hist[fund_hist["ticker"] == ticker].copy()
    if subset.empty or value_col not in subset.columns:
        return np.nan
    deduped = _preprocess_periodic_history(subset)
    q = deduped[deduped["period_type"].astype(str).str.lower().isin({"quarterly", "q", "quarter"})].copy()
    if q.empty:
        return np.nan
    q[value_col] = pd.to_numeric(q[value_col], errors="coerce")
    q["period_ts"] = _fiscal_period_to_timestamp(q["fiscal_period"], q["period_type"])
    asof_ts = pd.Timestamp(asof_date)
    q = q[q["period_ts"].notna() & (q["period_ts"] <= asof_ts) & q[value_col].notna()].sort_values("period_ts")
    if len(q) < 4:
        return np.nan
    return float(q[value_col].iloc[-4:].sum())


def build_snapshot(
    price_window: pd.DataFrame,
    daily: pd.DataFrame,
    fund_hist: pd.DataFrame,
    asof_date: str,
) -> pd.DataFrame:
    if price_window.empty:
        return pd.DataFrame()

    price_window = price_window.copy()
    price_window["date"] = pd.to_datetime(price_window["date"])
    price_window = price_window.sort_values(["ticker", "date"])

    groups = []
    for _, g in price_window.groupby("ticker", sort=False):
        g = g.copy()
        g["ret_daily"] = g["close"].pct_change()
        g["sma20"] = g["close"].rolling(20).mean()
        g["sma50"] = g["close"].rolling(50).mean()
        g["sma200"] = g["close"].rolling(200).mean()
        g["avg_value_20d"] = g["value"].rolling(20).mean()
        g["high_52w"] = g["close"].rolling(252).max()
        g["low_52w"] = g["close"].rolling(252).min()
        g["vol_20d"] = g["ret_daily"].rolling(20).std()
        for name, n in [("ret_1w", 5), ("ret_1m", 21), ("ret_3m", 63), ("ret_6m", 126), ("ret_1y", 252)]:
            g[name] = g["close"].pct_change(n)
        groups.append(g.iloc[-1])

    latest = pd.DataFrame(groups)
    latest["date"] = latest["date"].dt.strftime("%Y-%m-%d")
    latest = latest.rename(columns={"date": "asof_date"})
    latest = latest[latest["asof_date"] == asof_date]

    merged = latest.merge(daily, how="left", on="ticker")
    for col in ("foreign_net_buy_volume", "foreign_net_buy_value"):
        if col not in merged.columns:
            merged[col] = np.nan
    merged["close"] = merged["close"].astype(float)
    eps_num = pd.to_numeric(merged["eps"], errors="coerce")
    bps_num = pd.to_numeric(merged["bps"], errors="coerce")
    per_num = pd.to_numeric(merged["per"], errors="coerce")
    pbr_num = pd.to_numeric(merged["pbr"], errors="coerce")
    dps_num = pd.to_numeric(merged["dps"], errors="coerce")

    merged["roe_proxy"] = _safe_ratio(eps_num, bps_num)
    merged["eps_positive"] = (eps_num > 0).fillna(False).astype(int)
    merged["dist_sma20"] = merged["close"] / merged["sma20"] - 1
    merged["dist_sma50"] = merged["close"] / merged["sma50"] - 1
    merged["dist_sma200"] = merged["close"] / merged["sma200"] - 1
    denom = merged["high_52w"] - merged["low_52w"]
    merged["pos_52w"] = np.where(denom > 0, (merged["close"] - merged["low_52w"]) / denom, np.nan)
    merged["near_52w_high_ratio"] = np.where(merged["high_52w"] > 0, merged["close"] / merged["high_52w"], np.nan)
    merged["current_value"] = merged["value"]
    merged["relative_value"] = _safe_ratio(merged["current_value"], merged["avg_value_20d"])
    merged["turnover_20d"] = _safe_ratio(merged["avg_value_20d"], merged["mcap"])

    growth = merged[["ticker"]].copy()

    def _growth_row(ticker: str) -> pd.Series:
        bundle = compute_growth_bundle(fund_hist=fund_hist, ticker=ticker, asof=asof_date)
        eps_3y = _result_to_row(bundle["eps_growth_past_3y"])
        eps_5y = _result_to_row(bundle["eps_growth_past_5y"])
        eps_yoy = _result_to_row(bundle["eps_growth_this_year_over_year"])
        eps_ttm = _result_to_row(bundle["eps_growth_ttm"])
        eps_qoq = _result_to_row(bundle["eps_growth_qtr_over_qtr"])
        sales_qoq = _result_to_row(bundle["sales_growth_qtr_over_qtr"])
        sales_ttm = _result_to_row(bundle["sales_growth_ttm"])
        sales_3y = _result_to_row(bundle["sales_growth_past_3y"])
        sales_5y = _result_to_row(bundle["sales_growth_past_5y"])
        return pd.Series(
            {
                "eps_cagr_3y": eps_3y["value"],
                "eps_cagr_5y": eps_5y["value"],
                "eps_yoy_q": eps_yoy["value"],
                "eps_growth_ttm": eps_ttm["value"],
                "eps_qoq": eps_qoq["value"],
                "sales_growth_qoq": sales_qoq["value"],
                "sales_growth_ttm": sales_ttm["value"],
                "sales_cagr_3y": sales_3y["value"],
                "sales_cagr_5y": sales_5y["value"],
                "eps_cagr_3y_window_years": eps_3y["window_years"],
                "eps_cagr_3y_asof": eps_3y["asof"],
                "eps_cagr_3y_sample_count": eps_3y["sample_count"],
                "eps_cagr_5y_window_years": eps_5y["window_years"],
                "eps_cagr_5y_asof": eps_5y["asof"],
                "eps_cagr_5y_sample_count": eps_5y["sample_count"],
                "eps_yoy_q_window_years": eps_yoy["window_years"],
                "eps_yoy_q_asof": eps_yoy["asof"],
                "eps_yoy_q_sample_count": eps_yoy["sample_count"],
                "sales_cagr_3y_window_years": sales_3y["window_years"],
                "sales_cagr_3y_asof": sales_3y["asof"],
                "sales_cagr_3y_sample_count": sales_3y["sample_count"],
            }
        )

    growth = pd.concat([growth, growth["ticker"].apply(_growth_row)], axis=1)
    merged = merged.merge(growth, on="ticker", how="left")

    # Expanded fundamental metrics (NaN fallback when source fields are unavailable)
    merged["pe_ratio"] = per_num
    merged["forward_pe"] = np.nan
    rev_ttm = merged["ticker"].map(lambda t: _ttm_sum_from_quarterly(fund_hist, t, asof_date, "revenue"))
    merged["ps_ratio"] = _safe_ratio(merged["mcap"], rev_ttm)
    merged["pb_ratio"] = pbr_num
    merged["peg_ratio"] = _safe_ratio(per_num, merged["eps_cagr_5y"] * 100.0)

    latest_periodic = merged["ticker"].map(lambda t: _latest_periodic_row(fund_hist, t, asof_date))
    rev_latest = latest_periodic.map(lambda r: np.nan if r is None else pd.to_numeric(r.get("revenue"), errors="coerce"))
    op_latest = latest_periodic.map(lambda r: np.nan if r is None else pd.to_numeric(r.get("operating_income"), errors="coerce"))
    net_latest = latest_periodic.map(lambda r: np.nan if r is None else pd.to_numeric(r.get("net_income"), errors="coerce"))
    debt_latest = latest_periodic.map(lambda r: np.nan if r is None else pd.to_numeric(r.get("debt"), errors="coerce"))
    cash_latest = latest_periodic.map(lambda r: np.nan if r is None else pd.to_numeric(r.get("cash"), errors="coerce"))

    ebitda_ttm = merged["ticker"].map(lambda t: _ttm_sum_from_quarterly(fund_hist, t, asof_date, "ebitda"))
    if "revenue" not in fund_hist.columns:
        logger.warning("ps/ev_sales missing input column 'revenue'; related metrics are set to NaN")
    if "debt" not in fund_hist.columns:
        logger.warning("ev/ev-based metrics missing input column 'debt'; EV-based metrics default to NaN")
    if "cash" not in fund_hist.columns:
        logger.warning("ev/ev-based metrics missing input column 'cash'; EV-based metrics default to NaN")
    if "ebitda" not in fund_hist.columns:
        logger.warning("ev_ebitda missing input column 'ebitda'; metric is set to NaN")

    enterprise_value = pd.to_numeric(merged["mcap"], errors="coerce") + debt_latest - cash_latest
    invalid_ev = enterprise_value.isna() | (enterprise_value <= 0)
    enterprise_value = enterprise_value.mask(invalid_ev, np.nan)

    merged["ps"] = merged["ps_ratio"]
    merged["peg"] = merged["peg_ratio"]
    merged["ev"] = enterprise_value
    merged["ev_sales"] = _safe_ratio(enterprise_value, rev_ttm)
    merged["ev_ebitda"] = _safe_ratio(enterprise_value, ebitda_ttm)

    merged["gross_margin"] = np.nan
    merged["operating_margin"] = _safe_ratio(op_latest, rev_latest)
    merged["net_margin"] = _safe_ratio(net_latest, rev_latest)
    merged["roa"] = np.nan
    merged["roe"] = _safe_ratio(eps_num, bps_num)
    merged["roic"] = np.nan

    merged["debt_equity"] = np.nan
    merged["lt_debt_equity"] = np.nan
    merged["current_ratio"] = np.nan
    merged["quick_ratio"] = np.nan
    merged["payout_ratio"] = _safe_ratio(dps_num, eps_num)

    counts = price_window.groupby("ticker")["close"].size()
    merged["has_price_5y"] = merged["ticker"].map(lambda t: int(counts.get(t, 0) >= 252 * 5))
    merged["has_price_10y"] = merged["ticker"].map(lambda t: int(counts.get(t, 0) >= 252 * 10))

    merged["asof_date"] = asof_date
    merged["calc_version"] = CALC_VERSION

    cols = [
        "asof_date", "ticker", "name", "market", "close", "mcap", "avg_value_20d", "current_value", "relative_value", "turnover_20d",
        "per", "pbr", "div", "dps", "eps", "bps", "reserve_ratio", "fiscal_period", "period_type", "reported_date", "consolidation_type", "financial_source", "roe_proxy", "eps_positive", "sma20", "sma50", "sma200",
        "dist_sma20", "dist_sma50", "dist_sma200", "high_52w", "low_52w", "pos_52w", "near_52w_high_ratio",
        "vol_20d", "ret_1w", "ret_1m", "ret_3m", "ret_6m", "ret_1y", "eps_cagr_3y", "eps_cagr_5y", "eps_yoy_q", "eps_growth_ttm", "eps_qoq", "sales_growth_qoq", "sales_growth_ttm", "sales_cagr_3y", "sales_cagr_5y",
        "pe_ratio", "forward_pe", "ps_ratio", "pb_ratio", "peg_ratio", "ps", "peg", "ev", "ev_sales", "ev_ebitda",
        "gross_margin", "operating_margin", "net_margin", "roa", "roe", "roic",
        "debt_equity", "lt_debt_equity", "current_ratio", "quick_ratio", "payout_ratio", "foreign_net_buy_volume", "foreign_net_buy_value",
        "eps_cagr_3y_window_years", "eps_cagr_3y_asof", "eps_cagr_3y_sample_count",
        "eps_cagr_5y_window_years", "eps_cagr_5y_asof", "eps_cagr_5y_sample_count",
        "eps_yoy_q_window_years", "eps_yoy_q_asof", "eps_yoy_q_sample_count",
        "sales_cagr_3y_window_years", "sales_cagr_3y_asof", "sales_cagr_3y_sample_count", "has_price_5y", "has_price_10y", "calc_version",
    ]
    return merged[cols]
