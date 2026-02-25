from __future__ import annotations

import numpy as np
import pandas as pd

from stock_screener.features.fundamental_growth import (
    GrowthResult,
    _fiscal_period_to_timestamp,
    _preprocess_periodic_history,
    compute_growth_bundle,
)

CALC_VERSION = "v1.4"


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
    merged["close"] = merged["close"].astype(float)
    eps_num = pd.to_numeric(merged["eps"], errors="coerce")
    bps_num = pd.to_numeric(merged["bps"], errors="coerce")
    per_num = pd.to_numeric(merged["per"], errors="coerce")
    pbr_num = pd.to_numeric(merged["pbr"], errors="coerce")
    dps_num = pd.to_numeric(merged["dps"], errors="coerce")

    merged["roe_proxy"] = np.where((bps_num > 0), eps_num / bps_num, np.nan)
    merged["eps_positive"] = (eps_num > 0).fillna(False).astype(int)
    merged["dist_sma20"] = merged["close"] / merged["sma20"] - 1
    merged["dist_sma50"] = merged["close"] / merged["sma50"] - 1
    merged["dist_sma200"] = merged["close"] / merged["sma200"] - 1
    denom = merged["high_52w"] - merged["low_52w"]
    merged["pos_52w"] = np.where(denom > 0, (merged["close"] - merged["low_52w"]) / denom, np.nan)
    merged["near_52w_high_ratio"] = np.where(merged["high_52w"] > 0, merged["close"] / merged["high_52w"], np.nan)
    merged["current_value"] = merged["value"]
    merged["relative_value"] = np.where(merged["avg_value_20d"] > 0, merged["current_value"] / merged["avg_value_20d"], np.nan)
    merged["turnover_20d"] = merged["avg_value_20d"] / merged["mcap"]

    growth = merged[["ticker"]].copy()

    def _growth_row(ticker: str) -> pd.Series:
        bundle = compute_growth_bundle(fund_hist=fund_hist, ticker=ticker, asof=asof_date)
        eps_5y = _result_to_row(bundle["eps_growth_past_5y"])
        eps_yoy = _result_to_row(bundle["eps_growth_this_year_over_year"])
        eps_ttm = _result_to_row(bundle["eps_growth_ttm"])
        eps_qoq = _result_to_row(bundle["eps_growth_qtr_over_qtr"])
        sales_qoq = _result_to_row(bundle["sales_growth_qtr_over_qtr"])
        sales_ttm = _result_to_row(bundle["sales_growth_ttm"])
        sales_5y = _result_to_row(bundle["sales_growth_past_5y"])
        return pd.Series(
            {
                "eps_cagr_5y": eps_5y["value"],
                "eps_yoy_q": eps_yoy["value"],
                "eps_growth_ttm": eps_ttm["value"],
                "eps_qoq": eps_qoq["value"],
                "sales_growth_qoq": sales_qoq["value"],
                "sales_growth_ttm": sales_ttm["value"],
                "sales_cagr_5y": sales_5y["value"],
                "eps_cagr_5y_window_years": eps_5y["window_years"],
                "eps_cagr_5y_asof": eps_5y["asof"],
                "eps_cagr_5y_sample_count": eps_5y["sample_count"],
                "eps_yoy_q_window_years": eps_yoy["window_years"],
                "eps_yoy_q_asof": eps_yoy["asof"],
                "eps_yoy_q_sample_count": eps_yoy["sample_count"],
            }
        )

    growth = pd.concat([growth, growth["ticker"].apply(_growth_row)], axis=1)
    merged = merged.merge(growth, on="ticker", how="left")

    # Expanded fundamental metrics (NaN fallback when source fields are unavailable)
    merged["pe_ratio"] = per_num
    merged["forward_pe"] = np.nan
    rev_ttm = merged["ticker"].map(lambda t: _ttm_sum_from_quarterly(fund_hist, t, asof_date, "revenue"))
    op_ttm = merged["ticker"].map(lambda t: _ttm_sum_from_quarterly(fund_hist, t, asof_date, "operating_income"))
    net_ttm = merged["ticker"].map(lambda t: _ttm_sum_from_quarterly(fund_hist, t, asof_date, "net_income"))
    merged["ps_ratio"] = np.where(rev_ttm > 0, merged["mcap"] / rev_ttm, np.nan)
    merged["pb_ratio"] = pbr_num
    merged["peg_ratio"] = np.where(merged["eps_cagr_5y"] > 0, per_num / (merged["eps_cagr_5y"] * 100.0), np.nan)
    merged["ev_sales"] = np.nan
    merged["ev_ebitda"] = np.nan

    latest_periodic = merged["ticker"].map(lambda t: _latest_periodic_row(fund_hist, t, asof_date))
    rev_latest = latest_periodic.map(lambda r: np.nan if r is None else pd.to_numeric(r.get("revenue"), errors="coerce"))
    op_latest = latest_periodic.map(lambda r: np.nan if r is None else pd.to_numeric(r.get("operating_income"), errors="coerce"))
    net_latest = latest_periodic.map(lambda r: np.nan if r is None else pd.to_numeric(r.get("net_income"), errors="coerce"))

    merged["gross_margin"] = np.nan
    merged["operating_margin"] = np.where(rev_latest > 0, op_latest / rev_latest, np.nan)
    merged["net_margin"] = np.where(rev_latest > 0, net_latest / rev_latest, np.nan)
    merged["roa"] = np.nan
    merged["roe"] = np.where(bps_num > 0, eps_num / bps_num, np.nan)
    merged["roic"] = np.nan

    merged["debt_equity"] = np.nan
    merged["lt_debt_equity"] = np.nan
    merged["current_ratio"] = np.nan
    merged["quick_ratio"] = np.nan
    merged["payout_ratio"] = np.where(eps_num > 0, dps_num / eps_num, np.nan)

    counts = price_window.groupby("ticker")["close"].size()
    merged["has_price_5y"] = merged["ticker"].map(lambda t: int(counts.get(t, 0) >= 252 * 5))
    merged["has_price_10y"] = merged["ticker"].map(lambda t: int(counts.get(t, 0) >= 252 * 10))

    merged["asof_date"] = asof_date
    merged["calc_version"] = CALC_VERSION

    cols = [
        "asof_date", "ticker", "name", "market", "close", "mcap", "avg_value_20d", "current_value", "relative_value", "turnover_20d",
        "per", "pbr", "div", "dps", "eps", "bps", "reserve_ratio", "fiscal_period", "period_type", "reported_date", "consolidation_type", "financial_source", "roe_proxy", "eps_positive", "sma20", "sma50", "sma200",
        "dist_sma20", "dist_sma50", "dist_sma200", "high_52w", "low_52w", "pos_52w", "near_52w_high_ratio",
        "vol_20d", "ret_1w", "ret_1m", "ret_3m", "ret_6m", "ret_1y", "eps_cagr_5y", "eps_yoy_q", "eps_growth_ttm", "eps_qoq", "sales_growth_qoq", "sales_growth_ttm", "sales_cagr_5y",
        "pe_ratio", "forward_pe", "ps_ratio", "pb_ratio", "peg_ratio", "ev_sales", "ev_ebitda",
        "gross_margin", "operating_margin", "net_margin", "roa", "roe", "roic",
        "debt_equity", "lt_debt_equity", "current_ratio", "quick_ratio", "payout_ratio",
        "eps_cagr_5y_window_years", "eps_cagr_5y_asof", "eps_cagr_5y_sample_count",
        "eps_yoy_q_window_years", "eps_yoy_q_asof", "eps_yoy_q_sample_count", "has_price_5y", "has_price_10y", "calc_version",
    ]
    return merged[cols]
