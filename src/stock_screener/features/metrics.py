from __future__ import annotations

import numpy as np
import pandas as pd

from stock_screener.features.fundamental_growth import GrowthResult, compute_growth_bundle

CALC_VERSION = "v1.2"


def _result_to_row(result: GrowthResult) -> dict[str, object]:
    return {
        "value": np.nan if result.value is None else result.value,
        "window_years": result.window_years,
        "asof": result.asof,
        "sample_count": result.sample_count,
    }


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
    merged["roe_proxy"] = np.where((merged["bps"].fillna(0) > 0), merged["eps"] / merged["bps"], np.nan)
    merged["eps_positive"] = (merged["eps"].fillna(0) > 0).astype(int)
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
        sales_qoq = _result_to_row(bundle["sales_growth_qtr_over_qtr"])
        return pd.Series(
            {
                "eps_cagr_5y": eps_5y["value"],
                "eps_yoy_q": eps_yoy["value"],
                "eps_growth_ttm": eps_ttm["value"],
                "sales_growth_qoq": sales_qoq["value"],
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

    merged["asof_date"] = asof_date
    merged["calc_version"] = CALC_VERSION

    cols = [
        "asof_date", "ticker", "name", "market", "close", "mcap", "avg_value_20d", "current_value", "relative_value", "turnover_20d",
        "per", "pbr", "div", "dps", "eps", "bps", "reserve_ratio", "fiscal_period", "period_type", "reported_date", "consolidation_type", "financial_source", "roe_proxy", "eps_positive", "sma20", "sma50", "sma200",
        "dist_sma20", "dist_sma50", "dist_sma200", "high_52w", "low_52w", "pos_52w", "near_52w_high_ratio",
        "vol_20d", "ret_1w", "ret_1m", "ret_3m", "ret_6m", "ret_1y", "eps_cagr_5y", "eps_yoy_q", "eps_growth_ttm", "sales_growth_qoq",
        "eps_cagr_5y_window_years", "eps_cagr_5y_asof", "eps_cagr_5y_sample_count",
        "eps_yoy_q_window_years", "eps_yoy_q_asof", "eps_yoy_q_sample_count", "calc_version",
    ]
    return merged[cols]
