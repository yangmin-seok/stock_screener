from __future__ import annotations

import numpy as np
import pandas as pd

CALC_VERSION = "v1.1"


def _nearest_on_or_before(series: pd.Series, target: pd.Timestamp) -> float:
    subset = series[series.index <= target]
    if subset.empty:
        return np.nan
    return subset.iloc[-1]


def _eps_growth_metrics(fund_hist: pd.DataFrame, ticker: str, asof_date: str) -> tuple[float, float]:
    subset = fund_hist[fund_hist["ticker"] == ticker][["date", "eps"]].copy()
    if subset.empty:
        return np.nan, np.nan
    subset["date"] = pd.to_datetime(subset["date"])
    subset = subset.dropna(subset=["eps"]).sort_values("date")
    if subset.empty:
        return np.nan, np.nan

    eps_series = pd.Series(subset["eps"].values, index=subset["date"])
    asof_ts = pd.Timestamp(asof_date)
    eps_now = _nearest_on_or_before(eps_series, asof_ts)

    eps_5y_ago = _nearest_on_or_before(eps_series, asof_ts - pd.DateOffset(years=5))
    if pd.notna(eps_now) and pd.notna(eps_5y_ago) and eps_now > 0 and eps_5y_ago > 0:
        eps_cagr_5y = (eps_now / eps_5y_ago) ** (1 / 5) - 1
    else:
        eps_cagr_5y = np.nan

    # Quarterly YoY approximation using quarter-end EPS snapshot vs same quarter last year.
    q_end = (asof_ts.to_period("Q")).end_time.normalize()
    q_prev_year = q_end - pd.DateOffset(years=1)
    eps_q = _nearest_on_or_before(eps_series, q_end)
    eps_q_prev = _nearest_on_or_before(eps_series, q_prev_year)
    if pd.notna(eps_q) and pd.notna(eps_q_prev) and eps_q_prev > 0:
        eps_yoy_q = eps_q / eps_q_prev - 1
    else:
        eps_yoy_q = np.nan

    return eps_cagr_5y, eps_yoy_q


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
    merged["turnover_20d"] = merged["avg_value_20d"] / merged["mcap"]

    growth = merged[["ticker"]].copy()
    growth[["eps_cagr_5y", "eps_yoy_q"]] = growth["ticker"].apply(
        lambda x: pd.Series(_eps_growth_metrics(fund_hist, x, asof_date))
    )
    merged = merged.merge(growth, on="ticker", how="left")

    merged["asof_date"] = asof_date
    merged["calc_version"] = CALC_VERSION

    cols = [
        "asof_date", "ticker", "name", "market", "close", "mcap", "avg_value_20d", "turnover_20d",
        "per", "pbr", "div", "dps", "eps", "bps", "roe_proxy", "eps_positive", "sma20", "sma50", "sma200",
        "dist_sma20", "dist_sma50", "dist_sma200", "high_52w", "low_52w", "pos_52w", "near_52w_high_ratio",
        "vol_20d", "ret_1w", "ret_1m", "ret_3m", "ret_6m", "ret_1y", "eps_cagr_5y", "eps_yoy_q", "calc_version",
    ]
    return merged[cols]
