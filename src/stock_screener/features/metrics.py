from __future__ import annotations

import numpy as np
import pandas as pd

CALC_VERSION = "v1.0"


def build_snapshot(price_window: pd.DataFrame, daily: pd.DataFrame, asof_date: str) -> pd.DataFrame:
    if price_window.empty:
        return pd.DataFrame()

    price_window = price_window.copy()
    price_window["date"] = pd.to_datetime(price_window["date"])
    price_window = price_window.sort_values(["ticker", "date"])

    groups = []
    for ticker, g in price_window.groupby("ticker", sort=False):
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
    merged["turnover_20d"] = merged["avg_value_20d"] / merged["mcap"]

    merged["asof_date"] = asof_date
    merged["calc_version"] = CALC_VERSION

    cols = [
        "asof_date", "ticker", "name", "market", "close", "mcap", "avg_value_20d", "turnover_20d",
        "per", "pbr", "div", "eps", "bps", "roe_proxy", "eps_positive", "sma20", "sma50", "sma200",
        "dist_sma20", "dist_sma50", "dist_sma200", "high_52w", "low_52w", "pos_52w", "vol_20d",
        "ret_1w", "ret_1m", "ret_3m", "ret_6m", "ret_1y", "calc_version",
    ]
    return merged[cols]
