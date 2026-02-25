import numpy as np
import pandas as pd

from stock_screener.features.metrics import build_snapshot


def test_build_snapshot_includes_expanded_fundamental_metrics_with_nan_fallback():
    price_window = pd.DataFrame(
        [
            {
                "date": (pd.Timestamp("2025-01-31") - pd.Timedelta(days=i)).strftime("%Y-%m-%d"),
                "ticker": "AAA",
                "open": 100 + i,
                "high": 100 + i,
                "low": 100 + i,
                "close": 100 + i,
                "volume": 1000,
                "value": 1_000_000,
            }
            for i in range(260, -1, -1)
        ]
    )

    daily = pd.DataFrame(
        [
            {
                "ticker": "AAA",
                "name": "Alpha",
                "market": "KOSPI",
                "mcap": 1_000_000_000,
                "per": 10.0,
                "pbr": 1.2,
                "div": 1.0,
                "dps": 100.0,
                "reserve_ratio": 300.0,
                "eps": 200.0,
                "bps": 1000.0,
                "fiscal_period": "2024Q4",
                "period_type": "quarterly",
                "reported_date": "2025-01-20",
                "consolidation_type": "C",
                "financial_source": "unit",
            }
        ]
    )

    fund_hist = pd.DataFrame(
        [
            {"ticker": "AAA", "fiscal_period": "2023Q1", "period_type": "quarterly", "consolidation_type": "C", "revenue": 100.0, "operating_income": 10.0, "net_income": 7.0, "eps": 10.0, "reported_date": "2023-05-01"},
            {"ticker": "AAA", "fiscal_period": "2023Q2", "period_type": "quarterly", "consolidation_type": "C", "revenue": 110.0, "operating_income": 11.0, "net_income": 8.0, "eps": 11.0, "reported_date": "2023-08-01"},
            {"ticker": "AAA", "fiscal_period": "2023Q3", "period_type": "quarterly", "consolidation_type": "C", "revenue": 120.0, "operating_income": 12.0, "net_income": 9.0, "eps": 12.0, "reported_date": "2023-11-01"},
            {"ticker": "AAA", "fiscal_period": "2023Q4", "period_type": "quarterly", "consolidation_type": "C", "revenue": 130.0, "operating_income": 13.0, "net_income": 10.0, "eps": 13.0, "reported_date": "2024-02-01"},
            {"ticker": "AAA", "fiscal_period": "2024Q1", "period_type": "quarterly", "consolidation_type": "C", "revenue": 140.0, "operating_income": 14.0, "net_income": 11.0, "eps": 14.0, "reported_date": "2024-05-01"},
            {"ticker": "AAA", "fiscal_period": "2024Q2", "period_type": "quarterly", "consolidation_type": "C", "revenue": 150.0, "operating_income": 15.0, "net_income": 12.0, "eps": 15.0, "reported_date": "2024-08-01"},
            {"ticker": "AAA", "fiscal_period": "2024Q3", "period_type": "quarterly", "consolidation_type": "C", "revenue": 160.0, "operating_income": 16.0, "net_income": 13.0, "eps": 16.0, "reported_date": "2024-11-01"},
            {"ticker": "AAA", "fiscal_period": "2024Q4", "period_type": "quarterly", "consolidation_type": "C", "revenue": 170.0, "operating_income": 17.0, "net_income": 14.0, "eps": 17.0, "reported_date": "2025-02-01"},
        ]
    )

    snapshot = build_snapshot(price_window=price_window, daily=daily, fund_hist=fund_hist, asof_date="2025-01-31")
    row = snapshot.iloc[0]

    assert "pe_ratio" in snapshot.columns
    assert "sales_growth_ttm" in snapshot.columns
    assert "eps_cagr_3y" in snapshot.columns
    assert "sales_cagr_3y" in snapshot.columns
    assert row["pe_ratio"] == 10.0
    assert np.isnan(row["forward_pe"])
    assert row["ps_ratio"] > 0
    assert row["ps"] == row["ps_ratio"]
    assert row["operating_margin"] > 0
    assert row["peg"] == row["peg_ratio"] or (np.isnan(row["peg"]) and np.isnan(row["peg_ratio"]))
    assert np.isnan(row["ev"])
    assert np.isnan(row["ev_sales"])
    assert np.isnan(row["ev_ebitda"])
    assert np.isnan(row["debt_equity"])
    assert row["payout_ratio"] == 0.5



def test_build_snapshot_includes_technical_and_foreign_metrics_with_nan_defaults():
    asof_date = "2025-01-31"
    dates = pd.date_range("2025-01-01", periods=31, freq="D")

    price_window = pd.DataFrame(
        {
            "date": [d.strftime("%Y-%m-%d") for d in dates],
            "ticker": ["AAA"] * len(dates),
            "open": [100.0] * len(dates),
            "high": [102.0] * len(dates),
            "low": [99.0] * len(dates),
            "close": [101.0] * len(dates),
            "volume": [1000] * len(dates),
            "value": [101000.0] * len(dates),
            "foreign_net_buy_volume": [np.nan] * 11 + [100.0] * 20,
            "foreign_net_buy_value": [np.nan] * 11 + [1_000.0] * 20,
        }
    )

    daily = pd.DataFrame(
        [
            {
                "ticker": "AAA",
                "name": "Alpha",
                "market": "KOSPI",
                "mcap": 1_000_000_000,
                "per": 10.0,
                "pbr": 1.0,
                "div": 0.0,
                "dps": 0.0,
                "reserve_ratio": 200.0,
                "eps": 100.0,
                "bps": 1000.0,
                "fiscal_period": "2024Q4",
                "period_type": "quarterly",
                "reported_date": "2025-01-15",
                "consolidation_type": "C",
                "financial_source": "unit",
            }
        ]
    )

    fund_hist = pd.DataFrame(
        [
            {
                "ticker": "AAA",
                "fiscal_period": "2024Q4",
                "period_type": "quarterly",
                "consolidation_type": "C",
                "revenue": 100.0,
                "operating_income": 10.0,
                "net_income": 8.0,
                "eps": 10.0,
                "reported_date": "2025-01-15",
            }
        ]
    )

    snapshot = build_snapshot(price_window=price_window, daily=daily, fund_hist=fund_hist, asof_date=asof_date)
    row = snapshot.iloc[0]

    assert {"rsi_14", "atr_14", "gap_pct", "foreign_net_buy_volume_20d", "foreign_net_buy_ratio", "foreign_net_buy_value_20d"}.issubset(snapshot.columns)
    assert row["rsi_14"] == 50.0
    assert row["atr_14"] == 3.0
    assert row["gap_pct"] == 100.0 / 101.0 - 1
    assert row["foreign_net_buy_volume_20d"] == 100.0
    assert row["foreign_net_buy_ratio"] == 1.0
    assert row["foreign_net_buy_value_20d"] == 20_000.0


def test_build_snapshot_foreign_metrics_default_to_nan_when_input_column_missing():
    asof_date = "2025-01-31"
    dates = pd.date_range("2025-01-01", periods=31, freq="D")
    price_window = pd.DataFrame(
        {
            "date": [d.strftime("%Y-%m-%d") for d in dates],
            "ticker": ["BBB"] * len(dates),
            "open": [100.0] * len(dates),
            "high": [102.0] * len(dates),
            "low": [99.0] * len(dates),
            "close": [101.0] * len(dates),
            "volume": [1000] * len(dates),
            "value": [101000.0] * len(dates),
        }
    )
    daily = pd.DataFrame(
        [
            {
                "ticker": "BBB",
                "name": "Beta",
                "market": "KOSDAQ",
                "mcap": 500_000_000,
                "per": 10.0,
                "pbr": 1.0,
                "div": 0.0,
                "dps": 0.0,
                "reserve_ratio": 200.0,
                "eps": 100.0,
                "bps": 1000.0,
                "fiscal_period": "2024Q4",
                "period_type": "quarterly",
                "reported_date": "2025-01-15",
                "consolidation_type": "C",
                "financial_source": "unit",
            }
        ]
    )

    snapshot = build_snapshot(
        price_window=price_window,
        daily=daily,
        fund_hist=pd.DataFrame(columns=["ticker", "fiscal_period", "period_type", "consolidation_type", "reported_date"]),
        asof_date=asof_date,
    )
    row = snapshot.iloc[0]

    assert pd.isna(row["foreign_net_buy_volume"])
    assert pd.isna(row["foreign_net_buy_volume_20d"])
    assert pd.isna(row["foreign_net_buy_ratio"])
    assert pd.isna(row["foreign_net_buy_value_20d"])
