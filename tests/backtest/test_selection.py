import math

import pandas as pd

from stock_screener.backtest.selection import build_target_weights, select_tickers


def test_select_tickers_all_mode():
    df = pd.DataFrame([{"ticker": "AAA"}, {"ticker": "BBB"}])
    selected, meta = select_tickers(df, {"mode": "all", "min_holdings": 3})

    assert selected == ["AAA", "BBB"]
    assert meta["below_min_holdings"] is True


def test_select_tickers_cap_n_sorts_desc_with_nan_last():
    df = pd.DataFrame(
        [
            {"ticker": "AAA", "mcap": 100},
            {"ticker": "BBB", "mcap": None},
            {"ticker": "CCC", "mcap": 300},
        ]
    )

    selected, meta = select_tickers(
        df,
        {
            "mode": "cap_n",
            "cap_n": 2,
            "sort_by": "mcap",
            "sort_direction": "desc",
            "min_holdings": 1,
        },
    )

    assert selected == ["CCC", "AAA"]
    assert meta["below_min_holdings"] is False


def test_select_tickers_cap_n_with_less_rows_returns_all_available():
    df = pd.DataFrame([{"ticker": "AAA", "mcap": 10}])
    selected, _ = select_tickers(df, {"mode": "cap_n", "cap_n": 5, "sort_by": "mcap", "sort_direction": "desc"})
    assert selected == ["AAA"]


def test_build_target_weights_equal_and_max_weight_renormalized():
    weights = build_target_weights(["AAA", "BBB", "CCC"], weighting="equal", max_weight=0.4)

    assert set(weights) == {"AAA", "BBB", "CCC"}
    assert math.isclose(sum(weights.values()), 1.0, rel_tol=1e-9)
    assert all(w <= 0.4 + 1e-12 for w in weights.values())
