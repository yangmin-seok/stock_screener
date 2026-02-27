import math

import pandas as pd

from stock_screener.storage.db import init_db
from stock_screener.storage.repository import Repository


def _seed_basic_universe(repo: Repository) -> None:
    repo.upsert_tickers(
        pd.DataFrame(
            [
                {"ticker": "AAA", "name": "Alpha", "market": "KOSPI", "active_flag": 1},
                {"ticker": "BBB", "name": "Beta", "market": "KOSDAQ", "active_flag": 1},
                {"ticker": "calendar_ticker", "name": "Calendar", "market": "KOSPI", "active_flag": 1},
            ]
        )
    )


def test_get_trading_dates_calendar_sorted_and_range_filterable(tmp_path):
    db = tmp_path / "x.db"
    init_db(db)
    repo = Repository(db)
    _seed_basic_universe(repo)

    repo.upsert_prices(
        pd.DataFrame(
            [
                {"date": "2025-01-03", "ticker": "AAA", "open": 10, "high": 10, "low": 10, "close": 10, "volume": 100, "value": 1000},
                {"date": "2025-01-02", "ticker": "AAA", "open": 11, "high": 11, "low": 11, "close": 11, "volume": 100, "value": 1100},
                {"date": "2025-01-04", "ticker": "AAA", "open": 12, "high": 12, "low": 12, "close": 12, "volume": 100, "value": 1200},
                {"date": "2025-01-02", "ticker": "calendar_ticker", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1, "value": 1},
                {"date": "2025-01-04", "ticker": "calendar_ticker", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1, "value": 1},
                {"date": "2025-01-03", "ticker": "calendar_ticker", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1, "value": 1},
            ]
        )
    )

    dates = repo.get_trading_dates()

    assert dates == ["2025-01-02", "2025-01-03", "2025-01-04"]
    assert [d for d in dates if "2025-01-03" <= d <= "2025-01-04"] == ["2025-01-03", "2025-01-04"]


def test_get_asof_frame_foreign_window_matches_manual_sum_and_keeps_nan(tmp_path):
    db = tmp_path / "x.db"
    init_db(db)
    repo = Repository(db)
    _seed_basic_universe(repo)

    repo.upsert_prices(
        pd.DataFrame(
            [
                {"date": "2025-01-02", "ticker": "AAA", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1000, "value": 100000},
                {"date": "2025-01-03", "ticker": "AAA", "open": 101, "high": 102, "low": 100, "close": 101, "volume": 1200, "value": 121200},
                {"date": "2025-01-04", "ticker": "AAA", "open": 103, "high": 104, "low": 102, "close": 103, "volume": 1300, "value": 133900},
                {"date": "2025-01-02", "ticker": "BBB", "open": 50, "high": 51, "low": 49, "close": 50, "volume": 500, "value": 25000},
                {"date": "2025-01-03", "ticker": "BBB", "open": 51, "high": 52, "low": 50, "close": 51, "volume": 600, "value": 30600},
                {"date": "2025-01-04", "ticker": "BBB", "open": 52, "high": 53, "low": 51, "close": 52, "volume": 700, "value": 36400},
            ]
        )
    )
    repo.upsert_investor_flow(
        pd.DataFrame(
            [
                {"date": "2025-01-02", "ticker": "AAA", "foreign_net_buy_volume": 5, "foreign_net_buy_value": 5000},
                {"date": "2025-01-03", "ticker": "AAA", "foreign_net_buy_volume": 7, "foreign_net_buy_value": 7000},
                {"date": "2025-01-04", "ticker": "AAA", "foreign_net_buy_volume": None, "foreign_net_buy_value": None},
                {"date": "2025-01-02", "ticker": "BBB", "foreign_net_buy_volume": 1, "foreign_net_buy_value": 1000},
                {"date": "2025-01-03", "ticker": "BBB", "foreign_net_buy_volume": 2, "foreign_net_buy_value": 2000},
                {"date": "2025-01-04", "ticker": "BBB", "foreign_net_buy_volume": 3, "foreign_net_buy_value": 3000},
            ]
        )
    )

    frame = repo.get_asof_frame("2025-01-04", foreign_window=3)
    aaa = frame.loc[frame["ticker"] == "AAA"].iloc[0]

    assert aaa["foreign_cum_volume_20d"] == 12
    assert aaa["foreign_cum_value_20d"] == 12000
    assert math.isnan(float(aaa["foreign_net_buy_volume"]))
    assert math.isnan(float(aaa["foreign_net_buy_value"]))


def test_get_price_panel_columns_sorted_and_nan_preserved(tmp_path):
    db = tmp_path / "x.db"
    init_db(db)
    repo = Repository(db)
    _seed_basic_universe(repo)

    repo.upsert_prices(
        pd.DataFrame(
            [
                {"date": "2025-01-03", "ticker": "BBB", "open": 51, "high": 52, "low": 50, "close": None, "volume": 600, "value": 30600},
                {"date": "2025-01-02", "ticker": "AAA", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1000, "value": 100000},
                {"date": "2025-01-03", "ticker": "AAA", "open": 101, "high": 102, "low": 100, "close": 101, "volume": 1200, "value": 121200},
                {"date": "2025-01-02", "ticker": "BBB", "open": None, "high": 51, "low": 49, "close": 50, "volume": 500, "value": 25000},
            ]
        )
    )

    panel = repo.get_price_panel(["BBB", "AAA"], "2025-01-02", "2025-01-03")

    assert list(panel.columns) == ["date", "ticker", "open", "close"]
    assert panel[["date", "ticker"]].to_dict("records") == [
        {"date": "2025-01-02", "ticker": "AAA"},
        {"date": "2025-01-02", "ticker": "BBB"},
        {"date": "2025-01-03", "ticker": "AAA"},
        {"date": "2025-01-03", "ticker": "BBB"},
    ]

    bbb_open = panel.loc[(panel["date"] == "2025-01-02") & (panel["ticker"] == "BBB"), "open"].iloc[0]
    bbb_close = panel.loc[(panel["date"] == "2025-01-03") & (panel["ticker"] == "BBB"), "close"].iloc[0]
    assert math.isnan(float(bbb_open))
    assert math.isnan(float(bbb_close))


def test_get_asof_frame_universe_filters_reduce_rows_and_match_conditions(tmp_path):
    db = tmp_path / "x.db"
    init_db(db)
    repo = Repository(db)
    _seed_basic_universe(repo)

    repo.upsert_prices(
        pd.DataFrame(
            [
                {"date": "2025-01-03", "ticker": "AAA", "open": 100, "high": 101, "low": 99, "close": 100, "volume": 1000, "value": 100000},
                {"date": "2025-01-04", "ticker": "AAA", "open": 101, "high": 102, "low": 100, "close": 101, "volume": 1100, "value": 111100},
                {"date": "2025-01-03", "ticker": "BBB", "open": 50, "high": 51, "low": 49, "close": 50, "volume": 500, "value": 50000},
                {"date": "2025-01-04", "ticker": "BBB", "open": 49, "high": 50, "low": 48, "close": 49, "volume": 500, "value": 49000},
            ]
        )
    )
    repo.upsert_cap(
        pd.DataFrame(
            [
                {"date": "2025-01-04", "ticker": "AAA", "mcap": 5_000_000_000, "shares": 1_000_000, "volume": 1100, "value": 111100},
                {"date": "2025-01-04", "ticker": "BBB", "mcap": 500_000_000, "shares": 1_000_000, "volume": 500, "value": 49000},
            ]
        )
    )

    base = repo.get_asof_frame("2025-01-04", window=2)
    filtered = repo.get_asof_frame(
        "2025-01-04",
        window=2,
        min_avg_value_20d=80_000,
        min_mcap=1_000_000_000,
    )

    assert len(base) > len(filtered)
    assert set(filtered["ticker"]) == {"AAA"}
    assert (filtered["avg_value_20d"] >= 80_000).all()
    assert (filtered["mcap"] >= 1_000_000_000).all()
