from stock_screener.backtest.calendar import make_rebalance_signal_dates


def test_make_rebalance_signal_dates_yearly_month_end_anchor():
    trading_dates = [
        "2024-01-02",
        "2024-06-03",
        "2024-12-30",
        "2025-01-03",
        "2025-07-01",
        "2025-12-29",
    ]

    signal_dates = make_rebalance_signal_dates(trading_dates, rule="Y", anchor="month_end")

    assert signal_dates == ["2024-12-30", "2025-12-29"]


def test_make_rebalance_signal_dates_yearly_month_start_anchor():
    trading_dates = [
        "2024-01-02",
        "2024-06-03",
        "2024-12-30",
        "2025-01-03",
        "2025-07-01",
        "2025-12-29",
    ]

    signal_dates = make_rebalance_signal_dates(trading_dates, rule="YEARLY", anchor="month_start")

    assert signal_dates == ["2024-01-02", "2025-01-03"]
