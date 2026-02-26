import pandas as pd

from stock_screener.backtest.config import BacktestConfig
from stock_screener.backtest.engine import run_backtest


class FakeRepo:
    def __init__(self) -> None:
        self.trading_dates = [
            "2025-01-02",
            "2025-01-03",
            "2025-01-06",
            "2025-01-07",
            "2025-01-08",
            "2025-01-09",
            "2025-01-10",
            "2025-01-13",
            "2025-01-14",
            "2025-01-15",
        ]

    def get_trading_dates(self) -> list[str]:
        return self.trading_dates

    def get_asof_frame(self, dt: str, foreign_window: int = 20) -> pd.DataFrame:
        base = {
            "ticker": ["AAA", "BBB"],
            "name": ["Alpha", "Beta"],
            "market": ["KOSPI", "KOSDAQ"],
            "pbr": [0.7, 1.5],
            "per": [8.0, 9.0],
            "roe_proxy": [0.12, 0.11],
            "foreign_cum_value_20d": [5000.0, 1000.0],
            "foreign_cum_volume_20d": [100.0, 200.0],
            "foreign_pressure_by_avg_value": [0.1, 0.02],
            "foreign_pressure_by_mcap": [0.01, 0.005],
        }
        if dt >= "2025-01-08":
            base["foreign_cum_value_20d"] = [500.0, 6000.0]
        return pd.DataFrame(base)

    def get_price_panel(self, tickers: list[str], start_date: str, end_date: str) -> pd.DataFrame:
        rows: list[dict[str, object]] = []
        for dt in self.trading_dates:
            if not (start_date <= dt <= end_date):
                continue
            if "AAA" in tickers:
                rows.append({"date": dt, "ticker": "AAA", "open": 100.0, "close": 100.0})
            if "BBB" in tickers:
                rows.append({"date": dt, "ticker": "BBB", "open": 50.0, "close": 50.0})
        return pd.DataFrame(rows)


def _cfg(fee_bps: float = 0.0, filters: dict | None = None) -> BacktestConfig:
    return BacktestConfig(
        run={
            "name": "smoke",
            "start_date": "2025-01-02",
            "end_date": "2025-01-15",
            "rebalance": "W",
            "initial_capital": 1_000_000,
        },
        universe={},
        filters=filters or {},
        selection={
            "mode": "cap_n",
            "cap_n": 1,
            "sort_by": "foreign_cum_value_20d",
            "sort_direction": "desc",
            "empty_selection_policy": "cash",
        },
        portfolio={"weighting": "equal"},
        costs={"fee_bps": fee_bps, "slippage_bps": 0.0},
        output={},
    )


def test_engine_smoke_produces_equity_curve_and_rebalances():
    repo = FakeRepo()
    result = run_backtest(_cfg(), repo)

    assert not result["equity_curve"].empty
    assert len(result["rebalance_log"]) >= 1
    assert result["summary"]["rebalances"] >= 1


def test_engine_filter_changes_selected_ticker():
    repo = FakeRepo()
    filters = {
        "foreign_cum": {
            "enabled": True,
            "field": "foreign_cum",
            "op": "gte",
            "value": 3000.0,
            "unit": "value",
            "normalize": "none",
        }
    }
    result = run_backtest(_cfg(filters=filters), repo)
    selected = result["rebalance_log"]["selected_tickers"].tolist()

    assert any("AAA" in tickers for tickers in selected)
    assert any("BBB" in tickers for tickers in selected)


def test_engine_costs_reduce_final_equity():
    repo = FakeRepo()

    res_no_cost = run_backtest(_cfg(fee_bps=0.0), repo)
    res_cost = run_backtest(_cfg(fee_bps=50.0), repo)

    assert res_cost["summary"]["final_equity"] < res_no_cost["summary"]["final_equity"]


def test_engine_handles_empty_equity_records_for_monthly_single_month():
    repo = FakeRepo()
    config = BacktestConfig(
        run={
            "name": "empty-equity",
            "start_date": "2025-01-02",
            "end_date": "2025-01-15",
            "rebalance": "M",
            "initial_capital": 1_000_000,
        },
        universe={},
        filters={},
        selection={
            "mode": "cap_n",
            "cap_n": 1,
            "sort_by": "foreign_cum_value_20d",
            "sort_direction": "desc",
            "empty_selection_policy": "cash",
        },
        portfolio={"weighting": "equal"},
        costs={"fee_bps": 0.0, "slippage_bps": 0.0},
        output={},
    )

    result = run_backtest(config, repo)

    assert isinstance(result, dict)
    assert result["equity_curve"].empty
    assert list(result["equity_curve"].columns) == ["date", "equity_close", "return"]
    assert result["summary"]["final_equity"] == 1_000_000


def test_engine_weekly_equity_curve_contains_intermediate_trading_days():
    repo = FakeRepo()
    result = run_backtest(_cfg(), repo)

    equity_dates = result["equity_curve"]["date"].tolist()
    rebalance_exec_dates = result["rebalance_log"]["exec_date"].tolist()

    assert len(equity_dates) > len(rebalance_exec_dates)
    assert any(date not in rebalance_exec_dates for date in equity_dates)


def test_engine_monthly_equity_curve_contains_intermediate_trading_days():
    repo = FakeRepo()
    config = BacktestConfig(
        run={
            "name": "monthly-with-middle-days",
            "start_date": "2025-01-02",
            "end_date": "2025-02-20",
            "rebalance": "M",
            "initial_capital": 1_000_000,
        },
        universe={},
        filters={},
        selection={
            "mode": "cap_n",
            "cap_n": 1,
            "sort_by": "foreign_cum_value_20d",
            "sort_direction": "desc",
            "empty_selection_policy": "cash",
        },
        portfolio={"weighting": "equal"},
        costs={"fee_bps": 0.0, "slippage_bps": 0.0},
        output={},
    )
    repo.trading_dates = [
        "2025-01-02",
        "2025-01-03",
        "2025-01-06",
        "2025-01-07",
        "2025-01-08",
        "2025-01-31",
        "2025-02-03",
        "2025-02-04",
        "2025-02-05",
        "2025-02-06",
        "2025-02-07",
        "2025-02-10",
        "2025-02-11",
        "2025-02-12",
        "2025-02-13",
        "2025-02-14",
        "2025-02-20",
    ]

    result = run_backtest(config, repo)
    equity_dates = result["equity_curve"]["date"].tolist()
    rebalance_exec_dates = result["rebalance_log"]["exec_date"].tolist()

    assert len(equity_dates) > len(rebalance_exec_dates)
    assert "2025-02-04" in equity_dates
    assert "2025-02-04" not in rebalance_exec_dates


def test_engine_counts_skipped_rebalance_when_signal_is_last_trading_day():
    repo = FakeRepo()
    config = BacktestConfig(
        run={
            "name": "skip-last-day",
            "start_date": "2025-01-02",
            "end_date": "2025-01-15",
            "rebalance": "M",
            "initial_capital": 1_000_000,
        },
        universe={},
        filters={},
        selection={
            "mode": "cap_n",
            "cap_n": 1,
            "sort_by": "foreign_cum_value_20d",
            "sort_direction": "desc",
            "empty_selection_policy": "cash",
        },
        portfolio={"weighting": "equal"},
        costs={"fee_bps": 0.0, "slippage_bps": 0.0},
        output={},
    )

    result = run_backtest(config, repo)

    assert result["summary"]["rebalances"] == 0
    assert result["summary"]["skipped_rebalances"] == 1
    assert result["summary"]["skipped_rebalance_reasons"] == {"no_next_trading_day": 1}

    rebalance_log = result["rebalance_log"]
    assert len(rebalance_log) == 1
    assert bool(rebalance_log.iloc[0]["skipped"])
    assert rebalance_log.iloc[0]["signal_date"] == "2025-01-15"
    assert rebalance_log.iloc[0]["skip_reason"] == "no_next_trading_day"


def test_engine_counts_skipped_rebalance_when_yearly_signal_is_last_trading_day():
    repo = FakeRepo()
    repo.trading_dates = [
        "2024-01-02",
        "2024-06-03",
        "2024-12-30",
        "2025-01-03",
        "2025-07-01",
        "2025-12-29",
    ]
    config = BacktestConfig(
        run={
            "name": "yearly-skip-last-day",
            "start_date": "2024-01-02",
            "end_date": "2025-12-29",
            "rebalance": "Y",
            "initial_capital": 1_000_000,
        },
        universe={},
        filters={},
        selection={
            "mode": "cap_n",
            "cap_n": 1,
            "sort_by": "foreign_cum_value_20d",
            "sort_direction": "desc",
            "empty_selection_policy": "cash",
        },
        portfolio={"weighting": "equal"},
        costs={"fee_bps": 0.0, "slippage_bps": 0.0},
        output={},
    )

    result = run_backtest(config, repo)

    assert result["summary"]["rebalances"] == 1
    assert result["summary"]["skipped_rebalances"] == 1
    assert result["summary"]["skipped_rebalance_reasons"] == {"no_next_trading_day": 1}

    rebalance_log = result["rebalance_log"]
    assert len(rebalance_log) == 2
    assert rebalance_log.iloc[0]["signal_date"] == "2024-12-30"
    assert rebalance_log.iloc[0]["exec_date"] == "2025-01-03"
    assert bool(rebalance_log.iloc[1]["skipped"])
    assert rebalance_log.iloc[1]["signal_date"] == "2025-12-29"
    assert rebalance_log.iloc[1]["skip_reason"] == "no_next_trading_day"


def test_engine_exposes_run_log_with_selection_and_rebalance_steps():
    repo = FakeRepo()
    result = run_backtest(_cfg(), repo)

    run_log = result["run_log"]
    assert isinstance(run_log, pd.DataFrame)
    assert not run_log.empty
    assert {"stage", "status", "universe_count", "filtered_count", "selected_count"}.issubset(run_log.columns)
    assert "selection" in run_log["stage"].tolist()
    assert "rebalance" in run_log["stage"].tolist()


def test_engine_run_log_records_skip_when_no_exec_date():
    repo = FakeRepo()
    config = BacktestConfig(
        run={
            "name": "skip-last-day-run-log",
            "start_date": "2025-01-02",
            "end_date": "2025-01-15",
            "rebalance": "M",
            "initial_capital": 1_000_000,
        },
        universe={},
        filters={},
        selection={
            "mode": "cap_n",
            "cap_n": 1,
            "sort_by": "foreign_cum_value_20d",
            "sort_direction": "desc",
            "empty_selection_policy": "cash",
        },
        portfolio={"weighting": "equal"},
        costs={"fee_bps": 0.0, "slippage_bps": 0.0},
        output={},
    )

    result = run_backtest(config, repo)
    run_log = result["run_log"]
    assert len(run_log) == 1
    assert run_log.iloc[0]["status"] == "skipped"
    assert run_log.iloc[0]["stage"] == "schedule"
    assert run_log.iloc[0]["message"] == "no_next_trading_day"


def test_engine_progress_callback_emits_done_event_and_summary_timing():
    repo = FakeRepo()
    events: list[dict[str, object]] = []

    def _cb(evt: dict[str, object]) -> None:
        events.append(evt)

    result = run_backtest(_cfg(), repo, progress_callback=_cb)

    assert events
    assert events[-1].get("stage") == "done"
    assert float(result["summary"].get("elapsed_seconds", 0.0)) >= 0.0
    assert int(result["summary"].get("signals_total", 0)) >= 1
