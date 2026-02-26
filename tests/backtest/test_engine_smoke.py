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
