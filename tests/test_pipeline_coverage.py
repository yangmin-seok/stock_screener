import pandas as pd

from stock_screener.features.metrics import build_snapshot
from stock_screener.storage.db import init_db
from stock_screener.storage.repository import Repository


def test_build_snapshot_adds_price_coverage_flags():
    days = pd.bdate_range("2014-01-01", periods=2600)
    rows = []
    for dt in days:
        rows.append({"date": dt.strftime("%Y-%m-%d"), "ticker": "AAA", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 1, "value": 1})
    asof = days[-1].strftime("%Y-%m-%d")
    price_window = pd.DataFrame(rows)
    daily = pd.DataFrame(
        [{"ticker": "AAA", "name": "A", "market": "KOSPI", "mcap": 1, "per": 1, "pbr": 1, "div": 1, "dps": 1, "reserve_ratio": 1, "eps": 1, "bps": 1, "fiscal_period": None, "period_type": None, "reported_date": None, "consolidation_type": None, "financial_source": "x"}]
    )
    fund_hist = pd.DataFrame(columns=["date", "ticker", "revenue", "eps", "bps", "fiscal_period", "period_type", "reported_date", "consolidation_type", "source"])

    snapshot = build_snapshot(price_window, daily, fund_hist, asof)
    assert int(snapshot.loc[0, "has_price_5y"]) == 1
    assert int(snapshot.loc[0, "has_price_10y"]) == 1


def test_collection_checkpoint_roundtrip(tmp_path):
    db = tmp_path / "x.db"
    init_db(db)
    repo = Repository(db)

    repo.upsert_collection_checkpoint("005930", last_price_date="2024-01-02")
    repo.upsert_collection_checkpoint("005930", last_fundamental_date="2024-01-31")

    checkpoint = repo.get_collection_checkpoint("005930")
    assert checkpoint["last_price_date"] == "2024-01-02"
    assert checkpoint["last_fundamental_date"] == "2024-01-31"
import sqlite3

from stock_screener.pipelines.daily_batch import DailyBatchPipeline


class _FakeCollector:
    def recent_business_day(self):
        return pd.Timestamp("2025-01-15").date()

    def tickers(self):
        return pd.DataFrame([{"ticker": "005930", "name": "Samsung", "market": "KOSPI", "active_flag": 1}])

    def ohlcv(self, from_dt, to_dt, ticker):
        return pd.DataFrame(
            [
                {
                    "date": "2025-01-15",
                    "ticker": ticker,
                    "open": 1,
                    "high": 1,
                    "low": 1,
                    "close": 1,
                    "volume": 1,
                    "value": 1,
                }
            ]
        )

    def trading_dates(self, from_dt, to_dt):
        return [pd.Timestamp("2025-01-15").date()]

    def market_cap(self, dt):
        return pd.DataFrame([{"date": "2025-01-15", "ticker": "005930", "mcap": 100, "shares": 1, "volume": 1, "value": 1}])

    def fundamental_market_metrics(self, dt):
        return pd.DataFrame([{"date": "2025-01-15", "ticker": "005930", "per": 1.0, "pbr": 1.0, "div": 0.0, "dps": 0.0}])


def test_daily_batch_financial_provider_order(monkeypatch, tmp_path):
    monkeypatch.setenv("DART_API_KEY", "dummy-key")
    pipeline = DailyBatchPipeline(tmp_path / "x.db")

    pipeline._ensure_dart_client()

    providers = [provider.config.source_name for provider in pipeline.financial_providers]
    assert providers == ["dart_primary", "pykrx_fallback"]


def test_daily_batch_chunk_logs_financial_non_null_counts(monkeypatch, tmp_path):
    monkeypatch.setenv("DART_API_KEY", "dummy-key")
    pipeline = DailyBatchPipeline(tmp_path / "x.db")
    pipeline.collector = _FakeCollector()

    financial_frame = pd.DataFrame(
        [
            {
                "ticker": "005930",
                "fiscal_period": "2024-12-31",
                "period_type": "annual",
                "reported_date": "2025-01-15",
                "consolidation_type": "consolidated",
                "revenue": 1000.0,
                "operating_income": 100.0,
                "net_income": 80.0,
                "eps": 10.0,
                "bps": None,
                "source": "dart_primary",
                "source_priority": 300,
                "is_correction": 0,
            },
            {
                "ticker": "000660",
                "fiscal_period": "2024-12-31",
                "period_type": "annual",
                "reported_date": "2025-01-15",
                "consolidation_type": "consolidated",
                "revenue": None,
                "operating_income": 40.0,
                "net_income": 25.0,
                "eps": None,
                "bps": 20.0,
                "source": "dart_primary",
                "source_priority": 300,
                "is_correction": 0,
            },
        ]
    )
    monkeypatch.setattr(pipeline, "_collect_financials", lambda dt: financial_frame)

    pipeline.run(
        asof_date="2025-01-15",
        lookback_days=5,
        initial_backfill=False,
        chunk_years=1,
        chunks=1,
        rebuild_snapshot=False,
    )

    with sqlite3.connect(tmp_path / "x.db") as conn:
        message = conn.execute(
            "SELECT message FROM job_log WHERE run_id = ? AND stage = ?",
            ("daily_batch:2025-01-15", "fundamental_chunk_1"),
        ).fetchone()[0]

    assert "eps_non_null=1" in message
    assert "bps_non_null=1" in message
    assert "revenue_non_null=1" in message
