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
