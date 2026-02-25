import pandas as pd

from stock_screener.storage.db import init_db
from stock_screener.storage.repository import Repository


def test_to_sql_records_converts_pd_na_to_none(tmp_path):
    repo = Repository(tmp_path / "x.db")
    df = pd.DataFrame({"a": [1, pd.NA], "b": ["x", pd.NA]})
    rows = repo._to_sql_records(df, ["a", "b"])
    assert rows[0] == (1, "x")
    assert rows[1] == (None, None)


def test_get_daily_join_prefers_non_null_financials_over_newer_null_row(tmp_path):
    db = tmp_path / "x.db"
    init_db(db)
    repo = Repository(db)

    repo.upsert_tickers(
        pd.DataFrame([{"ticker": "AAA", "name": "Alpha", "market": "KOSPI", "active_flag": 1}])
    )

    old_non_null = pd.DataFrame(
        [
            {
                "ticker": "AAA",
                "fiscal_period": "2024Q3",
                "period_type": "quarterly",
                "reported_date": "2024-11-20",
                "consolidation_type": "C",
                "source": "fallback",
                "revenue": 100.0,
                "operating_income": 10.0,
                "net_income": 8.0,
                "eps": 1.23,
                "bps": 10.0,
                "is_correction": 0,
                "source_priority": 1,
            }
        ]
    )
    newer_null = pd.DataFrame(
        [
            {
                "ticker": "AAA",
                "fiscal_period": "2024Q4",
                "period_type": "quarterly",
                "reported_date": "2025-02-10",
                "consolidation_type": "C",
                "source": "primary",
                "revenue": 120.0,
                "operating_income": 12.0,
                "net_income": 9.0,
                "eps": None,
                "bps": None,
                "is_correction": 0,
                "source_priority": 300,
            }
        ]
    )

    repo.upsert_financials(old_non_null, asof_date="2024-12-01")
    repo.upsert_financials(newer_null, asof_date="2025-02-15")

    daily = repo.get_daily_join("2025-02-20")
    row = daily.loc[daily["ticker"] == "AAA"].iloc[0]

    assert row["eps"] == 1.23
    assert row["bps"] == 10.0
    assert row["fiscal_period"] == "2024Q3"
    assert row["period_type"] == "quarterly"
    assert row["reported_date"] == "2024-11-20"
    assert row["financial_source"] == "fallback"
