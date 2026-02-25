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
import sqlite3

from stock_screener.collectors.dart_financial_provider import DartFinancialProvider


def test_upsert_financials_periodic_applies_conflict_priority(tmp_path):
    db = tmp_path / "x.db"
    init_db(db)
    repo = Repository(db)

    frame = pd.DataFrame(
        [
            {
                "ticker": "005930",
                "fiscal_period": "2024-12-31",
                "period_type": "annual",
                "reported_date": "2025-03-20",
                "consolidation_type": "consolidated",
                "source": "low_priority",
                "revenue": 900.0,
                "operating_income": 90.0,
                "net_income": 80.0,
                "eps": 9.0,
                "bps": 90.0,
                "is_correction": 0,
                "source_priority": 100,
            },
            {
                "ticker": "005930",
                "fiscal_period": "2024-12-31",
                "period_type": "annual",
                "reported_date": "2025-03-25",
                "consolidation_type": "consolidated",
                "source": "high_priority",
                "revenue": 1000.0,
                "operating_income": 100.0,
                "net_income": 85.0,
                "eps": 10.0,
                "bps": 100.0,
                "is_correction": 0,
                "source_priority": 300,
            },
            {
                "ticker": "005930",
                "fiscal_period": "2024-12-31",
                "period_type": "annual",
                "reported_date": "2025-03-22",
                "consolidation_type": "consolidated",
                "source": "correction",
                "revenue": 1100.0,
                "operating_income": 105.0,
                "net_income": 88.0,
                "eps": 11.0,
                "bps": 110.0,
                "is_correction": 1,
                "source_priority": 200,
            },
        ]
    )

    repo.upsert_financials_periodic(frame)

    with sqlite3.connect(db) as conn:
        row = conn.execute(
            """
            SELECT source, reported_date, is_correction, source_priority, eps, bps, revenue
            FROM financials_periodic
            WHERE ticker = ? AND fiscal_period = ? AND period_type = ? AND consolidation_type = ?
            """,
            ("005930", "2024-12-31", "annual", "consolidated"),
        ).fetchone()

    assert row == ("correction", "2025-03-22", 1, 200, 11.0, 110.0, 1100.0)


def test_upsert_financial_tables_accept_dart_normalized_rows(tmp_path):
    db = tmp_path / "x.db"
    init_db(db)
    repo = Repository(db)

    payload = [
        {
            "stock_code": "5930",
            "bsns_year": "2024",
            "reprt_code": "11011",
            "rcept_dt": "20250331",
            "fs_div": "CFS",
            "thstrm_amount": "1,234",
            "thstrm_operating_income": "456",
            "thstrm_net_income": "321",
            "thstrm_eps": "12.5",
            "thstrm_bps": "150.7",
        }
    ]
    normalized = DartFinancialProvider.normalize_dart_financials(payload, pd.Timestamp("2025-03-31").date())

    daily_rows = repo.upsert_financials(normalized, "2025-03-31")
    periodic_rows = repo.upsert_financials_periodic(normalized)

    assert daily_rows == 1
    assert periodic_rows == 1

    with sqlite3.connect(db) as conn:
        daily = pd.read_sql_query(
            "SELECT ticker, source, revenue, eps, bps, source_priority FROM financials_daily",
            conn,
        ).iloc[0]
        periodic = pd.read_sql_query(
            "SELECT ticker, source, revenue, eps, bps, source_priority FROM financials_periodic",
            conn,
        ).iloc[0]

    assert daily["ticker"] == "005930"
    assert daily["source"] == "dart_primary"
    assert pd.notna(daily["revenue"])
    assert float(daily["eps"]) == 12.5
    assert float(daily["bps"]) == 150.7
    assert int(daily["source_priority"]) == 300

    assert periodic["ticker"] == "005930"
    assert periodic["source"] == "dart_primary"
    assert pd.notna(periodic["revenue"])
    assert float(periodic["eps"]) == 12.5
    assert float(periodic["bps"]) == 150.7
    assert int(periodic["source_priority"]) == 300
