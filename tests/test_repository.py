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


def test_get_latest_batch_chunk_report_parses_quality_metrics(tmp_path):
    db = tmp_path / "x.db"
    init_db(db)
    repo = Repository(db)

    repo.log_job_stage(
        run_id="daily_batch:2026-02-20",
        stage="fundamental_chunk_1",
        status="success",
        row_count=120,
        message=(
            "chunk=1/2, range=2024-01-01~2024-12-31, "
            "eps_non_null=80, bps_non_null=70, revenue_non_null=90"
        ),
    )
    repo.log_job_stage(
        run_id="daily_batch:2026-02-20",
        stage="fundamental_chunk_2",
        status="failed",
        row_count=100,
        message=(
            "chunk=2/2, range=2023-01-01~2023-12-31, "
            "eps_non_null=40/100, bps_non_null=50/100, revenue_non_null=60/100"
        ),
    )

    rows = repo.get_latest_batch_chunk_report()

    assert len(rows) == 2
    assert rows[0]["chunk_idx"] == 1
    assert rows[0]["status"] == "success"
    assert rows[0]["eps_non_null"] == 80
    assert rows[0]["eps_ratio"] == "80/120 (66.7%)"
    assert rows[0]["bps_ratio"] == "70/120 (58.3%)"
    assert rows[0]["revenue_ratio"] == "90/120 (75.0%)"

    assert rows[1]["chunk_idx"] == 2
    assert rows[1]["status"] == "failed"
    assert rows[1]["eps_non_null"] == 40
    assert rows[1]["eps_ratio"] == "40/100 (40.0%)"
    assert rows[1]["bps_ratio"] == "50/100 (50.0%)"
    assert rows[1]["revenue_ratio"] == "60/100 (60.0%)"


def test_get_latest_batch_chunk_report_returns_latest_run(tmp_path):
    db = tmp_path / "x.db"
    init_db(db)
    repo = Repository(db)

    repo.log_job_stage(
        run_id="daily_batch:2026-02-19",
        stage="fundamental_chunk_1",
        status="success",
        row_count=10,
        message="chunk=1/1, eps_non_null=7",
    )
    repo.log_job_stage(
        run_id="daily_batch:2026-02-20",
        stage="fundamental_chunk_1",
        status="success",
        row_count=20,
        message="chunk=1/1, eps_non_null=11",
    )

    rows = repo.get_latest_batch_chunk_report()
    assert len(rows) == 1
    assert rows[0]["run_id"] == "daily_batch:2026-02-20"
    assert rows[0]["eps_ratio"] == "11/20 (55.0%)"


def test_financial_quality_reports_and_top_null_tickers(tmp_path):
    db = tmp_path / "x.db"
    init_db(db)
    repo = Repository(db)

    quality_rows = pd.DataFrame(
        [
            {
                "asof_date": "2026-02-20",
                "metric_date": "2026-02-19",
                "chunk_idx": 1,
                "metric_scope": "source",
                "provider": "",
                "source": "dart_primary",
                "fiscal_period": "",
                "period_type": "",
                "ticker": "",
                "rows_total": 10,
                "eps_null": 2,
                "bps_null": 1,
            },
            {
                "asof_date": "2026-02-20",
                "metric_date": "2026-02-19",
                "chunk_idx": 1,
                "metric_scope": "source",
                "provider": "",
                "source": "pykrx_fallback",
                "fiscal_period": "",
                "period_type": "",
                "ticker": "",
                "rows_total": 5,
                "eps_null": 1,
                "bps_null": 2,
            },
            {
                "asof_date": "2026-02-20",
                "metric_date": "2026-02-19",
                "chunk_idx": 1,
                "metric_scope": "period",
                "provider": "",
                "source": "",
                "fiscal_period": "2025Q4",
                "period_type": "quarterly",
                "ticker": "",
                "rows_total": 8,
                "eps_null": 2,
                "bps_null": 1,
            },
            {
                "asof_date": "2026-02-20",
                "metric_date": "2026-02-19",
                "chunk_idx": 1,
                "metric_scope": "period",
                "provider": "",
                "source": "",
                "fiscal_period": "2025",
                "period_type": "annual",
                "ticker": "",
                "rows_total": 7,
                "eps_null": 1,
                "bps_null": 2,
            },
            {
                "asof_date": "2026-02-20",
                "metric_date": "2026-02-19",
                "chunk_idx": 1,
                "metric_scope": "ticker",
                "provider": "",
                "source": "",
                "fiscal_period": "",
                "period_type": "",
                "ticker": "005930",
                "rows_total": 3,
                "eps_null": 1,
                "bps_null": 1,
            },
            {
                "asof_date": "2026-02-20",
                "metric_date": "2026-02-19",
                "chunk_idx": 1,
                "metric_scope": "ticker",
                "provider": "",
                "source": "",
                "fiscal_period": "",
                "period_type": "",
                "ticker": "000660",
                "rows_total": 4,
                "eps_null": 2,
                "bps_null": 1,
            },
            {
                "asof_date": "2026-02-20",
                "metric_date": "2026-02-19",
                "chunk_idx": 1,
                "metric_scope": "ticker",
                "provider": "",
                "source": "",
                "fiscal_period": "",
                "period_type": "",
                "ticker": "035420",
                "rows_total": 2,
                "eps_null": 0,
                "bps_null": 2,
            },
        ]
    )

    inserted = repo.upsert_financial_quality(quality_rows)
    assert inserted == 7

    source_report = repo.get_financial_quality_source_report("2026-02-20")
    assert source_report["source"].tolist() == ["dart_primary", "pykrx_fallback"]
    assert source_report.loc[source_report["source"] == "dart_primary", "eps_null_ratio"].iloc[0] == 0.2
    assert source_report.loc[source_report["source"] == "pykrx_fallback", "bps_null_ratio"].iloc[0] == 0.4

    period_report = repo.get_financial_quality_period_report("2026-02-20")
    assert len(period_report) == 2
    q_row = period_report.loc[period_report["period_type"] == "quarterly"].iloc[0]
    assert q_row["fiscal_period"] == "2025Q4"
    assert q_row["eps_null_ratio"] == 0.25

    top_tickers = repo.get_financial_quality_top_null_tickers("2026-02-20", limit=2)
    assert top_tickers["ticker"].tolist() == ["000660", "005930"]
    assert top_tickers.iloc[0]["null_total"] == 3
