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


def test_get_price_window_includes_foreign_flow_columns(tmp_path):
    db = tmp_path / "x.db"
    init_db(db)
    repo = Repository(db)

    repo.upsert_prices(
        pd.DataFrame(
            [
                {
                    "date": "2025-01-31",
                    "ticker": "AAA",
                    "open": 100.0,
                    "high": 110.0,
                    "low": 95.0,
                    "close": 105.0,
                    "volume": 1_000,
                    "value": 105_000.0,
                }
            ]
        )
    )
    repo.upsert_investor_flow(
        pd.DataFrame(
            [
                {
                    "date": "2025-01-31",
                    "ticker": "AAA",
                    "foreign_net_buy_volume": 123.0,
                    "foreign_net_buy_value": 987_654.0,
                }
            ]
        )
    )

    window = repo.get_price_window("2025-01-31", window=10)

    assert "foreign_net_buy_volume" in window.columns
    assert "foreign_net_buy_value" in window.columns
    row = window.iloc[0]
    assert float(row["foreign_net_buy_volume"]) == 123.0
    assert float(row["foreign_net_buy_value"]) == 987_654.0
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



def test_replace_snapshot_persists_new_snapshot_metric_columns(tmp_path):
    db = tmp_path / "x.db"
    init_db(db)
    repo = Repository(db)

    snapshot_row = {
        "asof_date": "2025-01-31",
        "ticker": "AAA",
        "name": "Alpha",
        "market": "KOSPI",
        "close": 101.0,
        "mcap": 1_000_000_000.0,
        "avg_value_20d": 100_000.0,
        "current_value": 101_000.0,
        "relative_value": 1.01,
        "turnover_20d": 0.01,
        "per": 10.0,
        "pbr": 1.2,
        "div": 1.0,
        "dps": 100.0,
        "eps": 200.0,
        "bps": 1000.0,
        "reserve_ratio": 300.0,
        "fiscal_period": "2024Q4",
        "period_type": "quarterly",
        "reported_date": "2025-01-20",
        "consolidation_type": "C",
        "financial_source": "unit",
        "roe_proxy": 0.2,
        "eps_positive": 1,
        "sma20": 100.0,
        "sma50": 100.0,
        "sma200": 100.0,
        "dist_sma20": 0.01,
        "dist_sma50": 0.01,
        "dist_sma200": 0.01,
        "high_52w": 120.0,
        "low_52w": 80.0,
        "pos_52w": 0.525,
        "near_52w_high_ratio": 0.841666,
        "vol_20d": 0.01,
        "rsi_14": 55.0,
        "atr_14": 1.5,
        "gap_pct": -0.01,
        "chg_from_open_pct": 0.02,
        "volatility_20d": 0.18,
        "ret_1w": 0.01,
        "ret_1m": 0.02,
        "ret_3m": 0.03,
        "ret_6m": 0.04,
        "ret_1y": 0.05,
        "eps_cagr_3y": 0.1,
        "eps_cagr_5y": 0.2,
        "eps_yoy_q": 0.11,
        "eps_growth_ttm": 0.12,
        "eps_qoq": 0.13,
        "sales_growth_qoq": 0.14,
        "sales_growth_ttm": 0.15,
        "sales_cagr_3y": 0.16,
        "sales_cagr_5y": 0.17,
        "pe_ratio": 10.0,
        "forward_pe": pd.NA,
        "ps_ratio": 1.5,
        "pb_ratio": 1.2,
        "peg_ratio": 0.8,
        "ps": 1.5,
        "peg": 0.8,
        "ev": pd.NA,
        "ev_sales": pd.NA,
        "ev_ebitda": pd.NA,
        "gross_margin": pd.NA,
        "operating_margin": 0.1,
        "net_margin": 0.08,
        "roa": pd.NA,
        "roe": 0.2,
        "roic": pd.NA,
        "debt_equity": pd.NA,
        "lt_debt_equity": pd.NA,
        "current_ratio": pd.NA,
        "quick_ratio": pd.NA,
        "payout_ratio": 0.5,
        "foreign_net_buy_volume": 1234.0,
        "foreign_net_buy_volume_20d": 1000.0,
        "foreign_net_buy_ratio": 1.234,
        "foreign_net_buy_value": 5_000_000.0,
        "foreign_net_buy_value_20d": 55_000_000.0,
        "eps_cagr_3y_window_years": 3,
        "eps_cagr_3y_asof": "2024Q4",
        "eps_cagr_3y_sample_count": 4,
        "eps_cagr_5y_window_years": 5,
        "eps_cagr_5y_asof": "2024Q4",
        "eps_cagr_5y_sample_count": 8,
        "eps_yoy_q_window_years": 1,
        "eps_yoy_q_asof": "2024Q4",
        "eps_yoy_q_sample_count": 2,
        "sales_cagr_3y_window_years": 3,
        "sales_cagr_3y_asof": "2024Q4",
        "sales_cagr_3y_sample_count": 4,
        "has_price_5y": 0,
        "has_price_10y": 0,
        "calc_version": "v1.4",
    }

    frame = pd.DataFrame([snapshot_row])
    inserted = repo.replace_snapshot("2025-01-31", frame)
    loaded = repo.load_snapshot("2025-01-31")

    assert inserted == 1
    assert len(loaded) == 1
    row = loaded.iloc[0]
    assert float(row["rsi_14"]) == 55.0
    assert float(row["atr_14"]) == 1.5
    assert float(row["gap_pct"]) == -0.01
    assert float(row["foreign_net_buy_volume_20d"]) == 1000.0
    assert float(row["foreign_net_buy_ratio"]) == 1.234
    assert float(row["foreign_net_buy_value_20d"]) == 55_000_000.0


def test_get_trading_dates_prefers_calendar_ticker_with_fallback(tmp_path):
    db = tmp_path / "x.db"
    init_db(db)
    repo = Repository(db)

    repo.upsert_prices(
        pd.DataFrame(
            [
                {"date": "2025-01-02", "ticker": "AAA", "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.5, "volume": 100, "value": 1_000.0},
                {"date": "2025-01-03", "ticker": "AAA", "open": 10.5, "high": 11.5, "low": 10.0, "close": 11.0, "volume": 120, "value": 1_200.0},
            ]
        )
    )

    assert repo.get_trading_dates() == ["2025-01-02", "2025-01-03"]

    repo.upsert_prices(
        pd.DataFrame(
            [
                {"date": "2025-01-03", "ticker": "calendar_ticker", "open": 0.0, "high": 0.0, "low": 0.0, "close": 0.0, "volume": 0.0, "value": 0.0},
                {"date": "2025-01-06", "ticker": "calendar_ticker", "open": 0.0, "high": 0.0, "low": 0.0, "close": 0.0, "volume": 0.0, "value": 0.0},
            ]
        )
    )

    assert repo.get_trading_dates() == ["2025-01-03", "2025-01-06"]


def test_get_asof_frame_computes_rolling_and_periodic_financials(tmp_path):
    db = tmp_path / "x.db"
    init_db(db)
    repo = Repository(db)

    repo.upsert_tickers(
        pd.DataFrame([{"ticker": "AAA", "name": "Alpha", "market": "KOSPI", "active_flag": 1}])
    )

    repo.upsert_prices(
        pd.DataFrame(
            [
                {"date": "2025-01-02", "ticker": "AAA", "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.0, "volume": 100, "value": 1000.0},
                {"date": "2025-01-03", "ticker": "AAA", "open": 10.0, "high": 11.0, "low": 9.0, "close": 11.0, "volume": 100, "value": 2000.0},
                {"date": "2025-01-06", "ticker": "AAA", "open": 11.0, "high": 12.0, "low": 10.0, "close": 12.0, "volume": 100, "value": 3000.0},
            ]
        )
    )
    repo.upsert_investor_flow(
        pd.DataFrame(
            [
                {"date": "2025-01-02", "ticker": "AAA", "foreign_net_buy_volume": 1.0, "foreign_net_buy_value": 10.0},
                {"date": "2025-01-03", "ticker": "AAA", "foreign_net_buy_volume": 2.0, "foreign_net_buy_value": 20.0},
                {"date": "2025-01-06", "ticker": "AAA", "foreign_net_buy_volume": 3.0, "foreign_net_buy_value": 30.0},
            ]
        )
    )
    repo.upsert_financials_periodic(
        pd.DataFrame(
            [
                {
                    "ticker": "AAA",
                    "fiscal_period": "2024-12-31",
                    "period_type": "annual",
                    "reported_date": "2025-01-01",
                    "consolidation_type": "consolidated",
                    "source": "fallback",
                    "revenue": 100.0,
                    "operating_income": 10.0,
                    "net_income": 9.0,
                    "eps": 2.0,
                    "bps": 20.0,
                    "is_correction": 0,
                    "source_priority": 100,
                }
            ]
        )
    )

    frame = repo.get_asof_frame("2025-01-06", window=2)
    row = frame.loc[frame["ticker"] == "AAA"].iloc[0]

    assert float(row["avg_value_20d"]) == 2500.0
    assert float(row["foreign_cum_volume_20d"]) == 5.0
    assert float(row["foreign_cum_value_20d"]) == 50.0
    assert float(row["eps"]) == 2.0
    assert float(row["bps"]) == 20.0
    assert float(row["roe_proxy"]) == 0.1


def test_get_asof_frame_preserves_nulls_for_filter_engine_policy(tmp_path):
    db = tmp_path / "x.db"
    init_db(db)
    repo = Repository(db)

    repo.upsert_tickers(
        pd.DataFrame([{"ticker": "AAA", "name": "Alpha", "market": "KOSPI", "active_flag": 1}])
    )
    repo.upsert_prices(
        pd.DataFrame(
            [
                {"date": "2025-01-02", "ticker": "AAA", "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.0, "volume": 100, "value": None}
            ]
        )
    )

    frame = repo.get_asof_frame("2025-01-02", window=20)
    row = frame.loc[frame["ticker"] == "AAA"].iloc[0]

    assert pd.isna(row["avg_value_20d"])
    assert pd.isna(row["eps"])
    assert pd.isna(row["bps"])


def test_get_price_panel_returns_requested_tickers_and_period(tmp_path):
    db = tmp_path / "x.db"
    init_db(db)
    repo = Repository(db)

    repo.upsert_prices(
        pd.DataFrame(
            [
                {"date": "2025-01-02", "ticker": "AAA", "open": 10.0, "high": 11.0, "low": 9.0, "close": 10.0, "volume": 100, "value": 1000.0},
                {"date": "2025-01-03", "ticker": "AAA", "open": 11.0, "high": 12.0, "low": 10.0, "close": 11.0, "volume": 120, "value": 1200.0},
                {"date": "2025-01-03", "ticker": "BBB", "open": 20.0, "high": 21.0, "low": 19.0, "close": 20.0, "volume": 80, "value": 1600.0},
            ]
        )
    )

    panel = repo.get_price_panel(["BBB", "AAA"], "2025-01-02", "2025-01-03")

    assert panel["ticker"].tolist() == ["AAA", "AAA", "BBB"]
    assert panel["date"].tolist() == ["2025-01-02", "2025-01-03", "2025-01-03"]
    assert list(panel.columns) == ["date", "ticker", "open", "close"]


def test_upsert_collection_checkpoints_bulk(tmp_path):
    db = tmp_path / "x.db"
    init_db(db)
    repo = Repository(db)

    rows = [
        {"ticker": "005930", "last_price_date": "2025-01-15", "last_fundamental_date": None},
        {"ticker": "000660", "last_price_date": None, "last_fundamental_date": "2025-01-10"},
    ]
    inserted = repo.upsert_collection_checkpoints_bulk(rows)

    assert inserted == 2
    cp1 = repo.get_collection_checkpoint("005930")
    cp2 = repo.get_collection_checkpoint("000660")
    assert cp1["last_price_date"] == "2025-01-15"
    assert cp2["last_fundamental_date"] == "2025-01-10"
