from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS ticker_master (
    ticker TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    market TEXT NOT NULL,
    active_flag INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS prices_daily (
    date TEXT NOT NULL,
    ticker TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    value REAL,
    source_ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, ticker)
);

CREATE TABLE IF NOT EXISTS cap_daily (
    date TEXT NOT NULL,
    ticker TEXT NOT NULL,
    mcap REAL,
    shares REAL,
    volume REAL,
    value REAL,
    source_ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, ticker)
);

CREATE TABLE IF NOT EXISTS fundamental_daily (
    date TEXT NOT NULL,
    ticker TEXT NOT NULL,
    per REAL,
    pbr REAL,
    div REAL,
    dps REAL,
    reserve_ratio REAL,
    source_ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, ticker)
);

CREATE TABLE IF NOT EXISTS financials_daily (
    date TEXT NOT NULL,
    ticker TEXT NOT NULL,
    fiscal_period TEXT,
    period_type TEXT,
    reported_date TEXT,
    consolidation_type TEXT,
    source TEXT,
    revenue REAL,
    operating_income REAL,
    net_income REAL,
    eps REAL,
    bps REAL,
    source_ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, ticker, fiscal_period, period_type, consolidation_type)
);

-- financials_daily is kept as collection-time audit history.
CREATE TABLE IF NOT EXISTS financials_periodic (
    ticker TEXT NOT NULL,
    fiscal_period TEXT NOT NULL,
    period_type TEXT NOT NULL,
    consolidation_type TEXT NOT NULL,
    reported_date TEXT,
    source TEXT,
    revenue REAL,
    operating_income REAL,
    net_income REAL,
    eps REAL,
    bps REAL,
    source_ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    is_correction INTEGER,
    source_priority INTEGER,
    PRIMARY KEY (ticker, fiscal_period, period_type, consolidation_type)
);

CREATE TABLE IF NOT EXISTS snapshot_metrics (
    asof_date TEXT NOT NULL,
    ticker TEXT NOT NULL,
    name TEXT,
    market TEXT,
    close REAL,
    mcap REAL,
    avg_value_20d REAL,
    current_value REAL,
    relative_value REAL,
    turnover_20d REAL,
    per REAL,
    pbr REAL,
    div REAL,
    dps REAL,
    eps REAL,
    bps REAL,
    reserve_ratio REAL,
    fiscal_period TEXT,
    period_type TEXT,
    reported_date TEXT,
    consolidation_type TEXT,
    financial_source TEXT,
    roe_proxy REAL,
    eps_positive INTEGER,
    sma20 REAL,
    sma50 REAL,
    sma200 REAL,
    dist_sma20 REAL,
    dist_sma50 REAL,
    dist_sma200 REAL,
    high_52w REAL,
    low_52w REAL,
    pos_52w REAL,
    near_52w_high_ratio REAL,
    vol_20d REAL,
    ret_1w REAL,
    ret_1m REAL,
    ret_3m REAL,
    ret_6m REAL,
    ret_1y REAL,
    eps_cagr_5y REAL,
    eps_yoy_q REAL,
    eps_growth_ttm REAL,
    sales_growth_qoq REAL,
    eps_cagr_5y_window_years INTEGER,
    eps_cagr_5y_asof TEXT,
    eps_cagr_5y_sample_count INTEGER,
    eps_yoy_q_window_years INTEGER,
    eps_yoy_q_asof TEXT,
    eps_yoy_q_sample_count INTEGER,
    has_price_5y INTEGER,
    has_price_10y INTEGER,
    calc_version TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (asof_date, ticker)
);

CREATE TABLE IF NOT EXISTS job_log (
    run_id TEXT NOT NULL,
    stage TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    message TEXT,
    row_count INTEGER,
    PRIMARY KEY (run_id, stage)
);

CREATE TABLE IF NOT EXISTS collection_checkpoint (
    ticker TEXT PRIMARY KEY,
    last_price_date TEXT,
    last_fundamental_date TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS batch_checkpoint (
    checkpoint_key TEXT PRIMARY KEY,
    checkpoint_value TEXT,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_prices_ticker_date ON prices_daily(ticker, date);
CREATE INDEX IF NOT EXISTS idx_cap_ticker_date ON cap_daily(ticker, date);
CREATE INDEX IF NOT EXISTS idx_fund_ticker_date ON fundamental_daily(ticker, date);
CREATE INDEX IF NOT EXISTS idx_fin_ticker_date ON financials_daily(ticker, date);
CREATE INDEX IF NOT EXISTS idx_fin_periodic_ticker_period ON financials_periodic(ticker, fiscal_period);
CREATE INDEX IF NOT EXISTS idx_snapshot_asof ON snapshot_metrics(asof_date);
"""


def get_connection(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, col_type: str) -> None:
    cols = {row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")


def init_db(db_path: str | Path) -> None:
    with get_connection(db_path) as conn:
        conn.executescript(SCHEMA)
        _ensure_column(conn, "financials_periodic", "reported_date", "TEXT")
        _ensure_column(conn, "financials_periodic", "source", "TEXT")
        _ensure_column(conn, "financials_periodic", "revenue", "REAL")
        _ensure_column(conn, "financials_periodic", "operating_income", "REAL")
        _ensure_column(conn, "financials_periodic", "net_income", "REAL")
        _ensure_column(conn, "financials_periodic", "eps", "REAL")
        _ensure_column(conn, "financials_periodic", "bps", "REAL")
        _ensure_column(conn, "financials_periodic", "source_ts", "TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP")
        _ensure_column(conn, "financials_periodic", "is_correction", "INTEGER")
        _ensure_column(conn, "financials_periodic", "source_priority", "INTEGER")
        _ensure_column(conn, "snapshot_metrics", "dps", "REAL")
        _ensure_column(conn, "fundamental_daily", "reserve_ratio", "REAL")
        _ensure_column(conn, "fundamental_daily", "div", "REAL")
        _ensure_column(conn, "fundamental_daily", "dps", "REAL")
        _ensure_column(conn, "snapshot_metrics", "reserve_ratio", "REAL")
        _ensure_column(conn, "snapshot_metrics", "current_value", "REAL")
        _ensure_column(conn, "snapshot_metrics", "relative_value", "REAL")
        _ensure_column(conn, "snapshot_metrics", "near_52w_high_ratio", "REAL")
        _ensure_column(conn, "snapshot_metrics", "eps_cagr_5y", "REAL")
        _ensure_column(conn, "snapshot_metrics", "eps_yoy_q", "REAL")
        _ensure_column(conn, "snapshot_metrics", "eps_growth_ttm", "REAL")
        _ensure_column(conn, "snapshot_metrics", "sales_growth_qoq", "REAL")
        _ensure_column(conn, "snapshot_metrics", "eps_cagr_5y_window_years", "INTEGER")
        _ensure_column(conn, "snapshot_metrics", "eps_cagr_5y_asof", "TEXT")
        _ensure_column(conn, "snapshot_metrics", "eps_cagr_5y_sample_count", "INTEGER")
        _ensure_column(conn, "snapshot_metrics", "eps_yoy_q_window_years", "INTEGER")
        _ensure_column(conn, "snapshot_metrics", "eps_yoy_q_asof", "TEXT")
        _ensure_column(conn, "snapshot_metrics", "eps_yoy_q_sample_count", "INTEGER")
        _ensure_column(conn, "snapshot_metrics", "has_price_5y", "INTEGER")
        _ensure_column(conn, "snapshot_metrics", "has_price_10y", "INTEGER")
        _ensure_column(conn, "snapshot_metrics", "fiscal_period", "TEXT")
        _ensure_column(conn, "snapshot_metrics", "period_type", "TEXT")
        _ensure_column(conn, "snapshot_metrics", "reported_date", "TEXT")
        _ensure_column(conn, "snapshot_metrics", "consolidation_type", "TEXT")
        _ensure_column(conn, "snapshot_metrics", "financial_source", "TEXT")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_fin_periodic_ticker_period ON financials_periodic(ticker, fiscal_period)"
        )
        conn.commit()


@contextmanager
def db_session(db_path: str | Path):
    conn = get_connection(db_path)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()
