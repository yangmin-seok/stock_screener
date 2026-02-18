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
    eps REAL,
    bps REAL,
    div REAL,
    dps REAL,
    source_ts TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (date, ticker)
);

CREATE TABLE IF NOT EXISTS snapshot_metrics (
    asof_date TEXT NOT NULL,
    ticker TEXT NOT NULL,
    name TEXT,
    market TEXT,
    close REAL,
    mcap REAL,
    avg_value_20d REAL,
    turnover_20d REAL,
    per REAL,
    pbr REAL,
    div REAL,
    eps REAL,
    bps REAL,
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
    vol_20d REAL,
    ret_1w REAL,
    ret_1m REAL,
    ret_3m REAL,
    ret_6m REAL,
    ret_1y REAL,
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

CREATE INDEX IF NOT EXISTS idx_prices_ticker_date ON prices_daily(ticker, date);
CREATE INDEX IF NOT EXISTS idx_cap_ticker_date ON cap_daily(ticker, date);
CREATE INDEX IF NOT EXISTS idx_fund_ticker_date ON fundamental_daily(ticker, date);
CREATE INDEX IF NOT EXISTS idx_snapshot_asof ON snapshot_metrics(asof_date);
"""


def get_connection(db_path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: str | Path) -> None:
    with get_connection(db_path) as conn:
        conn.executescript(SCHEMA)
        conn.commit()


@contextmanager
def db_session(db_path: str | Path):
    conn = get_connection(db_path)
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()
