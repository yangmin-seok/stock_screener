from __future__ import annotations

from pathlib import Path

import pandas as pd

from stock_screener.storage.db import db_session


class Repository:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    def upsert_tickers(self, frame: pd.DataFrame) -> int:
        if frame.empty:
            return 0
        rows = frame[["ticker", "name", "market", "active_flag"]].to_records(index=False).tolist()
        with db_session(self.db_path) as conn:
            conn.executemany(
                """
                INSERT INTO ticker_master(ticker, name, market, active_flag, updated_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(ticker) DO UPDATE SET
                    name=excluded.name,
                    market=excluded.market,
                    active_flag=excluded.active_flag,
                    updated_at=CURRENT_TIMESTAMP
                """,
                rows,
            )
        return len(rows)

    def upsert_prices(self, frame: pd.DataFrame) -> int:
        if frame.empty:
            return 0
        rows = frame[["date", "ticker", "open", "high", "low", "close", "volume", "value"]].to_records(index=False).tolist()
        with db_session(self.db_path) as conn:
            conn.executemany(
                """
                INSERT INTO prices_daily(date, ticker, open, high, low, close, volume, value, source_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(date, ticker) DO UPDATE SET
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume,
                    value=excluded.value,
                    source_ts=CURRENT_TIMESTAMP
                """,
                rows,
            )
        return len(rows)

    def upsert_cap(self, frame: pd.DataFrame) -> int:
        if frame.empty:
            return 0
        rows = frame[["date", "ticker", "mcap", "shares", "volume", "value"]].to_records(index=False).tolist()
        with db_session(self.db_path) as conn:
            conn.executemany(
                """
                INSERT INTO cap_daily(date, ticker, mcap, shares, volume, value, source_ts)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(date, ticker) DO UPDATE SET
                    mcap=excluded.mcap,
                    shares=excluded.shares,
                    volume=excluded.volume,
                    value=excluded.value,
                    source_ts=CURRENT_TIMESTAMP
                """,
                rows,
            )
        return len(rows)

    def upsert_fundamental(self, frame: pd.DataFrame) -> int:
        if frame.empty:
            return 0
        rows = frame[["date", "ticker", "per", "pbr", "eps", "bps", "div", "dps"]].to_records(index=False).tolist()
        with db_session(self.db_path) as conn:
            conn.executemany(
                """
                INSERT INTO fundamental_daily(date, ticker, per, pbr, eps, bps, div, dps, source_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(date, ticker) DO UPDATE SET
                    per=excluded.per,
                    pbr=excluded.pbr,
                    eps=excluded.eps,
                    bps=excluded.bps,
                    div=excluded.div,
                    dps=excluded.dps,
                    source_ts=CURRENT_TIMESTAMP
                """,
                rows,
            )
        return len(rows)

    def replace_snapshot(self, asof_date: str, frame: pd.DataFrame) -> int:
        with db_session(self.db_path) as conn:
            conn.execute("DELETE FROM snapshot_metrics WHERE asof_date = ?", (asof_date,))
            if frame.empty:
                return 0
            cols = [
                "asof_date", "ticker", "name", "market", "close", "mcap", "avg_value_20d", "turnover_20d",
                "per", "pbr", "div", "eps", "bps", "roe_proxy", "eps_positive", "sma20", "sma50", "sma200",
                "dist_sma20", "dist_sma50", "dist_sma200", "high_52w", "low_52w", "pos_52w", "vol_20d",
                "ret_1w", "ret_1m", "ret_3m", "ret_6m", "ret_1y", "calc_version",
            ]
            rows = frame[cols].to_records(index=False).tolist()
            conn.executemany(
                """
                INSERT INTO snapshot_metrics(
                    asof_date, ticker, name, market, close, mcap, avg_value_20d, turnover_20d,
                    per, pbr, div, eps, bps, roe_proxy, eps_positive, sma20, sma50, sma200,
                    dist_sma20, dist_sma50, dist_sma200, high_52w, low_52w, pos_52w, vol_20d,
                    ret_1w, ret_1m, ret_3m, ret_6m, ret_1y, calc_version
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                rows,
            )
        return len(frame)

    def get_price_window(self, end_date: str, window: int = 400) -> pd.DataFrame:
        query = """
        WITH ranked AS (
            SELECT p.*,
                   ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
            FROM prices_daily p
            WHERE date <= ?
        )
        SELECT date, ticker, open, high, low, close, volume, value
        FROM ranked
        WHERE rn <= ?
        ORDER BY ticker, date
        """
        with db_session(self.db_path) as conn:
            return pd.read_sql_query(query, conn, params=(end_date, window))

    def get_daily_join(self, dt: str) -> pd.DataFrame:
        query = """
        SELECT t.ticker, t.name, t.market,
               c.mcap,
               f.per, f.pbr, f.eps, f.bps, f.div
        FROM ticker_master t
        LEFT JOIN cap_daily c ON c.ticker = t.ticker AND c.date = ?
        LEFT JOIN fundamental_daily f ON f.ticker = t.ticker AND f.date = ?
        WHERE t.active_flag = 1
        """
        with db_session(self.db_path) as conn:
            return pd.read_sql_query(query, conn, params=(dt, dt))

    def get_latest_snapshot_date(self) -> str | None:
        with db_session(self.db_path) as conn:
            row = conn.execute("SELECT MAX(asof_date) AS d FROM snapshot_metrics").fetchone()
        return row[0] if row and row[0] else None

    def load_snapshot(self, asof_date: str) -> pd.DataFrame:
        with db_session(self.db_path) as conn:
            return pd.read_sql_query(
                "SELECT * FROM snapshot_metrics WHERE asof_date = ? ORDER BY ticker",
                conn,
                params=(asof_date,),
            )
