from __future__ import annotations

from pathlib import Path

import pandas as pd

from stock_screener.storage.db import db_session


class Repository:
    def __init__(self, db_path: str | Path):
        self.db_path = Path(db_path)

    @staticmethod
    def _to_sql_records(frame: pd.DataFrame, columns: list[str]) -> list[tuple]:
        data = frame[columns].copy()
        data = data.where(pd.notna(data), None)
        return [tuple(row) for row in data.itertuples(index=False, name=None)]


    def upsert_tickers(self, frame: pd.DataFrame) -> int:
        if frame.empty:
            return 0
        rows = self._to_sql_records(frame, ["ticker", "name", "market", "active_flag"])
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
        data = frame.copy()
        if "value" not in data.columns:
            data["value"] = pd.NA
        rows = self._to_sql_records(data, ["date", "ticker", "open", "high", "low", "close", "volume", "value"])
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
        rows = self._to_sql_records(frame, ["date", "ticker", "mcap", "shares", "volume", "value"])
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
        rows = self._to_sql_records(frame, ["date", "ticker", "per", "pbr", "eps", "bps", "div", "dps"])
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
                "per", "pbr", "div", "dps", "eps", "bps", "roe_proxy", "eps_positive", "sma20", "sma50", "sma200",
                "dist_sma20", "dist_sma50", "dist_sma200", "high_52w", "low_52w", "pos_52w", "near_52w_high_ratio",
                "vol_20d", "ret_1w", "ret_1m", "ret_3m", "ret_6m", "ret_1y", "eps_cagr_5y", "eps_yoy_q", "calc_version",
            ]
            rows = self._to_sql_records(frame, cols)
            placeholders = ", ".join(["?"] * len(cols))
            conn.executemany(
                f"""
                INSERT INTO snapshot_metrics(
                    asof_date, ticker, name, market, close, mcap, avg_value_20d, turnover_20d,
                    per, pbr, div, dps, eps, bps, roe_proxy, eps_positive, sma20, sma50, sma200,
                    dist_sma20, dist_sma50, dist_sma200, high_52w, low_52w, pos_52w, near_52w_high_ratio,
                    vol_20d, ret_1w, ret_1m, ret_3m, ret_6m, ret_1y, eps_cagr_5y, eps_yoy_q, calc_version
                ) VALUES ({placeholders})
                """,
                rows,
            )
        return len(frame)

    def get_price_window(self, end_date: str, window: int = 400) -> pd.DataFrame:
        query = """
        WITH ranked AS (
            SELECT p.date,
                   p.ticker,
                   p.open,
                   p.high,
                   p.low,
                   p.close,
                   p.volume,
                   COALESCE(c.value, p.value) AS value,
                   ROW_NUMBER() OVER (PARTITION BY p.ticker ORDER BY p.date DESC) AS rn
            FROM prices_daily p
            LEFT JOIN cap_daily c ON c.date = p.date AND c.ticker = p.ticker
            WHERE p.date <= ?
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
               f.per, f.pbr, f.eps, f.bps, f.div, f.dps
        FROM ticker_master t
        LEFT JOIN cap_daily c ON c.ticker = t.ticker AND c.date = ?
        LEFT JOIN fundamental_daily f ON f.ticker = t.ticker AND f.date = ?
        WHERE t.active_flag = 1
        """
        with db_session(self.db_path) as conn:
            return pd.read_sql_query(query, conn, params=(dt, dt))


    def get_fundamental_window(self, end_date: str, years: int = 6) -> pd.DataFrame:
        query = """
        SELECT date, ticker, per, pbr, eps, bps, div, dps
        FROM fundamental_daily
        WHERE date <= ?
          AND date >= date(?, ?)
        ORDER BY ticker, date
        """
        with db_session(self.db_path) as conn:
            return pd.read_sql_query(query, conn, params=(end_date, end_date, f"-{years} years"))


    def get_latest_price_date(self) -> str | None:
        with db_session(self.db_path) as conn:
            row = conn.execute("SELECT MAX(date) AS d FROM prices_daily").fetchone()
        return row[0] if row and row[0] else None

    def count_active_tickers(self) -> int:
        with db_session(self.db_path) as conn:
            row = conn.execute("SELECT COUNT(*) FROM ticker_master WHERE active_flag = 1").fetchone()
        return int(row[0]) if row and row[0] is not None else 0

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
