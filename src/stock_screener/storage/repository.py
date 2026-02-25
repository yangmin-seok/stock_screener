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
        data = frame.copy()
        if "reserve_ratio" not in data.columns:
            data["reserve_ratio"] = pd.NA
        rows = self._to_sql_records(data, ["date", "ticker", "per", "pbr", "div", "dps", "reserve_ratio"])
        with db_session(self.db_path) as conn:
            conn.executemany(
                """
                INSERT INTO fundamental_daily(date, ticker, per, pbr, div, dps, reserve_ratio, source_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(date, ticker) DO UPDATE SET
                    per=excluded.per,
                    pbr=excluded.pbr,
                    div=excluded.div,
                    dps=excluded.dps,
                    reserve_ratio=COALESCE(excluded.reserve_ratio, fundamental_daily.reserve_ratio),
                    source_ts=CURRENT_TIMESTAMP
                """,
                rows,
            )
        return len(rows)

    def upsert_financials(self, frame: pd.DataFrame, asof_date: str) -> int:
        """Upsert collection-time financial rows into financials_daily audit history."""
        if frame.empty:
            return 0
        data = frame.copy()
        data["date"] = asof_date
        for col, default in (("is_correction", 0), ("source_priority", 0)):
            if col not in data.columns:
                data[col] = default
        data["is_correction"] = pd.to_numeric(data["is_correction"], errors="coerce").fillna(0).astype(int)
        data["source_priority"] = pd.to_numeric(data["source_priority"], errors="coerce").fillna(0).astype(int)

        rows = self._to_sql_records(
            data,
            [
                "date",
                "ticker",
                "fiscal_period",
                "period_type",
                "reported_date",
                "consolidation_type",
                "source",
                "revenue",
                "operating_income",
                "net_income",
                "eps",
                "bps",
                "is_correction",
                "source_priority",
            ],
        )
        with db_session(self.db_path) as conn:
            conn.executemany(
                """
                INSERT INTO financials_daily(
                    date, ticker, fiscal_period, period_type, reported_date, consolidation_type, source,
                    revenue, operating_income, net_income, eps, bps, source_ts,
                    is_correction, source_priority
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
                ON CONFLICT(date, ticker, fiscal_period, period_type, consolidation_type) DO UPDATE SET
                    reported_date=excluded.reported_date,
                    source=excluded.source,
                    revenue=COALESCE(excluded.revenue, financials_daily.revenue),
                    operating_income=COALESCE(excluded.operating_income, financials_daily.operating_income),
                    net_income=COALESCE(excluded.net_income, financials_daily.net_income),
                    eps=COALESCE(excluded.eps, financials_daily.eps),
                    bps=COALESCE(excluded.bps, financials_daily.bps),
                    is_correction=COALESCE(excluded.is_correction, financials_daily.is_correction),
                    source_priority=COALESCE(excluded.source_priority, financials_daily.source_priority),
                    source_ts=CURRENT_TIMESTAMP
                """,
                rows,
            )
        return len(rows)

    def upsert_financials_periodic(self, frame: pd.DataFrame) -> int:
        if frame.empty:
            return 0
        data = frame.copy()
        for col, default in (("source_priority", 0), ("is_correction", 0)):
            if col not in data.columns:
                data[col] = default
        data["source_priority"] = pd.to_numeric(data["source_priority"], errors="coerce").fillna(0).astype(int)
        data["is_correction"] = pd.to_numeric(data["is_correction"], errors="coerce").fillna(0).astype(int)
        data["reported_date"] = pd.to_datetime(data["reported_date"], errors="coerce")

        # Tie-break priority: correction wins > latest reported date > higher source priority.
        data = data.sort_values(
            by=[
                "ticker",
                "fiscal_period",
                "period_type",
                "consolidation_type",
                "is_correction",
                "reported_date",
                "source_priority",
            ],
            ascending=[True, True, True, True, False, False, False],
        )
        deduped = data.drop_duplicates(
            subset=["ticker", "fiscal_period", "period_type", "consolidation_type"],
            keep="first",
        ).copy()
        deduped["reported_date"] = deduped["reported_date"].dt.strftime("%Y-%m-%d")

        rows = self._to_sql_records(
            deduped,
            [
                "ticker",
                "fiscal_period",
                "period_type",
                "consolidation_type",
                "reported_date",
                "source",
                "revenue",
                "operating_income",
                "net_income",
                "eps",
                "bps",
                "is_correction",
                "source_priority",
            ],
        )
        with db_session(self.db_path) as conn:
            conn.executemany(
                """
                INSERT INTO financials_periodic(
                    ticker, fiscal_period, period_type, consolidation_type,
                    reported_date, source, revenue, operating_income, net_income,
                    eps, bps, source_ts, is_correction, source_priority
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
                ON CONFLICT(ticker, fiscal_period, period_type, consolidation_type) DO UPDATE SET
                    reported_date=excluded.reported_date,
                    source=excluded.source,
                    revenue=COALESCE(excluded.revenue, financials_periodic.revenue),
                    operating_income=COALESCE(excluded.operating_income, financials_periodic.operating_income),
                    net_income=COALESCE(excluded.net_income, financials_periodic.net_income),
                    eps=COALESCE(excluded.eps, financials_periodic.eps),
                    bps=COALESCE(excluded.bps, financials_periodic.bps),
                    is_correction=COALESCE(excluded.is_correction, financials_periodic.is_correction),
                    source_priority=COALESCE(excluded.source_priority, financials_periodic.source_priority),
                    source_ts=CURRENT_TIMESTAMP
                """,
                rows,
            )
        return len(rows)

    def upsert_reserve_ratio(self, dt: str, frame: pd.DataFrame) -> int:
        if frame.empty:
            return 0
        data = frame.copy()
        data["date"] = dt
        rows = self._to_sql_records(data, ["date", "ticker", "reserve_ratio"])
        with db_session(self.db_path) as conn:
            conn.executemany(
                """
                INSERT INTO fundamental_daily(date, ticker, reserve_ratio, source_ts)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(date, ticker) DO UPDATE SET
                    reserve_ratio=excluded.reserve_ratio,
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
                "asof_date", "ticker", "name", "market", "close", "mcap", "avg_value_20d", "current_value", "relative_value", "turnover_20d",
                "per", "pbr", "div", "dps", "eps", "bps", "reserve_ratio", "fiscal_period", "period_type", "reported_date", "consolidation_type", "financial_source", "roe_proxy", "eps_positive", "sma20", "sma50", "sma200",
                "dist_sma20", "dist_sma50", "dist_sma200", "high_52w", "low_52w", "pos_52w", "near_52w_high_ratio",
                "vol_20d", "ret_1w", "ret_1m", "ret_3m", "ret_6m", "ret_1y", "eps_cagr_3y", "eps_cagr_5y", "eps_yoy_q", "eps_growth_ttm", "eps_qoq", "sales_growth_qoq", "sales_growth_ttm", "sales_cagr_3y", "sales_cagr_5y",
                "pe_ratio", "forward_pe", "ps_ratio", "pb_ratio", "peg_ratio", "ps", "peg", "ev", "ev_sales", "ev_ebitda",
                "gross_margin", "operating_margin", "net_margin", "roa", "roe", "roic",
                "debt_equity", "lt_debt_equity", "current_ratio", "quick_ratio", "payout_ratio",
                "eps_cagr_3y_window_years", "eps_cagr_3y_asof", "eps_cagr_3y_sample_count", "eps_cagr_5y_window_years", "eps_cagr_5y_asof", "eps_cagr_5y_sample_count", "eps_yoy_q_window_years", "eps_yoy_q_asof", "eps_yoy_q_sample_count", "sales_cagr_3y_window_years", "sales_cagr_3y_asof", "sales_cagr_3y_sample_count", "has_price_5y", "has_price_10y", "calc_version",
            ]
            rows = self._to_sql_records(frame, cols)
            placeholders = ", ".join(["?"] * len(cols))
            conn.executemany(
                f"""
                INSERT INTO snapshot_metrics(
                    asof_date, ticker, name, market, close, mcap, avg_value_20d, current_value, relative_value, turnover_20d,
                    per, pbr, div, dps, eps, bps, reserve_ratio, fiscal_period, period_type, reported_date, consolidation_type, financial_source, roe_proxy, eps_positive, sma20, sma50, sma200,
                    dist_sma20, dist_sma50, dist_sma200, high_52w, low_52w, pos_52w, near_52w_high_ratio,
                    vol_20d, ret_1w, ret_1m, ret_3m, ret_6m, ret_1y, eps_cagr_3y, eps_cagr_5y, eps_yoy_q, eps_growth_ttm, eps_qoq, sales_growth_qoq, sales_growth_ttm, sales_cagr_3y, sales_cagr_5y,
                    pe_ratio, forward_pe, ps_ratio, pb_ratio, peg_ratio, ps, peg, ev, ev_sales, ev_ebitda,
                    gross_margin, operating_margin, net_margin, roa, roe, roic,
                    debt_equity, lt_debt_equity, current_ratio, quick_ratio, payout_ratio,
                    eps_cagr_3y_window_years, eps_cagr_3y_asof, eps_cagr_3y_sample_count, eps_cagr_5y_window_years, eps_cagr_5y_asof, eps_cagr_5y_sample_count, eps_yoy_q_window_years, eps_yoy_q_asof, eps_yoy_q_sample_count, sales_cagr_3y_window_years, sales_cagr_3y_asof, sales_cagr_3y_sample_count, has_price_5y, has_price_10y, calc_version
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
        WITH fin AS (
            SELECT *
            FROM (
                SELECT
                    f.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY f.ticker
                        ORDER BY
                            (f.eps IS NOT NULL OR f.bps IS NOT NULL) DESC,
                            f.reported_date DESC,
                            f.fiscal_period DESC,
                            f.date DESC,
                            COALESCE(f.is_correction, 0) DESC,
                            COALESCE(f.source_priority, 0) DESC,
                            f.source_ts DESC
                    ) AS rn
                FROM financials_daily f
                WHERE f.date <= ?
            ) ranked
            WHERE ranked.rn = 1
        )
        SELECT t.ticker, t.name, t.market,
               c.mcap,
               f.per, f.pbr, f.div, f.dps, f.reserve_ratio,
               fin.eps, fin.bps, fin.fiscal_period, fin.period_type,
               fin.reported_date, fin.consolidation_type, fin.source AS financial_source
        FROM ticker_master t
        LEFT JOIN cap_daily c ON c.ticker = t.ticker AND c.date = ?
        LEFT JOIN fundamental_daily f ON f.ticker = t.ticker AND f.date = ?
        LEFT JOIN fin ON fin.ticker = t.ticker
        WHERE t.active_flag = 1
        """
        with db_session(self.db_path) as conn:
            return pd.read_sql_query(query, conn, params=(dt, dt, dt))

    def get_fundamental_window(self, end_date: str, years: int = 6) -> pd.DataFrame:
        """Backward-compatible alias: prefers periodic fundamental source for growth windows."""
        return self.get_fundamental_window_periodic(end_date=end_date, years=years)

    def get_fundamental_window_periodic(self, end_date: str, years: int = 6) -> pd.DataFrame:
        query = """
        SELECT
            NULL AS date,
            ticker,
            revenue,
            eps,
            bps,
            fiscal_period,
            period_type,
            reported_date,
            consolidation_type,
            source,
            source_ts,
            is_correction,
            source_priority
        FROM financials_periodic
        WHERE COALESCE(reported_date, fiscal_period) <= ?
          AND COALESCE(reported_date, fiscal_period) >= date(?, ?)
        ORDER BY ticker, period_type, fiscal_period
        """
        with db_session(self.db_path) as conn:
            return pd.read_sql_query(query, conn, params=(end_date, end_date, f"-{years} years"))

    def get_latest_financial_period(self, dt: str) -> dict[str, str | None]:
        query = """
        SELECT fiscal_period, period_type, reported_date
        FROM financials_daily
        WHERE date <= ?
        ORDER BY COALESCE(reported_date, fiscal_period, date) DESC
        LIMIT 1
        """
        with db_session(self.db_path) as conn:
            row = conn.execute(query, (dt,)).fetchone()
        if not row:
            return {"fiscal_period": None, "period_type": None, "reported_date": None}
        return {
            "fiscal_period": row[0],
            "period_type": row[1],
            "reported_date": row[2],
        }

    def get_latest_fundamental_date(self) -> str | None:
        with db_session(self.db_path) as conn:
            row = conn.execute("SELECT MAX(date) AS d FROM fundamental_daily").fetchone()
        return row[0] if row and row[0] else None

    def get_latest_price_date(self) -> str | None:
        with db_session(self.db_path) as conn:
            row = conn.execute("SELECT MAX(date) AS d FROM prices_daily").fetchone()
        return row[0] if row and row[0] else None

    def count_active_tickers(self) -> int:
        with db_session(self.db_path) as conn:
            row = conn.execute("SELECT COUNT(*) FROM ticker_master WHERE active_flag = 1").fetchone()
        return int(row[0]) if row and row[0] is not None else 0

    def get_active_tickers(self) -> list[str]:
        with db_session(self.db_path) as conn:
            rows = conn.execute("SELECT ticker FROM ticker_master WHERE active_flag = 1 ORDER BY ticker").fetchall()
        return [str(row[0]) for row in rows]


    def get_collection_checkpoint(self, ticker: str) -> dict[str, str | None]:
        with db_session(self.db_path) as conn:
            row = conn.execute(
                "SELECT last_price_date, last_fundamental_date FROM collection_checkpoint WHERE ticker = ?",
                (ticker,),
            ).fetchone()
        if not row:
            return {"last_price_date": None, "last_fundamental_date": None}
        return {"last_price_date": row[0], "last_fundamental_date": row[1]}

    def upsert_collection_checkpoint(
        self,
        ticker: str,
        *,
        last_price_date: str | None = None,
        last_fundamental_date: str | None = None,
    ) -> None:
        with db_session(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO collection_checkpoint(ticker, last_price_date, last_fundamental_date, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(ticker) DO UPDATE SET
                    last_price_date=COALESCE(excluded.last_price_date, collection_checkpoint.last_price_date),
                    last_fundamental_date=COALESCE(excluded.last_fundamental_date, collection_checkpoint.last_fundamental_date),
                    updated_at=CURRENT_TIMESTAMP
                """,
                (ticker, last_price_date, last_fundamental_date),
            )

    def log_job_stage(
        self,
        *,
        run_id: str,
        stage: str,
        status: str,
        message: str | None = None,
        row_count: int | None = None,
    ) -> None:
        with db_session(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO job_log(run_id, stage, status, started_at, ended_at, message, row_count)
                VALUES (
                    ?, ?, ?, CURRENT_TIMESTAMP,
                    CASE WHEN ? IN ('success', 'failed') THEN CURRENT_TIMESTAMP ELSE NULL END,
                    ?, ?
                )
                ON CONFLICT(run_id, stage) DO UPDATE SET
                    status=excluded.status,
                    ended_at=CASE
                        WHEN excluded.status IN ('success', 'failed') THEN CURRENT_TIMESTAMP
                        ELSE job_log.ended_at
                    END,
                    message=excluded.message,
                    row_count=excluded.row_count
                """,
                (run_id, stage, status, status, message, row_count),
            )

    def get_batch_checkpoint(self, checkpoint_key: str) -> str | None:
        with db_session(self.db_path) as conn:
            row = conn.execute(
                "SELECT checkpoint_value FROM batch_checkpoint WHERE checkpoint_key = ?",
                (checkpoint_key,),
            ).fetchone()
        return row[0] if row and row[0] else None

    def set_batch_checkpoint(self, checkpoint_key: str, checkpoint_value: str) -> None:
        with db_session(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO batch_checkpoint(checkpoint_key, checkpoint_value, updated_at)
                VALUES (?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(checkpoint_key) DO UPDATE SET
                    checkpoint_value=excluded.checkpoint_value,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (checkpoint_key, checkpoint_value),
            )

    def clear_batch_checkpoint(self, checkpoint_key: str) -> None:
        with db_session(self.db_path) as conn:
            conn.execute("DELETE FROM batch_checkpoint WHERE checkpoint_key = ?", (checkpoint_key,))

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
