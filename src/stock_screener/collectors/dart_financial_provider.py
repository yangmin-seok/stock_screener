from __future__ import annotations

from datetime import date
from typing import Any

import pandas as pd

from stock_screener.collectors.dart_client import DartClient
from stock_screener.collectors.fundamental_provider import (
    FUNDAMENTAL_SOURCE_PRIORITY,
    FundamentalProvider,
    FundamentalProviderConfig,
)


class DartFinancialProvider(FundamentalProvider):
    """Primary financial provider that normalizes DART payloads to internal schema."""

    config = FundamentalProviderConfig(source_name="dart_primary", priority_tier="primary")

    def __init__(self, client: DartClient):
        self.client = client

    @staticmethod
    def _first_present(frame: pd.DataFrame, candidates: list[str], default: Any = pd.NA) -> pd.Series:
        for column in candidates:
            if column in frame.columns:
                return frame[column]
        return pd.Series([default] * len(frame), index=frame.index)

    @staticmethod
    def _coerce_numeric(series: pd.Series, *, zero_as_na: bool = False) -> pd.Series:
        cleaned = (
            series.astype("string")
            .str.replace(",", "", regex=False)
            .str.replace("\u2212", "-", regex=False)
            .str.replace("\u2014", "", regex=False)
            .str.strip()
        )
        cleaned = cleaned.replace({"": pd.NA, "-": pd.NA, "N/A": pd.NA, "nan": pd.NA, "None": pd.NA})
        out = pd.to_numeric(cleaned, errors="coerce")
        out = out.mask(~pd.Series(pd.notna(out), index=out.index), pd.NA)
        out = out.replace([float("inf"), float("-inf")], pd.NA)
        if zero_as_na:
            out = out.mask(out == 0, pd.NA)
        return out

    @classmethod
    def _normalize_period_type(cls, frame: pd.DataFrame) -> pd.Series:
        reprt_code = cls._first_present(frame, ["reprt_code", "report_code"]).astype("string")
        report_nm = cls._first_present(frame, ["report_nm", "report_name"]).astype("string")

        period_type = pd.Series([pd.NA] * len(frame), index=frame.index, dtype="object")
        period_type = period_type.mask(reprt_code == "11011", "annual")
        period_type = period_type.mask(reprt_code.isin(["11012", "11013", "11014"]), "quarterly")

        period_type = period_type.mask(period_type.isna() & report_nm.str.contains("사업보고서", na=False), "annual")
        period_type = period_type.mask(
            period_type.isna() & report_nm.str.contains("분기보고서|반기보고서", na=False),
            "quarterly",
        )
        return period_type.fillna("annual")

    @classmethod
    def _normalize_fiscal_period(cls, frame: pd.DataFrame, period_type: pd.Series) -> pd.Series:
        fiscal_period = cls._first_present(frame, ["fiscal_period", "accounting_period", "stlm_dt"])
        fiscal_period = pd.to_datetime(fiscal_period, errors="coerce")

        year_series = cls._first_present(frame, ["bsns_year", "fiscal_year"]).astype("string")
        year_num = pd.to_numeric(year_series, errors="coerce")

        missing = fiscal_period.isna() & year_num.notna()
        annual_missing = missing & (period_type == "annual")
        quarterly_missing = missing & (period_type == "quarterly")

        fiscal_period = fiscal_period.mask(annual_missing, pd.to_datetime(year_num[annual_missing].astype("Int64").astype(str) + "-12-31"))

        reprt_code = cls._first_present(frame, ["reprt_code", "report_code"]).astype("string")
        quarter_end_map = {
            "11013": "-03-31",
            "11012": "-06-30",
            "11014": "-09-30",
            "11011": "-12-31",
        }
        quarter_suffix = reprt_code.map(quarter_end_map)
        quarter_date = pd.to_datetime(year_num.astype("Int64").astype(str) + quarter_suffix.fillna("-12-31"), errors="coerce")
        fiscal_period = fiscal_period.mask(quarterly_missing, quarter_date[quarterly_missing])

        return fiscal_period.dt.strftime("%Y-%m-%d")

    @classmethod
    def _normalize_consolidation_type(cls, frame: pd.DataFrame) -> pd.Series:
        raw = cls._first_present(frame, ["consolidation_type", "fs_div", "fs_nm"]).astype("string").str.lower()
        out = pd.Series(["consolidated"] * len(frame), index=frame.index)
        out = out.mask(raw.str.contains("ofs|별도|separate", na=False), "separate")
        out = out.mask(raw.str.contains("cfs|연결|consolidated", na=False), "consolidated")
        return out

    @classmethod
    def _normalize_is_correction(cls, frame: pd.DataFrame) -> pd.Series:
        corrected = cls._first_present(frame, ["is_correction", "is_revision"]).astype("string").str.lower()
        report_nm = cls._first_present(frame, ["report_nm", "report_name", "rcept_no"]).astype("string")
        out = corrected.isin(["1", "true", "y", "yes"])
        out = out | report_nm.str.contains("정정|correction|revision", case=False, na=False)
        return out.astype(int)

    @classmethod
    def normalize_dart_financials(cls, payload: pd.DataFrame | list[dict[str, Any]], dt: date) -> pd.DataFrame:
        frame = payload.copy() if isinstance(payload, pd.DataFrame) else pd.DataFrame(payload)
        if frame.empty:
            return pd.DataFrame()

        ticker = cls._first_present(frame, ["ticker", "stock_code", "corp_code"]).astype("string").str.zfill(6)
        period_type = cls._normalize_period_type(frame)
        fiscal_period = cls._normalize_fiscal_period(frame, period_type)

        reported_date = pd.to_datetime(
            cls._first_present(frame, ["reported_date", "rcept_dt", "report_date"]),
            errors="coerce",
        )
        reported_date = reported_date.fillna(pd.Timestamp(dt)).dt.strftime("%Y-%m-%d")

        out = pd.DataFrame(
            {
                "ticker": ticker,
                "fiscal_period": fiscal_period,
                "period_type": period_type,
                "reported_date": reported_date,
                "consolidation_type": cls._normalize_consolidation_type(frame),
                "revenue": cls._coerce_numeric(cls._first_present(frame, ["revenue", "sales", "thstrm_amount"])),
                "operating_income": cls._coerce_numeric(
                    cls._first_present(frame, ["operating_income", "op_income", "thstrm_operating_income"])
                ),
                "net_income": cls._coerce_numeric(
                    cls._first_present(frame, ["net_income", "profit", "thstrm_net_income"])
                ),
                "eps": cls._coerce_numeric(cls._first_present(frame, ["eps", "thstrm_eps"]), zero_as_na=True),
                "bps": cls._coerce_numeric(cls._first_present(frame, ["bps", "thstrm_bps"]), zero_as_na=True),
                "source": cls.config.source_name,
                "source_priority": FUNDAMENTAL_SOURCE_PRIORITY[cls.config.priority_tier],
                "is_correction": cls._normalize_is_correction(frame),
            }
        )

        out = out[out["ticker"].notna() & out["fiscal_period"].notna()]
        out = out.drop_duplicates(subset=["ticker", "fiscal_period", "period_type", "consolidation_type", "reported_date"])
        return out.reset_index(drop=True)

    def fetch_financials(self, dt: date) -> pd.DataFrame:
        fetch_fn = getattr(self.client, "fetch_financials", None)
        if fetch_fn is None:
            return pd.DataFrame()
        payload = fetch_fn(dt)
        return self.normalize_dart_financials(payload, dt)
