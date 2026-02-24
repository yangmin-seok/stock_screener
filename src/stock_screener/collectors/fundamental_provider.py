from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Protocol

import pandas as pd


FUNDAMENTAL_SOURCE_PRIORITY: dict[str, int] = {
    "primary": 300,
    "secondary": 200,
    "fallback": 100,
}

FUNDAMENTAL_MERGE_RULES: dict[str, str] = {
    "reported_date": "latest_report_wins",
    "is_correction": "correction_notice_wins",
    "missing_value": "fallback_provider",
}


@dataclass(frozen=True)
class FundamentalProviderConfig:
    source_name: str
    priority_tier: str


class FundamentalProvider(Protocol):
    config: FundamentalProviderConfig

    def fetch_financials(self, dt: date) -> pd.DataFrame:
        """Return financial rows for a given anchor date.

        Expected columns:
        ticker, fiscal_period, period_type, reported_date, consolidation_type,
        revenue, operating_income, net_income, eps, bps, source, source_priority,
        is_correction
        """


FINANCIAL_FIELDS = ("revenue", "operating_income", "net_income", "eps", "bps")
META_FIELDS = ("source", "fiscal_period", "period_type", "reported_date", "consolidation_type")


def merge_financial_records(records: list[pd.DataFrame]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame()

    combined = pd.concat([frame for frame in records if not frame.empty], ignore_index=True)
    if combined.empty:
        return combined

    combined["reported_date"] = pd.to_datetime(combined["reported_date"], errors="coerce")
    combined["fiscal_period"] = pd.to_datetime(combined["fiscal_period"], errors="coerce")
    combined["source_priority"] = pd.to_numeric(combined["source_priority"], errors="coerce").fillna(0)
    combined["is_correction"] = combined["is_correction"].fillna(0).astype(int)

    out_rows: list[dict] = []
    for (ticker, fiscal_period, period_type, consolidation_type), group in combined.groupby(
        ["ticker", "fiscal_period", "period_type", "consolidation_type"], dropna=False
    ):
        base = {
            "ticker": ticker,
            "fiscal_period": fiscal_period,
            "period_type": period_type,
            "consolidation_type": consolidation_type,
        }
        for field in FINANCIAL_FIELDS:
            candidates = group[group[field].notna()].sort_values(
                by=["is_correction", "reported_date", "source_priority"],
                ascending=[False, False, False],
            )
            if candidates.empty:
                base[field] = pd.NA
                continue
            pick = candidates.iloc[0]
            base[field] = pick[field]
            for meta in ("source", "reported_date"):
                key = f"{field}_{meta}"
                if key not in base or pd.isna(base[key]):
                    base[key] = pick[meta]

        chosen_meta = group.sort_values(
            by=["is_correction", "reported_date", "source_priority"],
            ascending=[False, False, False],
        ).iloc[0]
        base["source"] = chosen_meta["source"]
        base["reported_date"] = chosen_meta["reported_date"]
        out_rows.append(base)

    out = pd.DataFrame(out_rows)
    if not out.empty:
        out["fiscal_period"] = pd.to_datetime(out["fiscal_period"]).dt.strftime("%Y-%m-%d")
        out["reported_date"] = pd.to_datetime(out["reported_date"]).dt.strftime("%Y-%m-%d")
    return out
