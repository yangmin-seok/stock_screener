import math

import pandas as pd

from stock_screener.features.fundamental_growth import (
    _preprocess_periodic_history,
    _to_numeric_series,
    calc_cagr,
    calc_qoq,
    calc_ttm_growth,
    calc_yoy,
    compute_growth_bundle,
)


def test_periodic_dedupe_converges_growth_metrics_with_duplicate_fiscal_period_dates():
    rows = [
        {"ticker": "AAA", "date": "2022-05-01", "fiscal_period": "2022Q1", "period_type": "quarterly", "reported_date": "2022-05-01", "consolidation_type": "C", "source_ts": "2022-05-01", "source_priority": 1, "is_correction": 0, "eps": 1.0, "revenue": 10.0},
        {"ticker": "AAA", "date": "2022-08-01", "fiscal_period": "2022Q2", "period_type": "quarterly", "reported_date": "2022-08-01", "consolidation_type": "C", "source_ts": "2022-08-01", "source_priority": 1, "is_correction": 0, "eps": 2.0, "revenue": 20.0},
        {"ticker": "AAA", "date": "2022-11-01", "fiscal_period": "2022Q3", "period_type": "quarterly", "reported_date": "2022-11-01", "consolidation_type": "C", "source_ts": "2022-11-01", "source_priority": 1, "is_correction": 0, "eps": 3.0, "revenue": 30.0},
        {"ticker": "AAA", "date": "2023-02-01", "fiscal_period": "2022Q4", "period_type": "quarterly", "reported_date": "2023-02-01", "consolidation_type": "C", "source_ts": "2023-02-01", "source_priority": 1, "is_correction": 0, "eps": 4.0, "revenue": 40.0},
        # same fiscal period recollected later with correction; should win dedupe
        {"ticker": "AAA", "date": "2023-03-01", "fiscal_period": "2022Q4", "period_type": "quarterly", "reported_date": "2023-03-01", "consolidation_type": "C", "source_ts": "2023-03-01", "source_priority": 2, "is_correction": 1, "eps": 40.0, "revenue": 400.0},
        {"ticker": "AAA", "date": "2023-05-01", "fiscal_period": "2023Q1", "period_type": "quarterly", "reported_date": "2023-05-01", "consolidation_type": "C", "source_ts": "2023-05-01", "source_priority": 1, "is_correction": 0, "eps": 5.0, "revenue": 50.0},
        {"ticker": "AAA", "date": "2023-08-01", "fiscal_period": "2023Q2", "period_type": "quarterly", "reported_date": "2023-08-01", "consolidation_type": "C", "source_ts": "2023-08-01", "source_priority": 1, "is_correction": 0, "eps": 6.0, "revenue": 60.0},
        {"ticker": "AAA", "date": "2023-11-01", "fiscal_period": "2023Q3", "period_type": "quarterly", "reported_date": "2023-11-01", "consolidation_type": "C", "source_ts": "2023-11-01", "source_priority": 1, "is_correction": 0, "eps": 7.0, "revenue": 70.0},
        {"ticker": "AAA", "date": "2024-02-01", "fiscal_period": "2023Q4", "period_type": "quarterly", "reported_date": "2024-02-01", "consolidation_type": "C", "source_ts": "2024-02-01", "source_priority": 1, "is_correction": 0, "eps": 8.0, "revenue": 80.0},
    ]
    frame = pd.DataFrame(rows)

    raw_result = calc_ttm_growth(_to_numeric_series(frame, "eps"), asof="2024-03-01", period_type="quarterly")
    deduped = _preprocess_periodic_history(frame)
    deduped_result = calc_ttm_growth(_to_numeric_series(deduped, "eps"), asof="2024-03-01", period_type="quarterly")

    expected = (5.0 + 6.0 + 7.0 + 8.0) / (1.0 + 2.0 + 3.0 + 40.0) - 1.0
    assert deduped_result.value is not None
    assert math.isclose(deduped_result.value, expected, rel_tol=1e-12)
    assert raw_result.value is not None
    assert not math.isclose(raw_result.value, expected, rel_tol=1e-12)

    bundle = compute_growth_bundle(frame, ticker="AAA", asof="2024-03-01")
    assert bundle["eps_growth_ttm"].value is not None
    assert math.isclose(bundle["eps_growth_ttm"].value, expected, rel_tol=1e-12)


def test_period_type_guards_apply_to_growth_calculators():
    annual_series = pd.Series([1.0, 2.0], index=pd.to_datetime(["2022-12-31", "2023-12-31"]))
    quarterly_series = pd.Series([1.0, 2.0], index=pd.to_datetime(["2023-09-30", "2023-12-31"]))

    assert calc_qoq(quarterly_series, asof="2024-01-01", period_type="annual").value is None
    assert calc_ttm_growth(quarterly_series, asof="2024-01-01", period_type="annual").value is None
    assert calc_cagr(annual_series, asof="2024-01-01", window_years=1, period_type="quarterly").value is None
    assert calc_yoy(annual_series, asof="2024-01-01", period_type="quarterly").value is None


def test_compute_growth_bundle_exposes_new_growth_keys_and_legacy_alias():
    frame = pd.DataFrame(
        [
            {"ticker": "AAA", "fiscal_period": "2023Q1", "period_type": "quarterly", "consolidation_type": "C", "eps": 1.0, "revenue": 10.0},
            {"ticker": "AAA", "fiscal_period": "2023Q2", "period_type": "quarterly", "consolidation_type": "C", "eps": 1.1, "revenue": 11.0},
            {"ticker": "AAA", "fiscal_period": "2023Q3", "period_type": "quarterly", "consolidation_type": "C", "eps": 1.2, "revenue": 12.0},
            {"ticker": "AAA", "fiscal_period": "2023Q4", "period_type": "quarterly", "consolidation_type": "C", "eps": 1.3, "revenue": 13.0},
            {"ticker": "AAA", "fiscal_period": "2024Q1", "period_type": "quarterly", "consolidation_type": "C", "eps": 1.4, "revenue": 14.0},
            {"ticker": "AAA", "fiscal_period": "2024Q2", "period_type": "quarterly", "consolidation_type": "C", "eps": 1.5, "revenue": 15.0},
            {"ticker": "AAA", "fiscal_period": "2024Q3", "period_type": "quarterly", "consolidation_type": "C", "eps": 1.6, "revenue": 16.0},
            {"ticker": "AAA", "fiscal_period": "2024Q4", "period_type": "quarterly", "consolidation_type": "C", "eps": 1.7, "revenue": 17.0},
        ]
    )

    bundle = compute_growth_bundle(frame, ticker="AAA", asof="2025-01-01")

    assert "eps_growth_qtr_over_qtr" in bundle
    assert "sales_growth_ttm" in bundle
    assert "sales_growth_past_5y" in bundle
    assert bundle["eps_growth_this_year_over_year"] == bundle["eps_growth_qtr_over_qtr"]
