import pandas as pd

from stock_screener.collectors.fundamental_provider import merge_financial_records


def _base_record(**overrides):
    row = {
        "ticker": "005930",
        "fiscal_period": "2024-12-31",
        "period_type": "annual",
        "reported_date": "2025-03-20",
        "consolidation_type": "consolidated",
        "revenue": 1000.0,
        "operating_income": 100.0,
        "net_income": 80.0,
        "eps": 10.0,
        "bps": 100.0,
        "source": "dart_primary",
        "source_priority": 300,
        "is_correction": 0,
    }
    row.update(overrides)
    return row


def test_merge_financial_records_keeps_priority_rules_with_mixed_sources():
    dart_frame = pd.DataFrame(
        [
            _base_record(source="dart_primary", source_priority=300, reported_date="2025-03-20", eps=10.0),
            _base_record(source="dart_primary", source_priority=300, reported_date="2025-03-22", is_correction=1, eps=11.0),
        ]
    )
    fallback_frame = pd.DataFrame(
        [
            _base_record(source="pykrx_fallback", source_priority=100, reported_date="2025-03-25", eps=12.0),
        ]
    )

    merged = merge_financial_records([dart_frame, fallback_frame])

    assert len(merged) == 1
    row = merged.iloc[0]
    # is_correction 우선
    assert float(row["eps"]) == 11.0
    # row-level 메타 선택도 동일 우선순위 유지
    assert row["source"] == "dart_primary"
    assert row["reported_date"] == "2025-03-22"


def test_merge_financial_records_uses_reported_date_then_source_priority_for_tie_break():
    dart_frame = pd.DataFrame(
        [
            _base_record(source="dart_primary", source_priority=300, reported_date="2025-03-20", is_correction=0, eps=10.0),
            _base_record(source="dart_primary", source_priority=300, reported_date="2025-03-24", is_correction=0, eps=11.0),
        ]
    )
    fallback_frame = pd.DataFrame(
        [
            _base_record(source="pykrx_fallback", source_priority=100, reported_date="2025-03-24", is_correction=0, eps=9.0),
        ]
    )

    merged = merge_financial_records([dart_frame, fallback_frame])

    assert len(merged) == 1
    row = merged.iloc[0]
    # is_correction 동일이면 reported_date 최신값 우선
    assert float(row["eps"]) == 11.0
    # reported_date도 동일하면 source_priority 우선
    assert row["source"] == "dart_primary"
    assert row["reported_date"] == "2025-03-24"
