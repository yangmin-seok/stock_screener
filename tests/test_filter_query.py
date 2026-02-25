from stock_screener.web.filter_query import prune_query_filter_state


def test_prune_query_filter_state_removes_irrelevant_keys_when_mode_changes_to_any():
    query_state = {
        "rsi_filter_mode": "Any",
        "rsi_bucket": "30~50",
        "rsi_min_custom": "30",
        "rsi_max_custom": "70",
        "momentum_filter_mode": "Any",
        "momentum_metric": "ret_6m",
    }

    pruned = prune_query_filter_state(
        query_state,
        {
            "rsi_filter_mode": "Any",
            "momentum_filter_mode": "Any",
        },
    )

    assert "rsi_bucket" not in pruned
    assert "rsi_min_custom" not in pruned
    assert "rsi_max_custom" not in pruned
    assert "momentum_metric" not in pruned


def test_prune_query_filter_state_keeps_only_bucket_keys_in_bucket_mode_and_prunes_secondary_foreign_none():
    query_state = {
        "foreign_buy_filter_mode": "구간 선택",
        "foreign_buy_bucket": "0~1",
        "foreign_buy_min_custom": "0.2",
        "foreign_buy_max_custom": "0.8",
        "foreign_buy2_metric": "none",
        "foreign_buy2_filter_mode": "구간 선택",
        "foreign_buy2_bucket": "0~1",
        "foreign_buy2_min_custom": "0.1",
    }

    pruned = prune_query_filter_state(
        query_state,
        {
            "foreign_buy_filter_mode": "구간 선택",
            "foreign_buy2_filter_mode": "구간 선택",
            "foreign_buy2_metric": "none",
        },
    )

    assert pruned["foreign_buy_bucket"] == "0~1"
    assert "foreign_buy_min_custom" not in pruned
    assert "foreign_buy_max_custom" not in pruned
    assert "foreign_buy2_metric" not in pruned
    assert "foreign_buy2_filter_mode" not in pruned
