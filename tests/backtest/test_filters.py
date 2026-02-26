import pandas as pd

from stock_screener.backtest.filters import apply_filters


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "ticker": "AAA",
                "pbr": 0.8,
                "per": 12.0,
                "roe_proxy": 0.15,
                "foreign_cum_value_20d": 2000.0,
                "foreign_cum_volume_20d": 100.0,
                "foreign_pressure_by_avg_value": 0.10,
                "foreign_pressure_by_mcap": 0.02,
            },
            {
                "ticker": "BBB",
                "pbr": 1.3,
                "per": -3.0,
                "roe_proxy": 0.05,
                "foreign_cum_value_20d": 500.0,
                "foreign_cum_volume_20d": 50.0,
                "foreign_pressure_by_avg_value": 0.01,
                "foreign_pressure_by_mcap": 0.005,
            },
            {
                "ticker": "CCC",
                "pbr": None,
                "per": None,
                "roe_proxy": None,
                "foreign_cum_value_20d": None,
                "foreign_cum_volume_20d": None,
                "foreign_pressure_by_avg_value": None,
                "foreign_pressure_by_mcap": None,
            },
        ]
    )


def test_apply_filters_combined_and_diagnostics():
    filters = {
        "pbr": {"enabled": True, "field": "pbr", "op": "range", "min": 0.5, "max": 1.0, "inclusive": True},
        "roe": {"enabled": True, "field": "roe_proxy", "op": "gte", "value": 0.10},
        "foreign_cum": {
            "enabled": True,
            "field": "foreign_cum",
            "op": "gte",
            "value": 1000,
            "unit": "value",
            "normalize": "none",
        },
    }

    filtered, diagnostics = apply_filters(_frame(), filters)

    assert filtered["ticker"].tolist() == ["AAA"]
    assert [d["filter"] for d in diagnostics] == ["pbr", "roe", "foreign_cum"]
    assert diagnostics[0]["before_count"] == 3
    assert diagnostics[0]["after_count"] == 1


def test_apply_filters_missing_policy_drop_vs_ignore():
    filters_drop = {
        "pbr": {"enabled": True, "field": "pbr", "op": "gte", "value": 0.5, "missing_policy": "drop"}
    }
    filters_ignore = {
        "pbr": {"enabled": True, "field": "pbr", "op": "gte", "value": 0.5, "missing_policy": "ignore"}
    }

    dropped, _ = apply_filters(_frame(), filters_drop)
    ignored, _ = apply_filters(_frame(), filters_ignore)

    assert dropped["ticker"].tolist() == ["AAA", "BBB"]
    assert ignored["ticker"].tolist() == ["AAA", "BBB", "CCC"]


def test_apply_filters_per_special_rules():
    filters = {
        "per": {
            "enabled": True,
            "field": "per",
            "op": "lte",
            "value": 20,
            "drop_nonpositive": True,
            "clip_min": 0,
            "clip_max": 20,
        }
    }

    filtered, diagnostics = apply_filters(_frame(), filters)

    assert filtered["ticker"].tolist() == ["AAA"]
    assert diagnostics[0]["filter"] == "per"


def test_apply_filters_foreign_mapping_normalized_column():
    filters = {
        "foreign_cum": {
            "enabled": True,
            "field": "foreign_cum",
            "op": "gte",
            "value": 0.05,
            "unit": "value",
            "normalize": "by_avg_value",
        }
    }

    filtered, _ = apply_filters(_frame(), filters)
    assert filtered["ticker"].tolist() == ["AAA"]
