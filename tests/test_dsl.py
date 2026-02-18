from stock_screener.screener.dsl import FilterCondition, apply_filters
import pandas as pd


def test_apply_filters_gte_lte():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [10, 20, 30]})
    out = apply_filters(df, [FilterCondition("a", "gte", 2), FilterCondition("b", "lte", 20)])
    assert len(out) == 1
    assert out.iloc[0]["a"] == 2
