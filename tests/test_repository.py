import pandas as pd

from stock_screener.storage.repository import Repository


def test_to_sql_records_converts_pd_na_to_none(tmp_path):
    repo = Repository(tmp_path / "x.db")
    df = pd.DataFrame({"a": [1, pd.NA], "b": ["x", pd.NA]})
    rows = repo._to_sql_records(df, ["a", "b"])
    assert rows[0] == (1, "x")
    assert rows[1] == (None, None)
