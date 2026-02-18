import pandas as pd

from stock_screener.collectors.pykrx_client import PykrxCollector


def test_normalize_ohlcv_allows_missing_value_column_without_crash():
    frame = pd.DataFrame(
        {
            "시가": [100],
            "고가": [110],
            "저가": [90],
            "종가": [105],
            "거래량": [1000],
        },
        index=pd.to_datetime(["2026-01-02"]),
    )
    out = PykrxCollector._normalize_ohlcv(frame)
    assert "value" in out.columns
    assert pd.isna(out.loc[pd.Timestamp("2026-01-02"), "value"])


def test_normalize_ohlcv_uses_existing_value_column():
    frame = pd.DataFrame(
        {
            "시가": [100],
            "고가": [110],
            "저가": [90],
            "종가": [105],
            "거래량": [1000],
            "거래대금": [777000],
        },
        index=pd.to_datetime(["2026-01-02"]),
    )
    out = PykrxCollector._normalize_ohlcv(frame)
    assert out.loc[pd.Timestamp("2026-01-02"), "value"] == 777000
