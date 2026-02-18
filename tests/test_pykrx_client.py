import pandas as pd

from stock_screener.collectors.pykrx_client import PykrxCollector


def test_normalize_ohlcv_fallback_value_from_close_volume():
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
    assert out.loc[pd.Timestamp("2026-01-02"), "value"] == 105000


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
