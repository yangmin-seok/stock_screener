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



def test_normalize_foreign_investor_flow_supports_column_aliases_and_nan_handling():
    frame = pd.DataFrame(
        {
            "순매수수량": ["100", None],
            "순매수대금": ["1000", "-"] ,
        },
        index=["005930", "000660"],
    )

    out = PykrxCollector._normalize_foreign_investor_flow(frame, pd.Timestamp("2026-01-02").date())

    assert list(out.columns) == ["date", "ticker", "foreign_net_buy_volume", "foreign_net_buy_value"]
    assert out.loc[0, "date"] == "2026-01-02"
    assert out.loc[0, "ticker"] == "005930"
    assert out.loc[0, "foreign_net_buy_volume"] == 100.0
    assert out.loc[0, "foreign_net_buy_value"] == 1000.0
    assert pd.isna(out.loc[1, "foreign_net_buy_volume"])
    assert pd.isna(out.loc[1, "foreign_net_buy_value"])


def test_normalize_foreign_investor_flow_sets_missing_metric_columns_to_na():
    frame = pd.DataFrame({"기타": [1]}, index=["005930"])

    out = PykrxCollector._normalize_foreign_investor_flow(frame, pd.Timestamp("2026-01-02").date())

    assert out.loc[0, "ticker"] == "005930"
    assert pd.isna(out.loc[0, "foreign_net_buy_volume"])
    assert pd.isna(out.loc[0, "foreign_net_buy_value"])
