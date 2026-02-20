import pandas as pd

from stock_screener.collectors.naver_ratio_client import NaverRatioCollector


def test_extract_latest_reserve_ratio_from_html():
    html = """
    <html><body>
      <th>유보율</th>
      <td>1,234.56</td>
      <td>1,111.11</td>
    </body></html>
    """
    out = NaverRatioCollector._extract_latest_reserve_ratio_from_html(html)
    assert out == 1234.56


def test_extract_latest_reserve_ratio_returns_none_without_marker():
    html = "<html><body><td>123.45</td></body></html>"
    out = NaverRatioCollector._extract_latest_reserve_ratio_from_html(html)
    assert out is None


def test_latest_reserve_ratio_uses_parsed_values(monkeypatch):
    collector = NaverRatioCollector()

    def fake_fetch_html(ticker: str):
        return f"<th>유보율</th><td>{1000 + int(ticker)}</td>"

    monkeypatch.setattr(collector, "_fetch_html", fake_fetch_html)
    frame = collector.latest_reserve_ratio(["1", "2"])
    assert list(frame.columns) == ["ticker", "reserve_ratio"]
    assert len(frame) == 2
    assert frame.loc[frame["ticker"] == "1", "reserve_ratio"].iloc[0] == 1001
