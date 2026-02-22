import time

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


def test_extract_latest_reserve_ratio_from_text_pattern():
    html = "<html><body>항목: 유보율 : 2,345.67</body></html>"
    out = NaverRatioCollector._extract_latest_reserve_ratio_from_html(html)
    assert out == 2345.67


def test_latest_reserve_ratio_uses_parsed_values(monkeypatch):
    collector = NaverRatioCollector()

    def fake_fetch_html(ticker: str):
        return f"<th>유보율</th><td>{1000 + int(ticker)}</td>"

    monkeypatch.setattr(collector, "_fetch_html", fake_fetch_html)
    frame = collector.latest_reserve_ratio(["1", "2"])
    assert list(frame.columns) == ["ticker", "reserve_ratio"]
    assert len(frame) == 2
    assert frame.loc[frame["ticker"] == "1", "reserve_ratio"].iloc[0] == 1001


def test_latest_reserve_ratio_preserves_input_order_with_concurrency(monkeypatch):
    collector = NaverRatioCollector(max_workers=4)

    def fake_fetch_html(ticker: str):
        if ticker == "1":
            time.sleep(0.03)
        elif ticker == "2":
            time.sleep(0.01)
        return f"<th>유보율</th><td>{2000 + int(ticker)}</td>"

    monkeypatch.setattr(collector, "_fetch_html", fake_fetch_html)
    frame = collector.latest_reserve_ratio(["1", "2", "3"])

    assert list(frame["ticker"]) == ["1", "2", "3"]
    assert list(frame["reserve_ratio"]) == [2001, 2002, 2003]


def test_is_blocked_response_detects_throttle_page():
    html = "<html><body>비정상적인 접근이 감지되어 접근이 제한되었습니다.</body></html>"
    assert NaverRatioCollector._is_blocked_response(html) is True


def test_is_blocked_response_false_for_normal_html():
    html = "<html><body><th>유보율</th><td>123.4</td></body></html>"
    assert NaverRatioCollector._is_blocked_response(html) is False


def test_decode_response_fallback_to_euc_kr():
    html = "<html><body><th>유보율</th></body></html>"
    raw = html.encode("euc-kr")
    decoded = NaverRatioCollector._decode_response(raw, content_charset=None)
    assert "유보율" in decoded


def test_parse_miss_html_is_saved_once(monkeypatch, tmp_path):
    sample_path = tmp_path / "sample.html"
    collector = NaverRatioCollector(parse_miss_html_path=str(sample_path), max_workers=2)

    def fake_fetch_html(ticker: str):
        return "<html><body><div>no target marker here</div></body></html>"

    monkeypatch.setattr(collector, "_fetch_html", fake_fetch_html)
    frame = collector.latest_reserve_ratio(["1111", "2222"])

    assert frame.empty
    assert sample_path.exists()
    sample_text = sample_path.read_text(encoding="utf-8")
    assert "ticker=" in sample_text
    assert "no target marker here" in sample_text


def test_parse_miss_html_save_can_be_disabled(monkeypatch, tmp_path):
    sample_path = tmp_path / "sample-disabled.html"
    collector = NaverRatioCollector(parse_miss_html_path=str(sample_path), save_parse_miss_html=False)

    def fake_fetch_html(ticker: str):
        return "<html><body>no marker</body></html>"

    monkeypatch.setattr(collector, "_fetch_html", fake_fetch_html)
    collector.latest_reserve_ratio(["1111"])

    assert not sample_path.exists()


def test_extract_latest_reserve_ratio_with_status_parses_large_value():
    html = """
    <tr><th scope='row' class='line txt'>자본유보율</th>
    <td class='num line'>133,443.80</td><td class='num line'>120,000.00</td></tr>
    """
    out, status = NaverRatioCollector._extract_latest_reserve_ratio_with_status(html)

    assert status == "success"
    assert out == 120000.0

def test_extract_latest_reserve_ratio_with_status_supports_nested_th_and_uses_latest_column():
    html = """
    <tr>
      <th scope='row' class='line txt'><span>자본유보율</span></th>
      <td class='num'>133,158.88</td><td class='num'>89,793.21</td>
    </tr>
    """
    out, status = NaverRatioCollector._extract_latest_reserve_ratio_with_status(html)

    assert status == "success"
    assert out == 89793.21


def test_extract_latest_reserve_ratio_with_status_no_data_for_blank_row():
    html = """
    <tr><th scope='row' class='line txt'>자본유보율</th>
    <td class='num line'></td><td class='num line'></td><td class='num'></td></tr>
    """
    out, status = NaverRatioCollector._extract_latest_reserve_ratio_with_status(html)
    assert out is None
    assert status == "no_data"


def test_no_data_does_not_save_parse_sample(monkeypatch, tmp_path):
    sample_path = tmp_path / "sample-no-data.html"
    collector = NaverRatioCollector(parse_miss_html_path=str(sample_path))

    def fake_fetch_html(ticker: str):
        return "<tr><th>자본유보율</th><td class='num'></td><td class='num'></td></tr>"

    monkeypatch.setattr(collector, "_fetch_html", fake_fetch_html)
    frame = collector.latest_reserve_ratio(["1111"])

    assert frame.empty
    assert not sample_path.exists()
