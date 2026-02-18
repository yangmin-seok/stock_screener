# KR Stock Screener (pykrx + SQLite)

한국 주식 펀더멘털 스크리너입니다.

## 특징
- pykrx 기반 데이터 수집 (OHLCV, 시총, 펀더멘털)
- SQLite 캐시: 초기 1회만 느리고 이후 snapshot 재사용
- 지표 계산: MA/52주 위치/수익률/변동성/ROE proxy
- Streamlit 프론트: 프리셋 + 커스텀 필터 + CSV 다운로드

## 빠른 시작
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
streamlit run src/stock_screener/web/app.py
```

## 배치만 실행
```bash
python -m stock_screener.cli --db-path data/screener.db
```

## 기본 프리셋
- deep_value
- rerating
- dividend_lowvol
- momentum
