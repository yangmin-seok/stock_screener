# KR Stock Screener (pykrx + SQLite)

한국 주식 펀더멘털 스크리너입니다.

## 특징
- pykrx 기반 데이터 수집 (OHLCV, 시총, 펀더멘털)
- 펀더멘털 기본 필드 전부 수집: `PER/PBR/EPS/BPS/DIV/DPS`
- SQLite 캐시: 초기 1회만 느리고 이후 snapshot 재사용
- 지표 계산: MA/52주 위치/수익률/변동성/ROE proxy + EPS 성장률
- Streamlit 프론트: 프리셋 + 커스텀 필터 + CSV 다운로드

## 성장 스크리너 지표
- `eps_cagr_5y`: 최근 5년 EPS CAGR (근사치)
- `eps_yoy_q`: 최근 분기 EPS YoY (분기말 스냅샷 기준 근사치)
- `near_52w_high_ratio`: 현재가 / 52주 신고가

> 참고: pykrx 원천 특성상 분기 EPS YoY는 분기말 기준 EPS 스냅샷으로 계산한 근사치입니다.

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
- eps_growth_breakout
