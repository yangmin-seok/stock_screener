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


## 프리셋/필터 동작 방식
- 프리셋은 자주 쓰는 3개 조건 묶음을 한 번에 적용하는 시작점입니다.
- `none`을 선택하면 **프리셋 조건은 0개**이며, 사용자가 체크한 커스텀 조건만 적용됩니다.
- 필터는 체크된 조건만 순차 적용되며, 선택한 조건들끼리는 AND로 결합됩니다.
- UI에서 `스냅샷만 재계산` 버튼을 누르면 이미 수집된 DB 캐시로 `snapshot_metrics`만 다시 계산합니다(외부 pykrx 재호출 없음).

## 기본 프리셋
- deep_value
- rerating
- dividend_lowvol
- momentum
- eps_growth_breakout


## 실행 시간 가이드
- **전체 수집 + 스냅샷**: 최초 1회는 티커×기간 API 호출 때문에 수 분~수십 분이 걸릴 수 있습니다.
- **스냅샷만 재계산**: DB에 가격/시총/펀더멘털이 이미 있으면 보통 수 초~수십 초 수준입니다(환경/데이터량 의존).

