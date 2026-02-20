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
- `eps_yoy_q`: 최근 EPS 스냅샷 기준 1년 전 대비 YoY (근사치)
- `near_52w_high_ratio`: 현재가 / 52주 신고가

> 참고: pykrx 원천 특성상 분기/월말 스냅샷 EPS를 기준으로 계산한 근사치입니다.

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

## 유보율 업데이트
- CLI에서 유보율만 갱신(네이버 크롤링):
```bash
python -m stock_screener.cli --db-path data/screener.db --update-reserve-only
```
- 유보율 갱신 후 스냅샷 재계산:
```bash
python -m stock_screener.cli --db-path data/screener.db --snapshot-only
```
- Streamlit UI에서도 `유보율만 업데이트` 버튼으로 동일 기능을 실행할 수 있습니다.


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
- 가격/시총 수집 기간은 `asof_date - (lookback_days * 2)`부터 `asof_date`까지의 거래일입니다. 예: `lookback_days=400`, `asof=2026-02-13`이면 대략 2023년 말부터 조회됩니다.
- EPS 성장률 계산용 펀더멘털은 별도로 최근 약 6년 구간에서 월말/분기말 거래일 앵커를 수집해, 5Y CAGR / YoY 계산에 필요한 히스토리를 확보합니다.
- **스냅샷만 재계산**: DB에 가격/시총/펀더멘털이 이미 있으면 보통 수 초~수십 초 수준입니다(환경/데이터량 의존).
