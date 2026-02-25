# KR Stock Screener (pykrx + SQLite)

한국 주식 펀더멘털 스크리너입니다.

## 특징
- pykrx 기반 데이터 수집 (OHLCV, 시총, 펀더멘털)
- 펀더멘털 기본 필드 전부 수집: `PER/PBR/EPS/BPS/DIV/DPS`
- SQLite 캐시: 초기 1회만 느리고 이후 snapshot 재사용
- 지표 계산: MA/52주 위치/수익률/변동성/ROE proxy + EPS 성장률
- Streamlit 프론트: 커스텀 필터 + CSV 다운로드
- 현재 UI는 **Descriptive 탭 중심**으로 실사용 가능하도록 상세 구현


## 펀더멘털 데이터 소스 우선순위
- 기본 수집 순서: **DART (primary) -> pykrx (fallback)**
- 병합 시 tie-break 규칙:
  1. `is_correction` 우선 (정정 공시 우선)
  2. `reported_date` 최신 우선
  3. `source_priority` 높은 값 우선

## 구현 상태 (중요)
- 현재 필터 UX는 **Descriptive 탭이 중심**이며, 이 문서도 해당 탭 사용법 위주로 안내합니다.
- Fundamental/Technical 탭은 기본 제어가 존재하지만, 운영 가이드는 Descriptive 기준으로 유지합니다.


## 현재 지원 지표/컬럼 (Valuation / Growth)
- **Valuation**
  - `pe_ratio`, `forward_pe`, `ps_ratio`, `pb_ratio`, `peg_ratio`
  - `ps`, `peg`, `ev`, `ev_sales`, `ev_ebitda`
- **Growth**
  - `eps_cagr_3y`, `eps_cagr_5y`, `eps_yoy_q`, `eps_growth_ttm`, `eps_qoq`
  - `sales_growth_qoq`, `sales_growth_ttm`, `sales_cagr_3y`, `sales_cagr_5y`

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

### Streamlit 기본 asof 동작
- 기본 `asof`는 **최신 거래일(가격 데이터 기준)** 을 우선 사용합니다.
- 최신 거래일에 snapshot이 없으면 최신 snapshot 날짜로 fallback 하며, UI에서 snapshot 재계산이 필요하다는 안내를 표시합니다.
- 최신 거래일 snapshot이 비어 있는 경우 앱 시작 시 `스냅샷만 재계산`을 자동으로 1회 시도합니다.

## 배치만 실행
```bash
python -m stock_screener.cli --db-path data/screener.db
```

## 수집 현황 확인
- 최근 `daily_batch:*` 실행의 청크별 성공/실패, `row_count`, 품질 지표(`eps_non_null`, `bps_non_null`, `revenue_non_null`)를 확인할 수 있습니다.
- 가능하면 message 내 분모(`metric_total` 또는 `metric=x/y`)를 사용하고, 분모가 없으면 `row_count`를 분모로 사용해 비율을 계산합니다.

```bash
python -m stock_screener.cli --db-path data/screener.db --report-latest-batch
```

예시 출력:

```text
Latest run_id: daily_batch:2026-02-20
chunk | status  | row_count | eps_non_null          | bps_non_null          | revenue_non_null      | message
------|---------|-----------|------------------------|-----------------------|-----------------------|--------
1/2   | success |       120 | 80/120 (66.7%)        | 70/120 (58.3%)        | 90/120 (75.0%)        | chunk=1/2, ...
2/2   | failed  |       100 | 40/100 (40.0%)        | 50/100 (50.0%)        | 60/100 (60.0%)        | chunk=2/2, ...
```

## 로컬 테스트 실행
아래 명령을 로컬 표준으로 사용합니다.

```bash
PYTHONPATH=src python -m pytest -q
```

- 경로 이슈 재현/디버그가 필요하면 임시 옵션을 추가해 `sys.path[0]`를 출력할 수 있습니다.

```bash
PYTHONPATH=src python -m pytest -q --debug-sys-path
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
- CLI 한 줄(유보율 업데이트 + 스냅샷 연속 실행):
```bash
python -m stock_screener.cli --db-path data/screener.db --update-reserve-only --rebuild-snapshot
```
- Streamlit UI에서도 `유보율만 업데이트` 버튼으로 동일 기능을 실행할 수 있습니다.


## Descriptive 탭 상세 가이드

### 1) 기본 철학
- Finviz 스타일처럼 사용자가 선택한 항목만 직접 적용합니다.
- 모든 조건은 **AND 결합**됩니다. (예: 시장 + 시총 + 가격 + 배당 ...)
- 각 조건은 기본값이 `Any`이며, `Any`는 해당 조건을 비활성화한 상태입니다.

### 2) 화면 구성
- **시장 영역**
  - `티커`: 콤마/공백으로 여러 티커 입력 가능
  - `시장`: 멀티 선택
- **범위형 필터 영역** (동일한 행/컬럼 그리드 정렬)
  - 시가총액
  - 가격
  - 배당
  - 평균 거래대금
  - 상대거래량
  - 모멘텀(지표 선택 + 범위)

### 3) 범위형 필터 공통 사용법
각 범위형 필터는 동일한 3가지 모드를 가집니다.

1. `Any`
   - 필터 미적용
2. `구간 선택`
   - 사전 정의된 버킷(구간) 중 1개를 선택
3. `직접 입력`
   - 최소/최대 값을 직접 숫자로 입력

즉, 사용자는 빠르게는 `구간 선택`, 정밀하게는 `직접 입력`으로 같은 필터를 제어할 수 있습니다.

### 4) 필터별 의미
- **시가총액**: 원화 기준 시가총액 범위
- **가격**: 현재가 범위
- **배당**: 배당수익률(%) 범위
- **평균 거래대금**: 최근 20거래일 일평균 거래대금(`avg_value_20d`)
- **상대거래량**: `current_value / avg_value_20d`
- **모멘텀**: 아래 지표 중 선택 후 동일한 범위 모드 적용
  - 3개월 수익률 (`ret_3m`)
  - 6개월 수익률 (`ret_6m`)
  - 1년 수익률 (`ret_1y`)
  - 52주 신고가 근접도 (`near_52w_high_ratio`)

### 5) 데이터 가용성에 따른 자동 처리
- 평균 거래대금/상대거래량/모멘텀처럼 파생 데이터가 비어 있으면 해당 행은 자동 비활성화됩니다.
- 비활성화 시 해당 필터 모드는 자동으로 `Any`로 돌아가며, UI에 안내 메시지가 표시됩니다.

### 6) 상태 동기화 (세션 + URL)
- Descriptive 필터 상태는 세션 상태와 URL 쿼리 파라미터를 함께 동기화합니다.
- 링크를 공유하거나 새로고침해도 가능한 한 같은 필터 상태가 재현되도록 설계되어 있습니다.

### 7) 스냅샷 재계산과 필터 적용
- `스냅샷만 재계산`은 **기존 DB 캐시를 재활용**하여 `snapshot_metrics`만 다시 계산합니다.
- 필터는 계산된 스냅샷 결과에 적용되며, 외부 pykrx 전체 재호출 없이 빠르게 반복 탐색할 수 있습니다.


## `screener.dsl` 모듈 유지 기준
- Streamlit UI에서는 현재 커스텀 체크 필터만 사용하며, 프리셋 DSL은 UI 경로에서 사용하지 않습니다.
- 다만 `src/stock_screener/screener/dsl.py`는 재사용 가능한 DataFrame 필터 유틸리티이며, 단위 테스트(`tests/test_dsl.py`)에서 동작을 검증하므로 유지합니다.



## Descriptive 탭 UI 원칙
- Descriptive 필터 행은 동일한 컬럼 그리드(기준선) 정렬을 유지합니다.
- 새 필터를 추가할 때는 기존 행과 수직 기준선이 어긋나지 않도록 동일 폭 컬럼 또는 동일 그리드의 다중 행 구성 방식을 우선합니다.
- `Any / 구간 선택 / 직접 입력` 모드 전환은 기존 상태/쿼리 파라미터 동기화 규칙을 그대로 따릅니다.


## 운영 설정 및 보안
- `.env`에 `DART_API_KEY`를 설정해야 DART 기반 펀더멘털 수집이 동작합니다.
- `DART_FINANCIALS_ENDPOINT`는 **옵션**입니다. 미설정 시 DART primary provider는 endpoint 미연결 상태(`None`)로 동작하며, 파이프라인은 fallback provider 경로를 그대로 사용합니다.
- `DART_FINANCIALS_ENDPOINT`를 설정한 경우에만 해당 endpoint를 사용합니다.
- 운영 환경에서는 키를 시크릿 매니저/환경변수로 주입하고, 저장소에 직접 커밋하지 마세요.
- `.env`/API 키 평문 파일은 반드시 `.gitignore`로 관리하고, 로그 출력 시 키 마스킹을 유지하세요.

### 환경변수 제거 방법 (`DART_FINANCIALS_ENDPOINT`)
- 현재 쉘 세션에서 제거: `unset DART_FINANCIALS_ENDPOINT`
- `.env`에서 제거: `DART_FINANCIALS_ENDPOINT=...` 라인을 삭제 후 앱/프로세스 재시작
- 쉘 시작 파일(`~/.bashrc`, `~/.zshrc`)에서 `export DART_FINANCIALS_ENDPOINT=...`를 제거 후 `source`로 반영

## 실행 시간 가이드
- **전체 수집 + 스냅샷**: 최초 1회는 티커×기간 API 호출 때문에 수 분~수십 분이 걸릴 수 있습니다.
- 가격/시총 수집 기간은 `asof_date - (lookback_days * 2)`부터 `asof_date`까지의 거래일입니다. 예: `lookback_days=400`, `asof=2026-02-13`이면 대략 2023년 말부터 조회됩니다.
- EPS 성장률 계산용 펀더멘털은 별도로 최근 약 6년 구간에서 월말/분기말 거래일 앵커를 수집해, 5Y CAGR / YoY 계산에 필요한 히스토리를 확보합니다.
- **스냅샷만 재계산**: DB에 가격/시총/펀더멘털이 이미 있으면 보통 수 초~수십 초 수준입니다(환경/데이터량 의존).
