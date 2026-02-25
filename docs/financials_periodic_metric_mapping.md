# financials_periodic 기반 지표 매핑 (v1.4)

## 구현 대상 지표

### Valuation
- P/E: `snapshot_metrics.per` (fundamental_daily 원천)
- Forward P/E: 원천 없음 → `NaN`
- P/S: `mcap / revenue_ttm` (`financials_periodic.revenue` 분기 4개 합)
- P/B: `snapshot_metrics.pbr` (fundamental_daily 원천)
- PEG: `P/E / (EPS Past 5Y * 100)`
- EV/Sales: EV 원천(순차입금/현금) 없음 → `NaN`
- EV/EBITDA: EBITDA/EV 원천 없음 → `NaN`

### Profitability
- Gross Margin: 매출원가 원천 없음 → `NaN`
- Operating Margin: `operating_income / revenue` (최신 periodic row)
- Net Margin: `net_income / revenue` (최신 periodic row)
- ROA: 총자산 원천 없음 → `NaN`
- ROE: `eps / bps` (proxy)
- ROIC: 투자자본 원천 없음 → `NaN`

### Growth
- EPS Q/Q: `financials_periodic.eps` 분기 series의 최근 2개
- EPS TTM: `financials_periodic.eps` 분기 series의 최근 8개로 TTM YoY
- EPS Past 5Y: `financials_periodic.eps` 연간 series 5Y CAGR
- Sales Q/Q: `financials_periodic.revenue` 분기 series의 최근 2개
- Sales TTM: `financials_periodic.revenue` 분기 series의 최근 8개로 TTM YoY
- Sales Past 5Y: `financials_periodic.revenue` 연간 series 5Y CAGR

### Leverage/Liquidity
- Debt/Equity: 부채 원천 없음 → `NaN`
- LT Debt/Equity: 장기부채 원천 없음 → `NaN`
- Current Ratio: 유동자산/유동부채 원천 없음 → `NaN`
- Quick Ratio: 당좌자산 원천 없음 → `NaN`
- Payout Ratio: `dps / eps` (`snapshot_metrics.dps`, `snapshot_metrics.eps`)

## 가용성/NaN 처리 규칙
- 계산식 분모가 `<= 0` 또는 결측이면 `NaN`.
- 입력 샘플 부족(예: TTM 8분기 미만, CAGR 6개 연간점 미만)이면 `NaN`.
- UI Fundamental 필터는 대상 컬럼에 non-null 값이 하나도 없으면 비활성화하고 `Any` 동작으로 폴백.

## 호환성 규칙
- 기존 키 `eps_yoy_q` 및 `apply_eps_yoy_q`는 유지.
- 내부 계산은 `EPS Q/Q`와 동일 값으로 채워 기존 쿼리 파라미터/세션 상태를 깨지 않음.
- 신규 지표는 신규 컬럼/필터 키로 추가 (`eps_qoq`, `sales_growth_qoq`, `sales_growth_ttm`, `sales_cagr_5y`).
