# AGENTS.md

## Repository expectations
- Work from `/workspace/stock_screener`.
- Primary app entrypoint is `src/stock_screener/web/app.py`.
- Keep UI behavior consistent with Finviz-like filtering (Descriptive/Fundamental/Technical tabs, direct user-selected filters).

## Current product decisions
- Presets are removed; do not reintroduce preset-based filtering unless explicitly requested.
- Default `asof` follows latest trading day data with snapshot fallback.
- Long-running jobs must remain cancellable from UI.
- Descriptive tab should support range-based selectors (market cap, price, dividend yield) with `Any / 구간 선택 / 직접 입력`.
- In Descriptive tab, keep control rows visually aligned to the same column grid; avoid mixing incompatible column splits that break vertical line-up.
- In Technical tab, keep `변동성` and `외국인` controls separated; do not merge into one row/group.
- Foreign screener metric is fixed to `foreign_net_buy_value_20d` (20D 누적금액, KRW).

## Coding guidelines
- Keep filter state and URL query sync in lockstep.
  - When adding/changing filters, update `FILTER_SPECS`, parse/serialize flow, and application logic together.
- Preserve backward compatibility for query/session keys when renaming filter fields.
  - Legacy `foreign_buy2_*` keys should be safely pruned from query sync while preserving old shared-link behavior.
- Avoid UI clutter: prefer one clear control path per filter and remove duplicate/overlapping controls.
- If a new UI row needs extra fields, prefer multi-row composition on the same base grid instead of introducing a conflicting grid width.
- Do not wrap imports in try/catch.

## Validation checklist (before commit)
- `python -m compileall src/stock_screener/web/app.py`
- Run targeted tests if touching non-UI logic (`pytest -q` or subset).
- If front-end behavior changes, try to capture a screenshot via browser tool; if unavailable, document failure reason.

## PR checklist
- Include summary, key behavior changes, and validation commands.
- Call out compatibility notes for query params/session state when relevant.
- Explicitly verify query/session key backward compatibility when filter/state keys are changed.
- If snapshot schema/columns changed, confirm DB migration path (`init_db`/`_ensure_column`) before merge.


## Backtest/UI operation notes (2026-02)
- Backtest tab rebalance selector supports `W / M / Y` (weekly/monthly/yearly).
- If users report the backtest gauge looks frozen at the beginning, verify UI progress rendering first:
  - progress bar now reserves a small visible baseline in early stages (including `total=0` events),
  - then maps engine `processed/total` updates to the visible range until completion.
- This is a display smoothing change only; execution order/cancellation semantics are unchanged.

## Fundamental handoff notes (2026-02)

### Current status
- DART primary provider + pykrx fallback provider are connected and running in production batch path.
  - `DART_FINANCIALS_ENDPOINT`는 운영 override 용도이며, 미설정 시 endpoint 기본값은 `None`으로 유지한다.
  - endpoint 미설정 상태에서는 DART provider output이 0 rows일 수 있으며 fallback(pykrx) 경로가 정상 동작해야 한다.
  - Financial merge priority is applied as `is_correction > reported_date > source_priority`.
- Quarterly financial canonicalization is **implemented and 운영 중** via `financials_periodic`.
  - `financials_daily` remains collection/audit history keyed by collection date.
  - Growth/snapshot paths consume periodic axis data (`fiscal_period`, `period_type`) with dedupe/tie-break rules.
- EPS/BPS null observability is **강화 완료/운영 중**.
  - Batch logs include source output, merged output, and pre-upsert null/non-null counts by chunk/date.
- Remaining issue: upstream source gaps can still produce snapshot-level `eps`, `bps` nulls on 일부 종목.

### Env operation note (DART)
- `DART_FINANCIALS_ENDPOINT` 제거 방법:
  - 현재 세션: `unset DART_FINANCIALS_ENDPOINT`
  - `.env`: 해당 키 라인 삭제 후 프로세스 재시작
  - 쉘 시작 파일(`~/.bashrc`, `~/.zshrc`): `export` 라인 제거 후 `source`

### Known risks
- Growth metrics can be biased if repeated fiscal periods are treated as independent time points.
- Finviz-style quarterly filters (`Q/Q`, `TTM`) require fiscal-period-normalized series, not collection-date snapshots.
- Snapshot-level `eps`, `bps` can remain null when upstream source is missing or when merge/upsert leaves gaps.

### Next-agent implementation plan
1. **EPS/BPS null 원인 분석 및 보정 전략 고도화 (미완료)**
   - Null 관측 지표를 기반으로 source/종목/기간별 결측 패턴을 정량화.
   - merge/upsert 이후 null이 증가하는 케이스를 분리해 재현 가능한 회귀 테스트 추가.
2. **Snapshot latest-financial selection 운영 규칙 명문화 (부분 완료, 추가 작업 필요)**
   - 현재 tie-breaker(`is_correction > reported_date > source_priority`)를 운영 문서/테스트에 고정.
   - 예외 케이스(동일 reported_date 다중 소스, 결측 reported_date)에 대한 deterministic 규칙 보강.
3. **Finviz-style quarterly/TTM 필터 검증 강화 (미완료)**
   - fiscal-period 정규화 데이터 기준으로 Q/Q, TTM 필터 산출값 검증 테스트 확장.
   - UI 노출 필터와 백엔드 계산식 간 불일치 여부를 릴리스 전 체크리스트에 포함.

### 10-year backfill operating SOP (2 years x 5 chunks)
- Execute long historical collection in chunks: `chunk_years=2`, `chunks=5`.
- Recommended command:
  - `python -m stock_screener.cli --db-path data/screener.db --asof-date <YYYY-MM-DD> --lookback-days 3650 --chunk-years 2 --chunks 5`
- After long collection, rebuild snapshot once:
  - `python -m stock_screener.cli --db-path data/screener.db --asof-date <YYYY-MM-DD> --snapshot-only --lookback-days 3650`
- Recent pipeline behavior:
  - When `lookback_days>=3650`, pipeline may auto-enable long-window price/cap/investor-flow backfill if DB earliest price is newer than target start.
  - Keep using chunk SOP for predictable operation and easier resume/retry.
- Track per chunk:
  - financial rows upserted
  - `eps_non_null` / `bps_non_null`
  - failures/retries
- On failure, restart from the failed chunk; avoid full 10-year rerun unless schema changed.

### Validation expectations for this area
- Mandatory compile check: `python -m compileall src/stock_screener/web/app.py`
- If non-UI logic changed, run targeted tests (`pytest -q` or subset).
- If fundamental filtering UI changes, capture a screenshot via browser tool when possible.
