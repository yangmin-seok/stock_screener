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

## Coding guidelines
- Keep filter state and URL query sync in lockstep.
  - When adding/changing filters, update `FILTER_SPECS`, parse/serialize flow, and application logic together.
- Preserve backward compatibility for query/session keys when renaming filter fields.
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

## Fundamental handoff notes (2026-02)

### Current status
- Quarterly financial canonicalization is **not implemented yet**.
  - `financials_daily` currently stores collected rows keyed by `(date, ticker, fiscal_period, period_type, consolidation_type)`.
  - This means identical fiscal periods can exist across multiple collection dates.
- `EPS/BPS` are being populated via `PykrxFinancialFallbackProvider`; nulls are still observed in production snapshots and need diagnosis.

### Known risks
- Growth metrics can be biased if repeated fiscal periods are treated as independent time points.
- Finviz-style quarterly filters (`Q/Q`, `TTM`) require fiscal-period-normalized series, not collection-date snapshots.
- Snapshot-level `eps`, `bps` can remain null when upstream source is missing or when merge/upsert leaves gaps.

### Next-agent implementation plan
1. **Introduce canonical periodic financial storage**
   - Add a periodic/canonical table (e.g., `financials_periodic`) keyed by
     `(ticker, fiscal_period, period_type, consolidation_type)`.
   - Keep `financials_daily` as collection/audit history.
2. **Switch growth inputs to fiscal period axis**
   - In growth computation paths, sort/index by `fiscal_period` (with `period_type` guards),
     not by collection `date`.
   - Enforce dedupe rules for same fiscal period: correction/latest report/source priority.
3. **Add EPS/BPS null observability**
   - Log null ratios at (a) source collection, (b) post-merge, (c) pre-upsert.
   - Include chunk/date-level counts in batch logs for quick diagnosis.
4. **Stabilize snapshot latest-financial selection rule**
   - Make tie-breakers explicit when selecting one latest financial row for snapshot (`reported_date`, corrections, source priority).
   - Keep current output keys for backward compatibility (`fiscal_period`, `period_type`, `reported_date`, `financial_source`).

### 10-year backfill operating SOP (2 years x 5 chunks)
- Execute long historical collection in chunks: `chunk_years=2`, `chunks=5`.
- For chunk 1~4: collect only (`rebuild_snapshot=False`).
- After chunk 5: run snapshot rebuild once.
- Track per chunk:
  - financial rows upserted
  - `eps_non_null` / `bps_non_null`
  - failures/retries
- On failure, restart from the failed chunk; avoid full 10-year rerun unless schema changed.

### Validation expectations for this area
- Mandatory compile check: `python -m compileall src/stock_screener/web/app.py`
- If non-UI logic changed, run targeted tests (`pytest -q` or subset).
- If fundamental filtering UI changes, capture a screenshot via browser tool when possible.
