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

## Coding guidelines
- Keep filter state and URL query sync in lockstep.
  - When adding/changing filters, update `FILTER_SPECS`, parse/serialize flow, and application logic together.
- Preserve backward compatibility for query/session keys when renaming filter fields.
- Avoid UI clutter: prefer one clear control path per filter and remove duplicate/overlapping controls.
- Do not wrap imports in try/catch.

## Validation checklist (before commit)
- `python -m compileall src/stock_screener/web/app.py`
- Run targeted tests if touching non-UI logic (`pytest -q` or subset).
- If front-end behavior changes, try to capture a screenshot via browser tool; if unavailable, document failure reason.

## PR checklist
- Include summary, key behavior changes, and validation commands.
- Call out compatibility notes for query params/session state when relevant.
