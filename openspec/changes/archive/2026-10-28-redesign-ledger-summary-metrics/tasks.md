# Tasks

## 1. Backend Aggregate Calculations

- [x] 1.1 Extend `list_transactions` in `backend/app/routers/transactions.py` to calculate running payday cycle and cumulative totals (inflow, outflow, net) in SQL aggregate query and include `summary` in the response payload. Verify with targeted test in `backend/tests/test_transactions.py`.
- [x] 1.2 Add comprehensive test cases in `backend/tests/test_transactions.py` verifying cycle window boundary clamping, internal movement exclusion, and filter preservation for cycle and cumulative summaries. Verify with `PYTHONPATH=backend backend/.venv/bin/pytest backend/tests/test_transactions.py`.

## 2. Frontend Ledger UI Redesign

- [x] 2.1 Update transaction query types and query handling in `frontend/src/app/ledger/page.tsx` to read `summary.cycle` and `summary.cumulative`. Verify with `npm run type-check`.
- [x] 2.2 Redesign desktop 3-card summary and mobile summary ribbon in `frontend/src/app/ledger/page.tsx` to display cycle window badge, cycle flow, cumulative flow, and scope switcher according to Scandinavian Tactile Minimalist standard. Verify with `npm run lint` and `npm run build`.
- [x] 2.3 Update existing tests and add new tests in `frontend/src/app/ledger/page.test.tsx` verifying rendering of cycle date range, cycle cash flow numbers, cumulative numbers, and scope toggle switching. Verify with `npm test` or `npx vitest run src/app/ledger/page.test.tsx`.

## 3. Specification Validation & Delivery

- [x] 3.1 Run `openspec validate redesign-ledger-summary-metrics` to ensure full structural compliance of OpenSpec artifacts.
- [x] 3.2 Verify clean git diff with `git diff --check` and present artifacts and visual redesign to user for approval.
