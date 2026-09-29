# Proposal

## Why

The current ledger summary cards ("Uang Masuk (Halaman Ini)", "Uang Keluar (Halaman Ini)", and "Selisih Bersih (Halaman Ini)") only compute cash flow totals across the 50 transactions visible on the active pagination page. When users navigate across multiple pages or seek to understand their true financial position, page-bound totals fail to provide actionable context: they do not reflect actual monthly payday cycle cash flow nor the all-time/filtered cumulative volume.

## What Changes

- Redesign the ledger summary section to display cash flow metrics based on:
  1. **Running Cycle (Siklus Berjalan)**: Inbound, outbound, and net cash flow within the user's active payday cycle window (e.g., 25th of previous month to 24th of current month).
  2. **Cumulative Overall (Total Kumulatif)**: Inbound, outbound, and net cash flow across the full filtered dataset (or all-time when no filters are applied).
- Update backend `GET /transactions` endpoint to return aggregated cash flow summary data (`summary.cycle` and `summary.cumulative`) computed directly in SQL, avoiding extra round-trips and bypassing pagination limits.
- Redesign the desktop 3-card summary and mobile summary ribbon in `frontend/src/app/ledger/page.tsx`:
  - Desktop: Prominent cycle metrics with cycle date badge (e.g. `25 Sep – 24 Okt`), alongside cumulative totals and an interactive toggle/dual-tier view.
  - Mobile: Clean summary ribbon with a cycle window indicator and scope toggle between cycle and cumulative cash flow.

## Capabilities

### Modified Capabilities
- `transaction-ledger-management`: Update requirement 1 to specify running payday cycle cash flow and overall cumulative cash flow summaries rather than pagination-limited page sums.

## Impact

- **Backend**: `backend/app/routers/transactions.py` query logic updated to calculate `cycle` and `cumulative` inflow, outflow, and net totals in SQL.
- **Frontend**: `frontend/src/app/ledger/page.tsx` UI and data models updated to render the dual-tier cycle and cumulative metrics.
- **Tests**: Extended tests in `backend/tests/test_transactions.py` and `frontend/src/app/ledger/page.test.tsx` verifying exact cycle and cumulative aggregation.
