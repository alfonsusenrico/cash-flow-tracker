## Approval

Pending. Requested by the owner on 2026-11-24 ("continue to plan") after Kalender showed Rp 98.982 for 23 November, whose expenses add up to Rp 109.290.

## Why

A reimbursement linked to one expense is spread today over every day its category and pillar were spent in the period. On 31 October two friends repaid Rp 136.900 of the Mie Tore meal; that repayment lowers every "Makanan & Minuman" day of the cycle by its share, so 23 November showed Rp 98.982 instead of the Rp 109.290 its rows add up to, and 31 October looks dearer than it was. The period figures are right, but no single day can be checked against its rows.

## Expected Outcome

- A day's Pengeluaran equals its expenses minus what was repaid on those expenses: 23 November reads Rp 109.290, and 31 October shows the Mie Tore meal net of the friends' share.
- Every period total, category and pillar figure stays exactly as it is.
- A reimbursement without links (only a category) still spreads over its category's days, because it belongs to no single day.

## What Changes

- The daily attribution of applied offsets: a linked amount lowers its expense's own day, category and pillar; only unlinked category reimbursements and the rest of a linked reimbursement keep the proportional spread.
- Everything built on the daily basis follows: Ringkasan's daily chart and table (pillar and category columns), Kalender's day totals and day panel, the heatmap and the trendline.

## Non-goals

- How much is offset in a period, or in which period (unchanged).
- Linking, unlinking or the reimbursement screens.

## Capabilities

### New Capabilities
None.

### Modified Capabilities
- `reimbursement-offsets`: "Offset Attribution to Spending Days".

No project capability is added; `money` applies: totals must not change, verified by tests on the existing seeds.

## Impact

- **Backend:** `services/spending_basis.py` (daily attribution), the offset loader in `services/reimbursements.py` if it must return linked parts per expense day, `services/report_figures.py` (`daily_offsets`) where it builds the same series; tests.
- **Frontend, data:** no change; no migration.
