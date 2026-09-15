# Proposal: Decouple Kakeibo Classification from Categories to Transaction-Level

## Why
Categories represent broad domains of spending (e.g. *Makanan & Minuman*, *Belanja*, *Transportasi*). Currently, categories are bound to a rigid `kakeibo_type` (`need`, `want`, `saving`), which forces labels like `Kesehatan (need)` into dropdowns, causes automatic overriding of user selection in Quick Capture, and artificially constrains categories. In reality, purchases under the same category can be either a vital Need or a discretionary Want depending on the transaction context.

## What Changes
- Decouple Kakeibo classification completely from Categories:
  - Remove `(need)` / `(want)` suffixes from category dropdowns and select options.
  - Remove automatic overriding in `QuickCaptureModal.tsx` so user's chosen Kakeibo pill remains active when selecting a category.
  - Remove rigid Kakeibo pillar selector from the Category Create/Edit modal in `insights/page.tsx`.
  - Remove redundant `Need` / `Want` pill badges from category tables and `CategoryDonutChart.tsx`.
  - In backend `dashboard.py`: aggregate Kakeibo breakdown directly from `COALESCE(t.kakeibo_type, 'need')` without falling back to category constraints.

## Capabilities
- `kakeibo-decoupling`: Transaction-centric Kakeibo classification with clean category presentation and unconstrained category management.

## Impact
- Frontend: `QuickCaptureModal.tsx`, `CategoryDonutChart.tsx`, `insights/page.tsx`.
- Backend: `dashboard.py` (Kakeibo aggregation query).
- Database: No schema alteration required (columns already support transaction-level `kakeibo_type`).

## Expected Outcome
- Category names appear clean and uncluttered across all UI surfaces.
- Users choose whether an expense is a Need, Want, or Saving directly per transaction without being forced or overridden by category selections.
- Kakeibo 50/30/20 breakdown accurately reflects real user transaction intent.
