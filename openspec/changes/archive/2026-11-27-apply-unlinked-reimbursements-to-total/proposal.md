# Proposal: apply-unlinked-reimbursements-to-total

## Approval

Approved by the owner on 2026-11-27 ("ok, do all") after discussing split and rounded repayments where manual linking is impractical for every transfer.

## Why

An income row in "Patungan & Ganti Uang" (`offsets_spending = true`) that has neither a linked expense nor an `offset_category_id` is excluded from income and subtracts Rp 0 from spending. When friends repay shared bills in split or rounded amounts (such as rounding a Rp 49.714 share to Rp 50.000), manual linking for every transfer is heavy, yet skipping it inflates `Pengeluaran` and understates `Selisih` across Laporan, Analisis, Beranda, and Transaksi.

## Expected Outcome

- Every unlinked, untagged reimbursement in a period automatically reduces that period's total `Pengeluaran` and `Belanja` (floored at zero), keeping headline totals, `Selisih`, and Beranda summaries accurate without requiring manual linking.
- Individual categories and Kakeibo pillars stay truthful without fake proportional spreading; whenever unlinked reimbursements reduce total spending, Laporan, Analisis, and Rincian show one explicit line ("Ganti uang belum ditautkan −Rp X" / "Unlinked reimbursements −Rp X") so every breakdown visibly adds up to `Pengeluaran`.
- When a newly recorded or recategorized reimbursement without links or offset category matches **exactly one** unlinked reportable expense of the exact same amount within the prior 14 days, the backend auto-links them (editable and removable anytime, never guessing when split/rounded or ambiguous).
- If the owner explicitly links a reimbursement and leaves the remainder as "Tidak dihitung", that explicit choice is preserved and stays unallocated.

## What Changes

- **Unlinked reimbursement deduction:** Update `load_spending_offsets` and `SpendingOffsets` in `backend/app/services/reimbursements.py` to separate unlinked untagged reimbursements (`NOT EXISTS reimbursement_links AND offset_category_id IS NULL`) from the explicitly unallocated remainder of linked reimbursements (`rest`), and apply unlinked reimbursements to period totals (`applied`, `applied_unlinked`, `applied_unlinked_spending`, `living`), floored at zero with overflow reported in `excess`.
- **Total-level propagation across reporting services:** Update `spending_basis.py`, `dashboard.py` (`get_kakeibo_breakdown`, `overview`, `analytics`, `explanations`), `cycle_summary.py`, `monthly_comparison.py`, `monthly_report.py`, `calendar.py`, `money_flow.py`, `transactions.py` (`ledger_cash_flow`), and `credit_figures.py` so every period total reflects `applied_unlinked_spending`, while individual categories, pillars, and `essential_cost` stay untouched and every sum invariant includes the explicit unlinked deduction line.
- **Exact 1-to-1 auto-linking:** Add `auto_link_exact_reimbursement` in `backend/app/services/reimbursement_links.py` and invoke it when a reimbursement is created (manual, capture, notification) or recategorized into an offsetting category without explicit links or `offset_category_id`, linking only when exactly one unlinked reportable expense of the exact same amount occurred in the preceding 14 days.
- **Frontend breakdown line:** Show the `"Ganti uang belum ditautkan"` / `"Unlinked reimbursements"` deduction row in `SpendingTable.tsx` (Laporan), `KakeiboRows.tsx` (Analisis), and Rincian/Beranda (`ExplanationDialog` / `cycle_summary` lines) whenever `unlinked_reimbursement > 0`, masked when balances are hidden.

## Non-goals

- Fuzzy or approximate amount matching for split or rounded repayments (unlinked total deduction handles them without guessing).
- Spreading unlinked reimbursements proportionally across unrelated categories or pillars.
- Changing how explicitly linked reimbursements or category-tagged reimbursements work.
- Database migrations (reuses existing `reimbursement_links` table).
- Production writes or manual backfills; verification is strictly local on synthetic data.

## Capabilities

### New Capabilities
None.

### Modified Capabilities
- `reimbursement-offsets`: unlinked untagged reimbursements reduce period totals (`Pengeluaran` and `Belanja`) floored at zero while leaving individual categories and pillars intact; exact 1-to-1 same-amount repayments within 14 days auto-link when unambiguous; explicitly unallocated remainders of linked reimbursements stay unallocated.
- `monthly-report`: pillar and category breakdowns reconcile to `Pengeluaran` via an explicit unlinked-reimbursements deduction line whenever `unlinked_reimbursement > 0`.

No project capability is added; existing `money` and `database` standards apply.

## Impact

- **Backend:** `reimbursements.py`, `reimbursement_links.py`, `spending_basis.py`, `dashboard.py`, `cycle_summary.py`, `monthly_comparison.py`, `monthly_report.py`, `calendar.py`, `money_flow.py`, `credit_figures.py`, `transactions.py`, `text_capture.py`, `notification_application.py`, `i18n/messages/{id,en}.json`, and backend tests.
- **Frontend:** `SpendingTable.tsx`, `KakeiboRows.tsx`, `lib/monthlyReport.ts`, `frontend/src/messages/{id,en}.json`, and frontend tests.
- **API & Data:** Additive fields (`unlinked_reimbursement` on comparison/kakeibo, `unlinked_reimbursement` line in Beranda/Rincian when > 0); no migration.
