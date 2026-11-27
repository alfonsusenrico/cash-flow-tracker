# Tasks: apply-unlinked-reimbursements-to-total

## 1. Backend unlinked reimbursement accounting

- [x] 1.1 Update `SpendingOffsets` and `load_spending_offsets` in `backend/app/services/reimbursements.py` to separate unlinked untagged reimbursements from linked `rest`, read window gross outflow and gross spending in the same query, apply unlinked reimbursements to period totals (`applied`, `applied_unlinked`, `applied_unlinked_spending`, `living`) floored at zero, and report overflow in `excess`. Evidence: unit and DB tests covering unlinked reduction, floor-at-zero excess, and preserving explicit linked `rest` as `unallocated`.
- [x] 1.2 Propagate `applied_unlinked_spending` through `spending_basis.py`, `dashboard.py` (`get_kakeibo_breakdown`, `overview`, `analytics`, `_spending_explanation`), `cycle_summary.py`, `monthly_comparison.py`, `monthly_report.py`, `calendar.py`, `money_flow.py`, `credit_figures.py`, `transactions.py` (`ledger_cash_flow`), and backend i18n messages (`labels.summary.unlinked_reimbursements` in `id` and `en`). Evidence: cross-endpoint reconciliation tests proving Beranda, Analisis, Kalender, Laporan, and unfiltered Transaksi all report identical `Pengeluaran` with unlinked reimbursements while category/pillar filters stay unaffected and `spending_on_credit` never triggers a false `RuntimeError`.

Expected Result: unlinked reimbursements reduce total `Pengeluaran` and `Belanja` consistently everywhere with every Rincian and summary invariant intact.

## 2. Exact single-match auto-linking

- [x] 2.1 Implement `auto_link_exact_reimbursement` in `backend/app/services/reimbursement_links.py` and call it on reimbursement creation (manual `POST /transactions`, `text_capture.py`, `notification_application.py`) and recategorization into an offsetting category without explicit links or `offset_category_id` (`PUT /transactions/{id}`). Evidence: DB tests covering 1-to-1 exact match within 14 days (auto-links), split/rounded amount (stays unlinked), two same-amount expenses (ambiguous, stays unlinked), expense >14 days old (stays unlinked), and manual `DELETE /reimburses` (never re-links).

Expected Result: unambiguous 1-to-1 pass-through repayments auto-link automatically without guessing on split or rounded transfers.

## 3. Frontend breakdown line

- [x] 3.1 Update `frontend/src/lib/monthlyReport.ts`, `frontend/src/components/report/SpendingTable.tsx`, `frontend/src/components/analysis/KakeiboRows.tsx`, and `frontend/src/messages/{id,en}.json` to render `"Ganti uang belum ditautkan"` / `"Unlinked reimbursements"` (`−Rp …`, masked when balances are hidden) whenever `unlinked_reimbursement > 0` and omit it when zero. Evidence: frontend tests for both non-zero, zero, and hidden-balance cases; `type-check`, `lint`, `test`, and `build`.

Expected Result: Laporan, Analisis, and Rincian visibly reconcile categories/pillars to total `Pengeluaran` whenever unlinked reimbursements apply.

## 4. Verify and deliver

- [x] 4.1 Run full backend suite against migrated disposable PostgreSQL (`TEST_DATABASE_URL`, `REQUIRE_TEST_DATABASE=1`, reporting any skips), full frontend checks, `openspec validate --all --strict`, and browser check of Laporan and Analisis on a production build with synthetic data at 1440 and 390 px, light and dark, axe and console clean; write `verification.md`.
- [ ] 4.2 After owner acceptance: archive with `openspec archive apply-unlinked-reimbursements-to-total`, commit, push `feat/apply-unlinked-reimbursements`, and when the owner says "open", create the PR (`gh pr create`) and report its URL.

Expected Result: verified PR ready for owner review and merge.
