# Design: apply-unlinked-reimbursements-to-total

## Context

See `proposal.md` ("Why"). In `backend/app/services/reimbursements.py`, `load_spending_offsets` computes three SQL parts in one query: `fallback` (unlinked reimbursements), `linked` (allocated `reimbursement_links`), and `rest` (unallocated remainder of linked reimbursements). Today, `fallback` rows with `offset_category_id IS NULL` are added to `unallocated` and reduce nothing.

## Goals / Non-Goals

- **Goals:** Automatically apply unlinked, untagged reimbursements to period `Pengeluaran` and `Belanja` without distorting individual categories/pillars; surface that deduction explicitly wherever categories/pillars appear beside the total; safely auto-link unambiguous 1-to-1 exact-amount pass-throughs within 14 days.
- **Non-Goals:** Fuzzy matching of split/rounded repayments; modifying essential monthly living cost (`need + unclassified` only); adding database migrations.

## Decisions

### 1. Separate unlinked reimbursements from linked remainders and apply them to totals

In `backend/app/services/reimbursements.py` `load_spending_offsets()`:
- Add one `UNION ALL` part (`'totals'`) inside the existing single query to read the window's `total_gross_outflow` (reportable expenses) and `total_gross_spending` (reportable expenses outside the `saving` pillar), preserving the single-statement contract.
- `fallback` rows with `offset_category_id IS NULL` accumulate into `unlinked_raw` rather than `unallocated`.
- `rest` rows (from explicitly linked reimbursements where the owner left the remainder uncounted) stay in `unallocated` and still reduce nothing.
- Compute:
  - `applied_unlinked = min(unlinked_raw, max(0, total_gross_outflow - category_applied))`
  - `applied_unlinked_spending = min(unlinked_raw, max(0, total_gross_spending - living_category_applied))`
  - `excess += unlinked_raw - applied_unlinked`
- On `SpendingOffsets`:
  - `applied = category_applied + applied_unlinked`
  - `applied_unlinked` and `applied_unlinked_spending` stored explicitly
  - `living` returns `living_category_applied + applied_unlinked_spending`

**Rejected:** Spreading unlinked reimbursements proportionally across all spending categories/pillars (distorts unrelated categories like Utilities or Equipment when a friend repays a shared dinner).

### 2. Explicit deduction line keeps every breakdown reconciling to the exact rupiah

- `spending_basis.py`: `total = max(0, net_cat_pillar_sum - offsets.applied_unlinked_spending)`, `reimbursed = cat_pillar_applied + offsets.applied_unlinked_spending`, `unlinked_reimbursed = offsets.applied_unlinked_spending`.
- `dashboard.py` (`get_kakeibo_breakdown`): keep pillar totals untouched; add `"unlinked_reimbursement": offsets.applied_unlinked_spending`; subtract `offsets.applied_unlinked_spending` from `total_allocated`.
- `cycle_summary.py` (`_keluar`): when `kakeibo["unlinked_reimbursement"] > 0`, append a line with `key: "unlinked_reimbursement"`, `amount: -unlinked`, `label: t(language, "labels.summary.unlinked_reimbursements")`, `href: ledger_href(period, type="income")`. Both `_keluar` and `spending_split` keep their exact sum assertions (`sum(lines) == total`).
- `dashboard.py` (`_spending_explanation`): uses the same lines, so Analisis Rincian automatically shows `"Ganti uang belum ditautkan −Rp X"` and adds up to `Pengeluaran`.
- `monthly_comparison.py`: `PeriodAmounts` carries `unlinked_reimbursement: int = 0`; `spending = max(0, sum(pillars) - unlinked_reimbursement)`. Expose `unlinked_reimbursement` on the comparison response so `SpendingTable.tsx` (Laporan) and `KakeiboRows.tsx` (Analisis) render the deduction row when > 0.
- `transactions.py` (`ledger_cash_flow`): unfiltered window uses `offsets.applied` (includes unlinked); category/pillar filters use `offsets.for_category(...)` / `offsets.for_pillar(...)` (excludes unlinked, since unlinked rows name no category or pillar).
- `credit_figures.py` / `cycle_summary.py`: guard `spending_on_credit["spent_on_credit"] <= spending["amount"]` by capping `spent_on_credit` to `min(spent_on_credit, spending["amount"])` when unlinked reimbursements lower net `Pengeluaran` below gross credit purchases (prevents false `RuntimeError`).

### 3. Strict 1-to-1 exact-amount auto-linking on reimbursement arrival/recategorization

In `backend/app/services/reimbursement_links.py`, add `auto_link_exact_reimbursement(cur, user_id: str, reimbursement_id: str) -> str | None`:
- Preconditions: transaction is `income`, category has `offsets_spending = TRUE`, `offset_category_id IS NULL`, `movement_id IS NULL`, and has zero rows in `reimbursement_links`.
- Query reportable, non-movement expenses of `user_id` where `amount = r.amount`, `date >= r.date - INTERVAL '14 days' AND date <= r.date`, and no existing positive allocation in `reimbursement_links` (`LIMIT 2`).
- Link **only** when `len(matches) == 1`: insert `(reimbursement_id, expense_id, user_id)` into `reimbursement_links` and call `reallocate()`.
- Invoke from: manual create (`POST /transactions`), manual update (`PUT /transactions/{id}`) only when switching into an offsetting category without explicit `offset_category_id` or links, text capture recording (`text_capture.py`), and notification recording (`notification_application.py`). Never invoked when the owner removes links (`DELETE /transactions/{id}/reimburses`).

**Rejected:** Matching approximate/rounded amounts automatically (risks false links between unrelated items).

## Failure Behavior, Compatibility, and Rollback

- **Compatibility:** Additive API fields (`unlinked_reimbursement`, extra negative line in `keluar.lines` only when > 0). No migration.
- **Rollback:** Reverting the deploy restores prior calculation; any auto-created `reimbursement_links` rows remain normal valid links editable in the UI.

## Risks / Trade-offs

- [A rare coincidence where an unrelated expense of the exact same rupiah amount occurred within 14 days of a reimbursement could auto-link] → Limited to exact 1-to-1 single match within 14 days, visibly shown in the transaction dialog ("Mengganti transaksi"), and removable with one click without ever being re-linked.
- [On a period with unlinked reimbursements, daily bars in `show-daily-spending-at-full-price` sum to gross while total is net] → Completely consistent with Change 1: `"Rp A dibelanjakan · −Rp B diganti"` includes `applied_unlinked_spending` in `B`, so `A − B = Pengeluaran`.
