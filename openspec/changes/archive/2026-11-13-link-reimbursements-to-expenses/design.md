## Context

`load_spending_offsets(cur, user_id, start, end)` is the one place every spending figure gets its reimbursement effect: it groups reimbursements dated in the window by `offset_category_id`, caps each at the category's spending in the window, and apportions it over the category's pillars. Report windows are computed when read; nothing is stored per period. A reimbursement is income in a category with `offsets_spending` and is never income in reports.

## Decisions

1. **Links with stored allocations.** `reimbursement_links(reimbursement_id, expense_id, user_id, amount, position)`, primary key on the pair, both foreign keys `ON DELETE CASCADE`. The allocated amount is stored when linking and recomputed when either side changes, so every read is a plain join. Rejected: allocating at read time, which needs every reimbursement of every linked expense for each window.
2. **Allocation.** For one reimbursement: its expenses ordered by date then id; each takes `min(left, expense amount − what other reimbursements already hold on it)`. Reimbursements sharing an expense are recomputed in their own date order, so the earlier one keeps its share. An expense is never lowered below zero.
3. **Where it counts.** A linked amount lowers its expense's category and effective pillar in the window that holds the *expense's* date. It is added to `applied`, `applied_by_category`, `applied_by_pillar` and `applied_by_category_pillar`. The category fallback then sees only reimbursements without links, capped by the category's spending left after the linked part, and apportioned over the pillars' spending left after it.
4. **The rest.** When linking, the owner chooses: "Tidak dihitung" (default: the unlinked rest stays a reimbursement and counts nowhere, as `reimbursement_unallocated`) or "Dicatat sebagai uang masuk" with an income category: the reimbursement's amount is lowered to what its links hold, and a new income row with the rest is written on the same account, date and notes, in the chosen category with owner provenance. The account balance does not change. Rejected: counting part of a reimbursement as income inside every income query, which touches every report.
5. **Validation.** The reimbursement must be the owner's income in an `offsets_spending` category; each expense the owner's, `type = expense`, not a movement leg, not on a credit account's payment, in a budget-reportable category. 1 to 20 expenses. Errors are 422 with codes in both catalogues. Linking replaces the previous links. A reimbursement with links ignores its `offset_category_id`, which is cleared when links are saved.
6. **Daily series.** Day attribution keeps spreading each category's applied amount over its spending days in proportion. A linked amount therefore lands on its category's days, not only its expense's day. Kept: exact per-day placement would need a second attribution path for a chart detail.
7. **Changes and deletes.** Editing the amount, date, type or category of either side, or deleting either side, recomputes the reimbursements that share the expenses involved. An expense that stops being an expense, or a reimbursement whose category stops offsetting, loses its links.
8. **Web.** A shared `ExpensePicker`: search over the owner's expenses of the last 90 days (title, date, amount, what is still not repaid), multi-select. The reimbursement's edit dialog and Catat (for an offsetting income category) show "Mengganti transaksi" with the picker, the allocation after saving, and the rest choice when something is left. The transaction dialog of an expense lists "Diganti Rp X" with each reimbursement; of a reimbursement, what it repaid and the rest.

9. **One statement per window.** The fallback, linked and rest parts are three `UNION ALL` parts of the existing offsets query, so pages keep their fixed statement count (the ledger and calendar tests pin it).
10. **Picker scope.** The picker lists title, date and amount. It does not show what other reimbursements already hold on an expense; the server caps every allocation and the transaction dialog shows the result.

## Failure behaviour

Linking happens in one database transaction with row locks on both sides; a failed check changes nothing. Concurrent links to the same expense serialise on the expense's lock.

## Risks

Linking lowers spending in closed periods; reports opened later show the corrected figures. The split in decision 4 changes a notification row's amount; the notification itself keeps its link to the reimbursement row.

## Migration and rollback

`V55` only adds a table. Rolling back the code leaves the table unused; dropping it loses links but no money.
