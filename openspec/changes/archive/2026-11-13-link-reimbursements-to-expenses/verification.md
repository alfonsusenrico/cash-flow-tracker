# Verification: link-reimbursements-to-expenses

Date: 2026-11-13 · Branch `feat/link-reimbursements-to-expenses` · Base `origin/main` `658ee71`. Plan approved by the
owner on 2026-11-13 ("ok then do it").

## Server (tasks 1.1 to 1.5)

- `V55__reimbursement_links.sql` applies after V54 (Flyway V1→V55 on Postgres 16).
- `reimbursement_links` service: allocation by reimbursement date, each expense by date then id, never more than the
  expense less what earlier reimbursements hold; recomputation across every reimbursement sharing an expense; the rest
  as a separate income row with owner provenance on the same account, date and notes.
- `load_spending_offsets`: one statement with three `UNION ALL` parts (fallback, linked, rest), so the pages' pinned
  statement counts hold. Linked parts lower their expense's category and pillar in the expense's window; the category
  fallback only covers reimbursements without links, capped by what the linked parts leave.
- Hooks: transaction edits, deletes and movement merges drop invalid links and recompute what they touch.
- `tests/test_reimbursement_links.py`, 9 tests: the CG Fun case (60.600, 193.555, 45.845 in the previous cycle, nothing in
  this one), the coffee alone with the rest unallocated, two reimbursements on one expense, never below zero, validation
  (another owner's row, an income, a missing row, 0 and 21 expenses, a non-reimbursement, a spending category or a
  system category as the rest), the rest as income with the balance unchanged, edits and deletes on both sides, the
  detail on both sides and unlinking, and the category fallback capped after links.
- Full backend suite as CI runs it: **2284 passed**. The run started before the last fix (refusing a system category such
  as Internal Movement as the rest's category); that fix's test file passed afterwards (9 passed).

## Web (tasks 2.1, 2.2)

- `ReimbursementLinksField` ("Mengganti transaksi"): search over the last 90 days, multi-select, remove, the rest choice
  with an income category (system and reimbursement categories left out). In the edit dialog and in Catat; the category
  link is hidden while expenses are chosen. The transaction dialog shows "Diganti Rp X" on an expense and "Mengganti"
  with the rest on a reimbursement, each row opening its transaction.
- Tests: edit dialog (link with the rest as income, search by text, unlink, no field on ordinary income, axe), Catat
  (create then link), transaction dialog (both sides, opening the linked row, axe). Frontend **1109 passed**; lint, type
  check and build pass.
- Browser: production build over the synthetic Markas seed migrated to V55, with a synthetic coffee repaid by a Rp 100.000
  reimbursement (the rest recorded as income) and an unlinked Rp 40.000 reimbursement: the edit dialog with the picker
  and the rest choice, the expense's dialog and the reimbursement's dialog at 1440 and 390, light and dark. 12 captures:
  0 console errors, 0 serious or critical axe violations, 0 horizontal overflow.

Owner's answer on 2026-11-13: "ok". Accepted.

## Not verified

- Daily series spread a linked amount over its category's spending days (decision 6), not only the expense's day.
- Production records are relinked after deployment, task 3.3, with the owner.
