## Why

A reimbursement (income in "Patungan & Ganti Uang") can only name the expense *category* it repays. That lowers the category's spending in the reimbursement's own period, spread over every expense of the category and its pillars. On 2026-11-13 the owner hit both limits:

- Friends repaid on 31 October for food bought on 28–29 October, in the previous cycle, so the repayment could not reduce that spending at all.
- The owner's mother paid Rp 100.000 back for a Rp 25.500 coffee filed under Belanja with the Tak Terduga pillar. The reduction was spread over all Belanja, so the coffee showed Rp 17.034, and Rp 74.500 of the money was really a gift.

The owner asked for reimbursements to link to the exact expenses they repay (2026-11-13, "ok then do it"), lean scope: no suggestions and no debt tracking. The lean scope and its defaults were approved the same day, with the rest of a reimbursement either not counted or recorded as income, chosen per reimbursement.

## Expected Outcome

- A reimbursement can link to one or more expenses. Its amount is applied to them oldest first, never more than an expense cost after earlier reimbursements.
- A linked expense's spending is lowered in its own period, category and pillar, whatever date the money came back. Pengeluaran, Kakeibo, Analisis, Laporan and the ledger summary follow.
- The rest of a reimbursement beyond its linked expenses is either not counted, or recorded as income in a category the owner picks, as a separate income row.
- The expense shows "Diganti Rp X" with what repaid it, and the reimbursement shows what it repaid.
- The category link keeps working where no expense is linked.

## What Changes

- Migration: `reimbursement_links` (reimbursement, expense, owner, allocated amount).
- `reimbursements` service: link allocation and reallocation; `load_spending_offsets` applies linked amounts to their expense's period, category and pillar before the category fallback, which is capped by what the linked part leaves.
- `PUT /api/transactions/{id}/reimburses` and `DELETE` to link and unlink; `GET /api/transactions/{id}` shows links on both sides; edits and deletes of either side reallocate.
- Web: a "Mengganti transaksi" picker and the rest choice in the reimbursement's edit form and in Catat; "Diganti Rp X" in the transaction dialog.

## Non-goals

- No suggestion of which expense a transfer repays; no list of who owes what.
- No manual split of an amount across expenses.
- No automatic conversion of existing category links.
- Daily series keep spreading a category's applied amount over its spending days in proportion (decision 6).

## Capabilities

- Modified: `reimbursement-offsets`.
- Touches the `money` capability's figures through the one shared offsets function. No new project capability.

## Impact

- `db/migrations/V55__reimbursement_links.sql`.
- `backend/app/services/reimbursements.py`, `backend/app/routers/transactions.py`, tests.
- `frontend/src/components/transactions/TransactionEditDialog.tsx`, `TransactionDialog.tsx`, `frontend/src/components/capture/ManualView.tsx`, a shared picker, messages, tests.
