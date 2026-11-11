## Why

Credit purchases look exactly like cash spending in the ledger: both are red. An unpaid purchase is a promise to pay later, so the owner cannot see at a glance what is still owed. Payments already settle purchases automatically, oldest due first, but a payment row does not show what it paid, so the link between a bill payment and its purchases is invisible.

This is part 1 of the owner's credit idea of 2026-11-11. Part 2, capturing SPayLater purchases and payoffs from notifications, waits for real samples from the owner.

## Expected Outcome

- An unpaid credit purchase shows its amount in its own amber colour on every transaction row and in its dialog, with the minus sign and the existing "pay later" text.
- Once fully paid it shows in the ordinary expense red with the existing "paid {date}" text, and stays a credit purchase underneath.
- Opening a credit payment lists what that payment paid, per purchase, instalment and fee, and any part kept for later.

## What Changes

- A new `--credit-fg` token and `.c-credit` class, in both themes.
- `RowAmount` picks the credit colour for a credit purchase that is not fully settled.
- `credit_bills` computes each payment's share of the items, from the same order `settle` already uses.
- `GET /api/transactions/{id}` adds an additive `credit.settled_items` and `credit.kept_for_later` for a payment row.
- The transaction dialog shows the list, each row opening that purchase.
- `en` and `id` messages for the new labels.

## Non-goals

- No new transaction type and no change to `transactions.type`.
- No manual picking of purchases when paying (dropped by the owner, 2026-11-11).
- No notification capture, no virtual-account matching and no source-account learning. Those are part 2.
- No change to any total, balance, bill or settlement order. Spending still counts on the purchase day.
- The list is not added to refund rows.

## Capabilities

### Modified Capabilities

- `credit-accounts`: the unpaid purchase colour, and a payment's list of what it paid.

No project capability is added. The `money` standards apply: figures must not change, and this is verified by parity.

## Impact

- Backend: `services/credit_bills.py`, `routers/transactions.py`. Read-only and additive; no migration.
- Frontend: `tokens.css`, `globals.css`, `components/transactions/rows.tsx`, `TransactionDialog.tsx`, `types.ts`, `messages/{en,id}.json`.
- Verification: `scripts/verify/markas/transaksi-checklist.js` and `beranda-checklist.js` expect every spending amount in `--out-fg` and must accept the credit colour.
- Local verification only. Production follows the normal merge and deploy workflow.

## Approval

Pending the owner's review.
