## Context

`credit_bills.build_schedule` computes items and credits at read time. `settle` sorts items by due date, then purchase date, and credits by date, then walks cumulative totals. Rows already carry `credit.role`, `credit.settled_on` and `credit.left_to_pay`. Every transaction row amount goes through `RowAmount` in `components/transactions/rows.tsx`: Transactions, Home, the dialog, the account detail and the calendar.

## Goals / Non-Goals

Goals: make "still owed" visible on the row, and make a payment explain itself. Non-goals are listed in the proposal; in particular no figure may change.

## Decisions

**D1 Display state, not a transaction type.** The colour is derived from the existing `credit` block. Rejected: a new `transactions.type = 'credit'`, because every sum, filter, export and report branches on `type` and the owner's Spending rule would need re-proving everywhere.

**D2 Unpaid means not fully settled.** A purchase is amber while `credit.role === "purchase"` and `credit.settled_on` is null. A purchase with some instalments paid stays amber. A purchase on an account with no payment day has no schedule, so `settled_on` is null and it stays amber. Rejected: amber only for overdue, which is what the warning colour already says.

**D3 Own token with a measured value.** Owner condition, 2026-11-11: enough contrast and not a shade close to red. Reusing `--warn-fg` was rejected: in light mode `#8F4F04` is a brown close to the dark red, and it would make unpaid credit read as "Overdue".

| Theme | `--credit-fg` | Contrast on canvas / surface / surface-2 | OKLab distance from `--out-fg` |
| --- | --- | --- | --- |
| Light | `#8A5A06` | 5.53 / 5.16 / 4.62 | 15.0 |
| Dark | `#FBBF24` | 10.92 / 10.10 / 8.94 | 22.6 |

`#92600A` was rejected after measuring: 4.20 on surface-2, below 4.5. Light mode cannot go much brighter and keep AA. Meaning does not depend on colour: the row keeps "pay later" or "paid {date}". The owner confirms the colour on the real page before the rest is built (task 1.3).

**D4 Paid returns to the expense red.** After settlement the row is ordinary spending in appearance, and the credit block, its filter and its settlement facts remain. Rejected: a third "paid credit" colour, which adds a colour to learn and no decision to make.

**D5 (default) Colour applies to rows and the dialog only.** Totals, the Kredit header, Catat's preview and charts keep their colours. Catat's preview already says "pay later" and is not a ledger row yet.

**D6 Compute a payment's share; do not store it.** Credits and items are both cumulative ranges in `settle`'s order, so a credit's share of an item is the overlap of the two ranges. Rejected: a stored payment-to-purchase table, which goes stale when a row is edited, deleted or backdated and would need a migration and rewriting logic.

Consequence, accepted: recording a backdated purchase can change what an earlier payment is shown to have paid. That matches how bills already recompute.

**D7 Identify a payment by its movement.** The dialog may be opened from either leg of the movement, while `Credit.transaction_id` is the leg on the credit account. `Credit` gains `movement_id`, already in `ROWS_SQL`, and the lookup uses it.

**D8 Detail endpoint only.** `settled_items` is added only under `with_installments=True`, as instalments are, so the list endpoint is not slowed. `load_credit_blocks` loads schedules for payment rows only in that mode.

**D9 (default) Shape of the list.** Each entry: `kind`, `transaction_id`, `title`, `installment`, `amount` (the share paid by this payment), `item_amount` and `due_date`. A partly covered item shows "Rp 100.000 of Rp 300.000". `kept_for_later` is the part of the payment that covers nothing yet. Opening debt has no `transaction_id` and is shown as a plain row labelled as opening debt.

**D10 (default) Rows open the purchase.** A listed row is a whole-row button that switches the same dialog to that purchase, through the dialog's existing `shownId`.

## API

`GET /api/transactions/{id}`, for `credit.role === "payment"` only, adds:

```json
{ "settled_items": [{ "kind": "purchase", "transaction_id": "…", "title": "Kopi Senja",
    "installment": { "number": 3, "count": 6 }, "amount": 100000, "item_amount": 100000, "due_date": "2026-12-11" }],
  "kept_for_later": 0 }
```

Additive and read-only. The sum of `amount` plus `kept_for_later` equals the payment amount, or the code raises `CreditInvariantError` as `build_schedule` does.

## Failure behaviour

A credit account with no payment day has no schedule. Its payment returns an empty list and `due_dates_unavailable: true`, and the dialog shows nothing extra. A deleted purchase simply leaves the computed list.

## Migration and rollback

None. Reverting the commit removes the fields and the colour.

## Risks

- Checklists asserting every spending amount is `--out-fg` will fail until updated (task 3.2).
- Light-mode amber and red stay fairly near each other because of the contrast limit, hence the owner check in task 1.3.

## Open Questions

None.
