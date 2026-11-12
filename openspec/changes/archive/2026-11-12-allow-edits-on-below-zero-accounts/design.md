## Context

`update_transaction` locks the account, computes the ledger balance, adds the old amount back when the row was an expense on the same account, and refuses when that is less than the new amount. With the row's own debit added back, an account that was at Rp 0 before a settled notification took it to −189.667 reports Rp 0 available for that same Rp 189.667 row, so any save fails. `update_movement` calls `ensure_sufficient_funds(required=new amount, exclude_movement_id=...)` with the same result. `mutate_topup` already checks only the positive difference.

Reproduced locally on `origin/main` `4597f91`: the regression test gets HTTP 409 with the owner's exact message.

## Goals / Non-goals

Goals: an edit never fails the funds check unless it takes more money from a liquid account. Non-goals: any change to creation paths, the notification exception, or messages.

## Decisions

1. **Check the extra outflow, not the whole amount.** The extra is `new amount − old amount` when the old row was an expense (movement: outbound leg) on the same account, otherwise the whole new amount. The check runs only when the extra is positive, and then keeps today's test (old debit added back, compared with the new amount), so the message and its numbers stay the same. Rejected: skipping the check when only non-money fields change. A lower amount on a below-zero account would still fail for no reason.
2. **Keep the existing message.** When a raise is refused the owner sees the amount available for this row and the new amount, as today. Rejected: reporting only the extra (the top-up style). The text would change for a case that already works.
3. **Fix both edit endpoints in place.** Each one already locks its rows and has the old values in hand. Rejected: a shared helper. Two call sites with different inputs don't need one yet.

## Failure behaviour

Unchanged: a refused edit returns 409 `insufficient_funds` and writes nothing. Accounts that are not liquid, investment positions and credit accounts are not checked, as before.

## Risks

An edit can keep an account below zero, but it cannot push it further down, and that state already exists and is already flagged for reconciliation.

## Rollback

Revert the commit. No data or schema change.
