# Design

## Context

See proposal.md for motivation. Main at `08b0bbd` has the amount top-up lifecycle: `investment_topups` journal rows whose id is the movement id, `create_topup`/`mutate_topup` with account locks in stable order, lazy `initialize_amount_tracking`, and `apply_contribution_delta` adjusting the aggregate value (`last_price`) and `investment_cost_basis`. Amount-mode products are excluded from price sync and unit trades. A unit-tracked product stores a per-unit `last_price`, so the lazy initializer cannot be reused for it. Production read on 2026-10-01: the two Bibit products have units, no symbol, and no ledger transactions; the two Rp112.590 RDN debits are standalone expenses linked to notification events.

## Goals / Non-Goals

Goals: reuse the existing top-up journal and lifecycle; keep both operations atomic and retry-safe; never change the funding balance when converting a recorded debit.

Non-Goals: converting amount products back to units, converting stock or gold, matching notifications to products, or repairing the existing trade-deletion unit drift.

## Decisions

### 1. Product conversion endpoint
`POST /api/accounts/{id}/amount-tracking` (no body). Under the account lock, require owned, active, `instrument_type = 'mutual_fund'`, no children, `units IS NOT NULL`, mode not `amount`. Compute value = round(units × last_price) when a price exists, else the locked ledger balance; basis per the spec precedence. Write `investment_tracking_mode = 'amount'`, `investment_cost_basis`, `last_price = value`, `units = NULL`, `avg_buy_price = NULL`, `investment_value_estimated = FALSE`, and keep `last_price_at` because the value still reflects the last real valuation. Leave `initial_balance` unchanged, as the top-up design requires. Errors: 404 unknown, 409 `already_amount_tracked`, 422 `unit_mutual_fund_required`.
Alternative rejected: accepting overrides in the same call. Update Nilai already sets an absolute value and cost after conversion, so one path owns corrections.

### 2. Expense conversion endpoint
`POST /api/investment-topups/from-transaction` with `transaction_id` and `target_account_id`. Take a transaction-scoped advisory lock on the expense id, lock the expense row, then lock source and target accounts in stable order. Require an owned expense with no movement, obligation, goal, recurring rule, or debt allocation, and validate accounts with the existing top-up rules. If the expense already belongs to an active top-up for the same product, return it as idempotent; otherwise refuse with 409 `transaction_not_convertible`.
Insert the journal row with key `transaction:<expense id>:<topup id>` (unique per attempt, so a later reconversion after deletion is possible) and `converted_from = {transaction_id, category_id, kakeibo_type, notes}`. Update the expense into the outbound leg (movement id/role, internal-movement expense category, the same kakeibo rule as other top-ups). Notes become "Top up Investasi" when the expense came from a notification, matching the merge endpoint's rule; otherwise the owner's notes stay. Insert the inbound leg and apply the contribution delta. No funds check: the debit already happened.
Alternative rejected: deleting the expense and calling `create_topup`. It would break the notification link and the notification idempotency key, so a later retry could record the debit again.

### 3. Deletion of converted top-ups
`mutate_topup(delete=True)` checks `converted_from`. When set, it reverses the delta, deletes only the inbound leg, restores the outbound leg to a standalone expense with the saved category, kakeibo, and notes, and tombstones the journal row. Amount and date keep their corrected values.

### 4. Schema
Flyway `V24__investment_topup_conversion.sql` adds nullable `investment_topups.converted_from JSONB`. The same statement lives in `backend/app/db/investment_topup_conversion.sql`, executed by `init_db` after `investment_topups.sql`, following the existing mirror pattern.

### 5. Web
Add `isConvertibleUnitFund` and `canConvertToTopup` helpers next to `listAmountInvestmentProducts`. The accounts options menu shows "Ubah ke pelacakan nominal" for eligible products and opens a confirmation stating the computed value and cost (from the account response: balance and cost basis) and that it cannot be undone. The ledger edit modal shows "Jadikan Top up Investasi" for eligible expenses. It opens a modal with a product select from `listAmountInvestmentProducts`, or a message pointing to Rekening when none exists. Both invalidate accounts, transactions, dashboard, and insights queries. Reuse the Modal focus handling and the existing alert pattern.

## Risks / Trade-offs

- [Conversion is one-way] → The confirmation says so. Units can't be rebuilt reliably from amounts, and the owner tracks money-market funds in rupiah.
- [A converted product with old trade history] → Old trade legs stay in the ledger. Value no longer derives from them, which matches amount mode. Their edit and delete paths already ignore units.
- [Notification retry after conversion] → The expense keeps its notification idempotency key and event link, so a retry finds the existing transaction instead of creating a new one.

## Migration Plan

Additive V24 deploys through the normal pipeline before the web uses the new endpoints. Rollback: revert the merge. The nullable column stays harmless. Converted products stay in amount mode, which the previous release already supports.
