# Design

## Context

See `proposal.md` for the motivation and the two delta specs for observable behavior. `transactions.obligation_id` currently links one obligation and the transaction create/update/delete routes adjust its balance directly. Quick Capture and ledger edit each expose one debt selector. Recurring rules also use the single ID. The financial integrity change already adds atomic ledger mutation and balance guards; this change builds on its completed contracts without replacing the recurring model.

## Goals / Non-Goals

**Goals:**

- Keep one cash ledger row and one account debit while applying a separately auditable amount to each selected debt.
- Make create, edit, delete, idempotent retry, and failure recovery obey the same allocation invariant.
- Preserve existing single-debt records and clients without a data rewrite.

**Non-Goals:**

- Split one bank transfer among multiple source accounts.
- Automatically distribute a fixed transaction amount proportionally across debts or support a payment amount not assigned to a debt.
- Add multi-debt recurring rules or alter existing settled bank notifications.

## Decisions

### 1. Add an allocation table and keep the legacy single link

Create a versioned `transaction_obligation_allocations` table with `transaction_id`, `obligation_id`, and positive integer `amount`; the transaction/debt pair is unique. A foreign key to the transaction cascades on transaction deletion. The obligation relationship restricts hard deletion while allocation history exists, so a payment remains reversible and explainable. Existing `transactions.obligation_id` stays for legacy and recurring clients. New multi-debt transactions use allocation rows and a null legacy ID. No backfill is needed; response serialization produces a common allocation breakdown for either representation.

Alternatives considered: storing IDs and amounts in JSON would weaken relational constraints and debt lookup; creating one expense per debt would duplicate the cash outflow; migrating every old payment would add risk without a behavior benefit.

### 2. Add an explicit, exclusive request field

Add `obligation_allocations: [{obligation_id, amount}]` to transaction create and patch models. Reject requests that supply this field together with a non-null legacy `obligation_id`, or supply it for non-expenses or investment trades. On creation, omit the field to preserve existing behavior. On PATCH, omission preserves the current linkage; an explicit empty list clears allocations. Changing a multi-debt payment amount without a corresponding valid allocation update is rejected. A single-item list remains valid, while old clients can continue using `obligation_id`.

Validate distinct IDs, positive integer amounts, exact sum, user ownership, active state for newly added debts, and per-debt outstanding capacity. Existing linked debts may be archived by the original payment; edit must restore the old effects before validating their replacement amounts. Return structured errors for sum mismatch, duplicate debt, unavailable debt, and allocation above outstanding so the forms can focus the relevant control.

### 3. Apply reversals and replacements in one database transaction

Create and update lock the transaction (for edit), source account, and all referenced obligations in a stable ID order. The route validates the final state, inserts or updates the one expense and allocation rows, then changes each obligation's remaining amount and archived state before one commit. On edit, reverse old debt effects, validate against restored balances, then apply the replacement. Delete reverses every allocation before deleting the transaction. An exception rolls back all effects. Idempotency lookup must precede any new allocation writes.

The existing single-debt code path continues to serve legacy and recurring payments; shared allocation helpers may consolidate the balance update logic if they preserve current behavior. Hard deletion of an obligation with allocation history returns an actionable conflict, rather than erasing the relationship. Normal archive remains available.

### 4. Expose the breakdown without multiplying ledger rows

List/detail responses include `obligation_allocations` with IDs, names, and amounts. For legacy rows, serialize a one-item breakdown from `obligation_id`; for unlinked rows, return an empty list. Fetch allocation metadata separately for the bounded page of transaction IDs or use a non-fanout aggregate, so pagination counts and cash totals remain one row per transaction. Filtering by obligation includes both the legacy ID and the new allocation relation. The ledger labels an allocated payment with its debt count and opens the full breakdown in detail/edit.

### 5. Use one allocation editor in capture and ledger edit

Quick Capture and ledger edit reuse a focused debt allocation control. Selecting a new debt defaults its payment to the debt's currently outstanding amount (including the original allocation when editing that debt), and the transaction total follows the sum of selected payments. With linked debts, the total is read-only: the debt rows are the source of truth, avoiding competing amount fields and silent mismatch. Each row shows its payment as text by default; an explicit `Ubah jumlah` action reveals editable per-debt amounts for partial payments. Editing those amounts updates the total immediately. An unselected row never contributes a payment, and submission still requires a positive amount for every selected debt and an exact total.

For one debt, submit the legacy field using the derived total. Reopening a saved payment loads its recorded amounts rather than replacing them with today's balances; newly added debts still default to their current outstanding amount. The control keeps saved archived debts visible when editing, but offers only active eligible debts for new additions. It preserves entered values after errors, announces validation errors, and uses the existing responsive dialog/bottom-sheet component. If the outstanding balance changes between selection and submission, the backend's capacity check remains authoritative and rejects the stale payment without cash or debt changes.

## Risks / Trade-offs

- **[An edit partially changes balances]** → Lock affected records and commit only after the final allocation and account state pass validation; test rollback on a forced late failure.
- **[Concurrent payments overpay the same debt]** → Lock debt rows before checking remaining amounts and reject the later request cleanly.
- **[Legacy and new links double-apply a payment]** → Enforce exclusivity in request validation and persisted write flow; test legacy create/update/delete unchanged.
- **[Historical debt is hard-deleted]** → Restrict hard deletion while allocated transactions exist and return a conflict; ordinary archive remains available.
- **[Ledger joins duplicate rows]** → Keep allocation loading bounded to selected transaction IDs and check pagination/count totals.

## Migration Plan

1. Add the new table in the next Flyway migration and the repository's idempotent startup schema path, without rewriting existing transactions.
2. Deploy the additive schema before code that writes allocation rows; old application code continues to work with the new table present.
3. Enable the API contract, response serialization, and forms together; retain legacy `obligation_id` for old clients and recurring rules.
4. Roll back application code if needed while leaving the additive table in place. Payments already using allocation rows require the new code for editing/reversal, so a rollback after use must gate those operations rather than silently drop allocation effects.
