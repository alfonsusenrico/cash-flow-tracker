# Proposal

## Why

A single real-world bank payment can settle several debts, but a ledger expense currently links to only one obligation. Recording several expenses for that one payment duplicates the cash outflow; assigning it to one debt leaves the other balances wrong.

## What Changes

- Allow one manually recorded expense to allocate its amount across one or more active debts, with a positive amount for each selected debt and a total that exactly equals the expense.
- Persist the per-debt allocations and apply all debt balance and archival changes atomically with the one ledger transaction.
- Show the allocation breakdown in Quick Capture and ledger detail/edit views; support changing or deleting the payment with complete reversal and reapplication.
- Default each newly selected debt to its outstanding balance and derive the transaction total from selected payments. Keep partial payments available through an explicit amount-edit action instead of requiring extra fields for a full payoff; preserve recorded amounts when reopening a transaction.
- Keep existing single-debt transactions and recurring debt rules compatible. Recurring rules continue to target one debt; multi-debt recurring schedules are outside this change.
- Reject duplicates, over-allocation, invalid debt ownership/status, mixed legacy and new linkage fields, and partial settlement without changing cash or debt balances.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `obligations-debt-tracker`: Add atomic allocation of one expense across multiple debts, with exact totals, payoff, edit, and reversal behavior.
- `transaction-ledger-management`: Show and edit a payment's per-debt breakdown while preserving one transaction and one cash outflow.

## Impact

- Backend transaction create, list/detail, update, and delete contracts; obligation balance updates and locking.
- A versioned allocation table migration in the existing database migration paths; existing transaction and debt rows remain valid.
- Quick Capture and ledger detail/edit forms, responsive presentation, query refresh, and regression tests.
- No new runtime dependency or production connection is required for implementation.
