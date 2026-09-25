# Spec Delta

## MODIFIED Requirements

### Requirement: Linking Ledger Payments to Obligations
The transaction recording system SHALL support an optional legacy `obligation_id` and an optional `obligation_allocations` list of distinct obligation IDs and positive integer amounts. A manually recorded expense MAY use either field, but SHALL NOT use both. When allocations are supplied, their sum SHALL equal the expense amount, each debt SHALL be active and owned by the user at creation, and no allocation SHALL exceed that debt's outstanding amount. The system SHALL persist one ledger expense and debit its source account once. It SHALL apply all allocated debt reductions and related archival changes atomically with that expense. Existing single-debt payments and recurring-rule payments using `obligation_id` SHALL retain their current behavior.

1. When an expense transaction payment reduces a debt's `remaining_amount` to 0 or less, the system SHALL set `remaining_amount = 0` and automatically set `is_archived = true`.
2. When an existing payment transaction linked to a debt is modified or deleted such that the debt's `remaining_amount` becomes greater than 0, the system SHALL automatically set `is_archived = false`.
3. Editing a payment's amount or allocations SHALL reverse its previous debt effects and apply the replacement effects in one atomic operation. A failed edit SHALL preserve the original transaction and all debt balances.
4. Deleting an allocated payment SHALL reverse every allocation and restore affected debts, including reactivation when applicable.
5. Retrying an idempotent creation request SHALL return the original transaction without applying cash or debt effects again.

#### Scenario: Logging a debt payment
- **WHEN** a user records an expense of 1,000,000 IDR linked to an obligation
- **THEN** the transaction is persisted in the ledger and the obligation's `remaining_amount` decreases by 1,000,000

#### Scenario: Logging a debt payment that pays off the debt
- **WHEN** a user records an expense of 1,000,000 IDR linked to an obligation with 1,000,000 IDR remaining
- **THEN** the transaction is persisted in the ledger, the obligation's `remaining_amount` decreases to 0, and `is_archived` is set to `true`

#### Scenario: Deleting or reversing a payoff transaction reactivates the debt
- **WHEN** a user deletes a payment transaction that previously paid off an obligation, returning its balance to 1,000,000 IDR
- **THEN** the obligation's `remaining_amount` increases by 1,000,000 IDR and `is_archived` is set to `false`

#### Scenario: Splitting one payment across two debts
- **WHEN** a user records one 1,500,000 IDR expense with allocations of 1,000,000 IDR to debt A and 500,000 IDR to debt B
- **THEN** the ledger contains one 1,500,000 IDR cash outflow, debt A decreases by 1,000,000 IDR, and debt B decreases by 500,000 IDR

#### Scenario: Rejecting an invalid split without partial effects
- **WHEN** allocations contain a duplicate debt, exceed a debt's outstanding amount, do not sum to the expense, or refer to another user's or archived debt
- **THEN** the entire request is rejected and no transaction, cash debit, or debt adjustment is persisted

#### Scenario: Editing a split after one debt was paid off
- **WHEN** a user replaces a saved split payment with a valid new allocation that reduces the amount assigned to a previously paid-off debt
- **THEN** the old allocations are reversed, new allocations are applied once, and that debt becomes active again if it has an outstanding balance

#### Scenario: Deleting a split payment
- **WHEN** a user deletes an expense previously allocated across several debts
- **THEN** the transaction is removed and every allocated amount is restored to its debt in the same operation

#### Scenario: Retrying a split payment
- **WHEN** the same idempotency key is submitted again after an allocated payment succeeds
- **THEN** the original transaction is returned and no cash or debt balance changes again
