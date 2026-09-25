# obligations-debt-tracker Specification

## Purpose
TBD - created by archiving change personal-finance-dashboard-os. Update Purpose after archive.

## Requirements

### Requirement: Debt & Obligations Management
The system SHALL support creating, editing, querying, and deleting recurring obligations and debts:
1. Each obligation SHALL contain `name`, `total_amount`, `remaining_amount`, optional `due_date`, optional `minimum_payment`, optional `notes`, and `is_archived` status.
2. The system SHALL expose `GET /api/obligations`, `POST /api/obligations`, `PATCH /api/obligations/{id}`, and `DELETE /api/obligations/{id}`.
3. Active debt queries (`GET /api/obligations` when `include_archived=false`) SHALL return only obligations where `is_archived = false` AND `remaining_amount > 0`.
4. Directly updating an obligation (`PATCH /api/obligations/{id}`) with `remaining_amount = 0` SHALL automatically set `is_archived = true`.
5. Directly updating an archived obligation with `remaining_amount > 0` SHALL set `is_archived = false`.

#### Scenario: Creating an obligation
- **WHEN** a user posts an obligation with total 12,000,000 IDR and remaining 10,000,000 IDR
- **THEN** the system persists the obligation and records the outstanding liability

#### Scenario: Querying active obligations excludes zero-balance debts
- **WHEN** a user queries `GET /api/obligations?include_archived=false`
- **THEN** any obligation with `remaining_amount <= 0` or `is_archived = true` is excluded from the returned list

#### Scenario: Direct update to zero remaining balance auto-archives
- **WHEN** a user updates an obligation with `remaining_amount: 0`
- **THEN** the system updates `remaining_amount` to 0 and automatically sets `is_archived` to `true`

### Requirement: Payoff Progress & Estimated Timeline
The system SHALL compute payoff progress and estimated payoff duration:
1. `payoff_percentage`: `min(100, round(((total_amount - remaining_amount) / total_amount) * 100))`.
2. `estimated_payoff_months`: `ceil(remaining_amount / max(1, minimum_payment))` when minimum payment is specified.

#### Scenario: Viewing debt payoff status
- **WHEN** a user views an obligation with 6,000,000 remaining out of 12,000,000 and minimum payment 1,000,000/month
- **THEN** the card renders 50% paid off and indicates approximately 6 months remaining to payoff

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

### Requirement: Automated Recurring Debt Amortization
The system SHALL support linking recurring payment rules directly to active debt obligations (`obligation_id`):
1. When a recurring rule linked to an obligation executes (whether automatically or via 1-tap confirmation), it SHALL record the expense and decrement `obligations.remaining_amount` by the payment amount.
2. If `remaining_amount` reaches 0, the obligation status SHALL indicate that it is fully paid off and the system SHALL automatically set `is_archived = true`.

#### Scenario: Recurring debt amortization execution
- **WHEN** a recurring rule of Rp 1.500.000 linked to an obligation with Rp 3.000.000 remaining executes
- **THEN** the system logs the transaction and updates the obligation's `remaining_amount` to Rp 1.500.000

#### Scenario: Recurring debt amortization payoff execution
- **WHEN** a recurring rule of Rp 1.500.000 linked to an obligation with Rp 1.500.000 remaining executes
- **THEN** the system logs the transaction, updates the obligation's `remaining_amount` to 0, and sets `is_archived` to `true`
