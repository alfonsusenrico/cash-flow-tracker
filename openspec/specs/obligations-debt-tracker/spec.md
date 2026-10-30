# obligations-debt-tracker Specification

## Purpose
Manage debts and obligations, track payoff progress and link actual or recurring payments to their lifecycle.

## Requirements

### Requirement: Debt & Obligations Management
The system SHALL support creating, editing, querying, and deleting recurring obligations and debts.

#### Scenario: Required behavior and constraints
- **WHEN** an owner manages an obligation or changes its remaining balance
- **THEN** the following required behavior and constraints hold:

  The system SHALL support creating, editing, querying, and deleting recurring obligations and debts:
  1. Each obligation SHALL contain `name`, positive `total_amount`, `remaining_amount` from zero through `total_amount`, optional `due_date`, optional non-negative `minimum_payment`, optional `notes`, and `is_archived`.
  2. Create and update SHALL validate the final combined state, including when total and remaining amounts change together.
  3. The system SHALL expose `GET /api/obligations`, `POST /api/obligations`, `PATCH /api/obligations/{id}`, and `DELETE /api/obligations/{id}`.
  4. Active queries (`GET /api/obligations` with `include_archived=false`) SHALL return only obligations where `is_archived = false` and `remaining_amount > 0`.
  5. Setting remaining amount to zero SHALL archive the obligation; raising it above zero through an authorized reversal or edit SHALL reactivate it.

#### Scenario: Rejecting debt above its original total
- **WHEN** a user submits a final obligation state whose remaining amount exceeds its total amount
- **THEN** the system rejects the request and preserves the previous obligation

#### Scenario: Validating a combined update
- **WHEN** a user updates total and remaining amounts in one request and the resulting values satisfy the obligation constraints
- **THEN** the system persists both values and recalculates payoff metrics from the final state

#### Scenario: Creating an obligation
- **WHEN** a user submits an obligation with a 12,000,000 IDR total and 10,000,000 IDR remaining
- **THEN** the system persists the coherent outstanding liability

#### Scenario: Querying active obligations excludes zero-balance debts
- **WHEN** a user queries active obligations
- **THEN** obligations with zero remaining balance or archived status are excluded

#### Scenario: Direct update to zero remaining balance auto-archives
- **WHEN** a user updates an obligation's remaining amount to zero
- **THEN** the system persists zero and archives the obligation in the same operation

### Requirement: Payoff Progress & Estimated Timeline
The system SHALL compute payoff progress from the total and remaining debt and SHALL NOT display an estimated payoff duration that ignores interest or changes to payments.

#### Scenario: Viewing debt payoff status
- **WHEN** a user views an obligation with 6,000,000 IDR remaining out of 12,000,000 IDR
- **THEN** the card SHALL show 50% paid and SHALL NOT claim an estimated payoff time

### Requirement: Linking Ledger Payments to Obligations
The transaction recording system SHALL support an optional legacy `obligation_id` and an optional `obligation_allocations` list of distinct obligation IDs and positive integer amounts, and SHALL let the owner link an already recorded expense to a debt without creating another transaction.

#### Scenario: Required behavior and constraints
- **WHEN** an expense linked to one or more debts is created, retried, edited, deleted or linked after recording
- **THEN** the following required behavior and constraints hold:

  The transaction recording system SHALL support an optional legacy `obligation_id` and an optional `obligation_allocations` list of distinct obligation IDs and positive integer amounts. A manually recorded expense MAY use either field, but SHALL NOT use both. When allocations are supplied, their sum SHALL equal the expense amount, each debt SHALL be active and owned by the user at creation, and no allocation SHALL exceed that debt's outstanding amount. The system SHALL persist one ledger expense and debit its source account once. It SHALL apply all allocated debt reductions and related archival changes atomically with that expense. Existing single-debt payments and historical recurring-rule payments using `obligation_id` SHALL retain their current behavior.

  1. When an expense transaction payment reduces a debt's `remaining_amount` to 0 or less, the system SHALL set `remaining_amount = 0` and automatically set `is_archived = true`.
  2. When an existing payment transaction linked to a debt is modified or deleted such that the debt's `remaining_amount` becomes greater than 0, the system SHALL automatically set `is_archived = false`.
  3. Editing a payment's amount or allocations SHALL reverse its previous debt effects and apply the replacement effects in one atomic operation. A failed edit SHALL preserve the original transaction and all debt balances.
  4. Deleting an allocated payment SHALL reverse every allocation and restore affected debts, including reactivation when applicable.
  5. Retrying an idempotent creation request SHALL return the original transaction without applying cash or debt effects again.
  6. Linking an already recorded ordinary expense to a debt SHALL allocate its full amount to that debt once; repeating the link SHALL change nothing, and an expense already linked to another debt SHALL be refused.
  7. Suggested candidates SHALL never be linked without the owner choosing one; several same-amount candidates SHALL be presented for review.

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

#### Scenario: Linking a captured payment
- **WHEN** the owner links a captured 500,000 IDR expense to a debt with 500,000 IDR remaining, twice or from two requests at once
- **THEN** the debt SHALL reach 0 and be archived once, no transaction SHALL be added, and deleting the expense later SHALL restore and reactivate the debt

### Requirement: Coherent Obligation Form Validation
The obligation form SHALL collect total amount, remaining amount, optional minimum payment, due date, and notes with field-level validation against the final combined state. It SHALL prevent a negative remaining amount, a remaining amount above total, or an invalid date, and SHALL explain automatic archive or reactivation behavior before submission.

#### Scenario: Correcting an excessive remaining balance
- **WHEN** a user enters a remaining amount greater than the total obligation
- **THEN** submission is prevented, the remaining field receives an associated error, and entered values remain available
