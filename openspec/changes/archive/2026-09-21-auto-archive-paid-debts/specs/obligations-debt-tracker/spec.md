# Spec Delta: Auto-Archive Paid-Off Debts and Obligations

## MODIFIED Requirements

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

### Requirement: Linking Ledger Payments to Obligations
The transaction recording system SHALL support an optional `obligation_id`. Recording an expense with `obligation_id` SHALL decrement `obligations.remaining_amount`:
1. When an expense transaction payment reduces `remaining_amount` to 0 or less, the system SHALL set `remaining_amount = 0` and automatically set `is_archived = true`.
2. When an existing payment transaction linked to an obligation is modified or deleted such that the obligation's `remaining_amount` becomes greater than 0, the system SHALL automatically set `is_archived = false`.

#### Scenario: Logging a debt payment
- **WHEN** a user records an expense of 1,000,000 IDR linked to an obligation
- **THEN** the transaction is persisted in the ledger and the obligation's `remaining_amount` decreases by 1,000,000

#### Scenario: Logging a debt payment that pays off the debt
- **WHEN** a user records an expense of 1,000,000 IDR linked to an obligation with 1,000,000 IDR remaining
- **THEN** the transaction is persisted in the ledger, the obligation's `remaining_amount` decreases to 0, and `is_archived` is set to `true`

#### Scenario: Deleting or reversing a payoff transaction reactivates the debt
- **WHEN** a user deletes a payment transaction that previously paid off an obligation, returning its balance to 1,000,000 IDR
- **THEN** the obligation's `remaining_amount` increases by 1,000,000 IDR and `is_archived` is set to `false`

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
