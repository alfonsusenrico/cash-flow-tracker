## ADDED Requirements

### Requirement: Debt & Obligations Management
The system SHALL support creating, editing, querying, and deleting recurring obligations and debts:
1. Each obligation SHALL contain `name`, `total_amount`, `remaining_amount`, optional `due_date`, optional `minimum_payment`, and optional `notes`.
2. The system SHALL expose `GET /api/obligations`, `POST /api/obligations`, `PATCH /api/obligations/{id}`, and `DELETE /api/obligations/{id}`.

#### Scenario: Creating an obligation
- **WHEN** a user posts an obligation with total 12,000,000 IDR and remaining 10,000,000 IDR
- **THEN** the system persists the obligation and records the outstanding liability

### Requirement: Payoff Progress & Estimated Timeline
The system SHALL compute payoff progress and estimated payoff duration:
1. `payoff_percentage`: `min(100, round(((total_amount - remaining_amount) / total_amount) * 100))`.
2. `estimated_payoff_months`: `ceil(remaining_amount / max(1, minimum_payment))` when minimum payment is specified.

#### Scenario: Viewing debt payoff status
- **WHEN** a user views an obligation with 6,000,000 remaining out of 12,000,000 and minimum payment 1,000,000/month
- **THEN** the card renders 50% paid off and indicates approximately 6 months remaining to payoff

### Requirement: Linking Ledger Payments to Obligations
The transaction recording system SHALL support an optional `obligation_id`. Recording an expense with `obligation_id` SHALL decrement `obligations.remaining_amount`.

#### Scenario: Logging a debt payment
- **WHEN** a user records an expense of 1,000,000 IDR linked to an obligation
- **THEN** the transaction is persisted in the ledger and the obligation's `remaining_amount` decreases by 1,000,000
