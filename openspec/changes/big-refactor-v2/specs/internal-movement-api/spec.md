# Spec: Internal Movement API

## ADDED Requirements

### Requirement: Internal Movement Endpoint
The system SHALL expose `POST /api/movements` accepting `{ source_account_id: UUID, target_account_id: UUID, amount: integer, notes?: string, date?: ISO8601 }` or account names. The endpoint SHALL atomically insert 2 transactions: first an expense row on source_account_id, second an income row on target_account_id, both with category "Internal Movement". The endpoint SHALL reject same source and target with 400.

#### Scenario: Create an internal movement
- **WHEN** `POST /api/movements` is called with source account A and target account B for amount 500000
- **THEN** 2 transactions are created: expense on account A and income on account B, both categorized as "Internal Movement"
- **AND** response returns `{ ok: true, expense_transaction_id: <uuid>, income_transaction_id: <uuid> }`

#### Scenario: Same source and target rejected
- **WHEN** `POST /api/movements` is called with identical source and target account IDs
- **THEN** response status code is 400 with an error detail message

### Requirement: Internal Movement Excluded from P&L
Any query computing living expenses or cycle income in `GET /api/pulse`, `GET /api/insights`, or dashboard aggregation SHALL exclude transactions whose category name is "Internal Movement".

#### Scenario: Internal movement excluded from pulse
- **WHEN** internal movement transactions exist
- **THEN** `GET /api/pulse` does not count them in today_spent or cycle_spent

## REMOVED Requirements

### Requirement: Transfer Transaction Type
The single-row `transfer` transaction type SHALL be completely removed. `type = 'transfer'` SHALL be rejected by transaction creation and update endpoints.

#### Scenario: Reject transfer type
- **WHEN** `POST /api/transactions` is called with `type = "transfer"`
- **THEN** the request is rejected with 422 Unprocessable Entity
