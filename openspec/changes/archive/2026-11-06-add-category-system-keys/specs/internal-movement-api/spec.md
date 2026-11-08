## MODIFIED Requirements

### Requirement: Internal Movement Endpoint
The system SHALL expose `POST /api/movements` accepting `{ source_account_id: UUID, target_account_id: UUID, amount: integer, notes?: string, date?: ISO8601 }` or account names. The endpoint SHALL atomically insert 2 transactions: first an expense row on source_account_id, second an income row on target_account_id, both in the owner's movement system category (key `internal_movement`) of the matching kind, whatever its name. The endpoint SHALL reject same source and target with 400.

#### Scenario: Create an internal movement
- **WHEN** `POST /api/movements` is called with source account A and target account B for amount 500000
- **THEN** 2 transactions are created: expense on account A and income on account B, both categorized in the movement system category ("Internal Movement" for a seeded owner)
- **AND** response returns `{ ok: true, expense_transaction_id: <uuid>, income_transaction_id: <uuid> }`

#### Scenario: Renamed movement category
- **GIVEN** the owner renamed both movement categories to "Transfer"
- **WHEN** `POST /api/movements` is called for amount 500000
- **THEN** both legs SHALL be filed in "Transfer", and no new "Internal Movement" category SHALL be created

#### Scenario: Same source and target rejected
- **WHEN** `POST /api/movements` is called with identical source and target account IDs
- **THEN** response status code is 400 with an error detail message

### Requirement: Internal Movement Excluded from P&L
Any query computing living expenses or cycle income in `GET /api/pulse`, `GET /api/insights`, or dashboard aggregation SHALL exclude transactions whose category carries the system key `internal_movement`, whatever the category is named.

#### Scenario: Internal movement excluded from pulse
- **WHEN** internal movement transactions exist
- **THEN** `GET /api/pulse` does not count them in today_spent or cycle_spent

#### Scenario: Excluded after a rename
- **GIVEN** the movement categories are renamed "Transfer"
- **WHEN** `GET /api/pulse` and the dashboard overview are requested
- **THEN** movement legs SHALL still count in neither spending nor income
