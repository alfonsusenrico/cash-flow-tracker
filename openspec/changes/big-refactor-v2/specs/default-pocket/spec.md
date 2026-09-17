# Spec: Default Pocket

## ADDED Requirements

### Requirement: Default Pocket Column and Validation
The `accounts` table SHALL have a nullable column `default_pocket_id UUID REFERENCES accounts(id) ON DELETE SET NULL`. `default_pocket_id` SHALL only be set on parent accounts (those with child pockets). `POST /api/accounts` and `PATCH /api/accounts/{id}` SHALL accept optional `default_pocket_id`. If `default_pocket_id` is provided, the system SHALL validate it is a direct child of the account being updated. `GET /api/accounts` response SHALL include `default_pocket_id` for each account.

#### Scenario: Mobile notification routes to default pocket
- **WHEN** mobile sends `POST /api/transactions` with `account_name = "BCA"` without a sub-pocket
- **THEN** the transaction is recorded against the default pocket account, not the parent

#### Scenario: Explicit sub-pocket overrides default
- **WHEN** mobile sends `account_name = "BCA Giro"`
- **THEN** the transaction is recorded against "BCA Giro", not the default pocket

#### Scenario: Account without pockets unaffected
- **WHEN** mobile sends `account_name = "GoPay"`
- **THEN** the transaction is recorded against "GoPay" itself
