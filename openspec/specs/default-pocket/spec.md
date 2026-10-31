# default-pocket Specification

## Purpose
Let a bank account name the child pocket that receives entries addressed to the parent, so parent-level notifications and captures land in a real pocket.

## Requirements

### Requirement: Default Pocket Column and Validation
The `accounts` table SHALL have a nullable column `default_pocket_id UUID REFERENCES accounts(id) ON DELETE SET NULL`. `default_pocket_id` SHALL only be set on parent accounts (those with child pockets). `POST /api/accounts` and `PATCH /api/accounts/{id}` SHALL accept optional `default_pocket_id`. If `default_pocket_id` is provided, the system SHALL validate it is a direct child of the account being updated. `GET /api/accounts` response SHALL include `default_pocket_id` for each account.

#### Scenario: Mobile notification routes to default pocket
- **GIVEN** a parent account "BCA" with the default pocket "BCA Harian"
- **WHEN** the companion posts a BCA notification to `POST /api/ingest/notifications` that names no pocket, and the server maps it to "BCA"
- **THEN** the transaction is recorded against "BCA Harian", not the parent
- **AND** a client that posts `account_name = "BCA"` to `POST /api/transactions` is routed to "BCA Harian" in the same way

#### Scenario: Explicit sub-pocket overrides default
- **GIVEN** a parent account "Bank Jago" with a default pocket and another pocket "Tabungan"
- **WHEN** the companion posts a Bank Jago notification that names the pocket "Tabungan", or a client posts `account_name = "Tabungan"`
- **THEN** the transaction is recorded against "Tabungan", not the default pocket

#### Scenario: Account without pockets unaffected
- **WHEN** the companion posts a GoPay notification and the owner's "GoPay" account has no pockets, or a client posts `account_name = "GoPay"`
- **THEN** the transaction is recorded against "GoPay" itself
