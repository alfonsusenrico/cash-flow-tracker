## ADDED Requirements

### Requirement: Idempotent Transaction Ingestion via Public API
The application SHALL provide an idempotent, public-standard `POST /api/transactions` endpoint allowing authenticated clients (companion apps, bots, scripts) to create transactions with optional idempotency enforcement:
1. If an `idempotency_key` is supplied and matches an existing transaction for the user, the server SHALL return the existing transaction record with HTTP 200 without creating a duplicate record.
2. If `idempotency_key` is not supplied or does not exist, the server SHALL persist the new transaction and return HTTP 201.
3. The server SHALL reject payloads where `amount <= 0` with HTTP 422.
4. The server SHALL reject payloads with an invalid `type` (must be `expense`, `income`, or `transfer`) with HTTP 422.

#### Scenario: Submitting duplicate transaction with same idempotency key
- **WHEN** client posts a transaction with `idempotency_key = "abc_123"` that was previously committed
- **THEN** server returns HTTP 200 with the previously created transaction and does not insert a new row in `transactions`

#### Scenario: Submitting invalid amount
- **WHEN** client posts a transaction with `amount = 0` or `amount = -5000`
- **THEN** server returns HTTP 422 Unprocessable Entity with validation details

### Requirement: Name-Based Entity Resolution on Transaction Creation
The `POST /api/transactions` endpoint SHALL accept human-readable entity names in lieu of database UUIDs:
1. If `account_id` is omitted, the client SHALL supply `account_name`. The server SHALL resolve the account name against user accounts using case-insensitive canonical matching, pocket synonym mapping, or auto-provisioning under a matched parent bank.
2. If `account_name` cannot be resolved to any account, the server SHALL return HTTP 400 Bad Request indicating the account could not be resolved.
3. If `type == 'transfer'`, the client SHALL supply either `transfer_target_account_id` or `target_account_name`. If omitted, server returns HTTP 400.
4. If `category_name` is supplied, the server SHALL resolve it to the user's category ID. If `category_name` is `"Internal Movement"` or `"Investasi"` and no such category exists in the database, the server SHALL auto-seed it immediately.

#### Scenario: Creating a transfer using account names
- **WHEN** client posts `type = "transfer"`, `account_name = "My Emergency Fund"`, `target_account_name = "Bank Jago"`, `category_name = "Internal Movement"`, `amount = 500000`
- **THEN** server resolves the source account to the user's emergency fund pocket, destination to Bank Jago, links the "Internal Movement" category, and returns HTTP 201

#### Scenario: Missing target account on transfer
- **WHEN** client posts `type = "transfer"` without `transfer_target_account_id` or `target_account_name`
- **THEN** server returns HTTP 400 Bad Request with `"Transfer requires a target account"`
