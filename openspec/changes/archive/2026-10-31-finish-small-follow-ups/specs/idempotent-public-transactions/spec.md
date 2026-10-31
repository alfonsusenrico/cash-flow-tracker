## MODIFIED Requirements

### Requirement: Idempotent Transaction Ingestion via Public API
The application SHALL provide an idempotent, public-standard `POST /api/transactions` endpoint allowing authenticated clients (companion apps, bots, scripts) to create transactions with optional idempotency enforcement.

#### Scenario: Required behavior and constraints
- **WHEN** an authenticated client creates or retries a transaction through the public API
- **THEN** the following required behavior and constraints hold:

  The application SHALL provide an idempotent, public-standard `POST /api/transactions` endpoint allowing authenticated clients (companion apps, bots, scripts) to create transactions with optional idempotency enforcement:
  1. If an `idempotency_key` is supplied and matches an existing transaction of the same owner, the server SHALL answer HTTP 200 with that transaction's `transaction_id`, `amount`, `type` and `idempotent: true`, without creating a duplicate record.
  2. If `idempotency_key` is not supplied or matches no transaction of the owner, the server SHALL persist the new transaction and answer HTTP 200 with `ok: true` and the new `transaction_id`.
  3. The server SHALL reject payloads where `amount <= 0` with HTTP 422.
  4. The server SHALL reject payloads with an invalid `type` (must be `expense` or `income`; `transfer` is not a transaction type) with HTTP 422.
  5. An internal transfer SHALL be recorded through `POST /api/movements`. A request that is not an investment trade and gives `target_account_id` or `target_account_name` SHALL create no transaction; when the target is one of the owner's accounts, the server SHALL answer HTTP 400.

#### Scenario: Submitting duplicate transaction with same idempotency key
- **WHEN** client posts a transaction with `idempotency_key = "abc_123"` that was previously committed
- **THEN** server returns HTTP 200 with the previously created transaction and does not insert a new row in `transactions`

#### Scenario: Submitting invalid amount
- **WHEN** client posts a transaction with `amount = 0` or `amount = -5000`
- **THEN** server returns HTTP 422 Unprocessable Entity with validation details

#### Scenario: Creating a new transaction
- **WHEN** client posts a valid expense with an `idempotency_key` it has not used before
- **THEN** server returns HTTP 200 with `ok: true` and the new `transaction_id`, and exactly one row exists for that key

#### Scenario: Transfer type or target account
- **WHEN** client posts `type = "transfer"`, or an expense with `target_account_id` that is not an investment trade
- **THEN** server returns HTTP 422 for the type or HTTP 400 for the target account, and records nothing
