# idempotent-public-transactions Specification

## Purpose
Provides an idempotent, public-standard API contract for transaction creation by authenticated companion applications, external automations, and bots.

## Requirements

### Requirement: Idempotent Transaction Ingestion via Public API
The application SHALL provide an idempotent, public-standard `POST /api/transactions` endpoint allowing authenticated clients (companion apps, bots, scripts) to create transactions with optional idempotency enforcement.

#### Scenario: Required behavior and constraints
- **WHEN** an authenticated client creates or retries a transaction through the public API
- **THEN** the following required behavior and constraints hold:

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
The `POST /api/transactions` and `POST /api/movements` endpoints SHALL accept human-readable entity names in lieu of database UUIDs and SHALL resolve a name only to exactly one of the requesting owner's active entities, never guessing and never creating one.

#### Scenario: Required behavior and constraints
- **WHEN** a transaction or movement request identifies entities by their names
- **THEN** the following required behavior and constraints hold:

  The endpoints SHALL accept human-readable entity names in lieu of database UUIDs:
  1. `POST /api/transactions` SHALL accept `account_name` when `account_id` is omitted, `target_account_name` for an investment trade's position when `target_account_id` is omitted, and `category_name` when `category_id` is omitted. `POST /api/movements` SHALL accept `source_account_name` and `target_account_name` when the matching id is omitted. An id, when supplied, SHALL take precedence over a name.
  2. A name SHALL match an account or category only when it equals that entity's name after trimming, collapsing repeated whitespace and ignoring letter case. An account SHALL also match when the name equals its instrument symbol, with or without the exchange suffix (for example `BBRI` or `BBRI.JK`). Substring, synonym and similarity matches SHALL NOT resolve a name.
  3. Only the requesting owner's non-archived accounts and categories SHALL be considered. Another owner's entities and archived entities SHALL never match and SHALL never appear as candidates.
  4. When a parent account and a pocket both match, the single matching pocket SHALL be chosen. A matched parent with pockets SHALL still be routed to its default pocket.
  5. When a category name matches both an expense and an income category, the one whose kind equals the transaction type SHALL be chosen. A resolved category whose kind differs from the transaction type SHALL be rejected with HTTP 400, except for an investment trade, as for `category_id`.
  6. When no entity matches, or more than one remains, the server SHALL return HTTP 422 with `detail.code` `account_name_unresolved` or `category_name_unresolved`, `detail.field` naming the request field, `detail.reason` `not_found` or `ambiguous`, a message, and up to 5 `candidates` (id and name; for accounts also the parent name; for categories also the kind): the matching entities when ambiguous, or the owner's closest names when not found.
  7. Name resolution SHALL NOT create, rename or modify any account, pocket or category, and an unresolved request SHALL NOT create a transaction.

#### Scenario: Creating a transfer using account names
- **WHEN** a client posts to `POST /api/movements` with `source_account_name = "  bca   HARIAN "`, `target_account_name = "Dana Darurat"` and `amount = 500000`, and the owner has the account "BCA Harian" and the pocket "Dana Darurat"
- **THEN** the server records one internal movement whose expense leg is on "BCA Harian" and whose income leg is on "Dana Darurat"

#### Scenario: Missing target account on transfer
- **WHEN** a client posts to `POST /api/movements` without `target_account_id` or `target_account_name`
- **THEN** the server returns HTTP 422 and records nothing

#### Scenario: Pocket preferred over a parent with the same name
- **GIVEN** an owner with a top-level account "Tabungan" and a pocket "tabungan" under "Bank Jago"
- **WHEN** a client posts an expense with `account_name = "Tabungan"`
- **THEN** the expense SHALL be recorded on the pocket

#### Scenario: Ambiguous account name
- **GIVEN** an owner with the pockets "Cadangan" under "Bank Jago" and "CADANGAN" under "Blu Contoh"
- **WHEN** a client posts an expense with `account_name = "cadangan"`
- **THEN** the server SHALL return HTTP 422 with `code = "account_name_unresolved"`, `reason = "ambiguous"` and both pockets as candidates with their parent names
- **AND** no transaction, account or category SHALL be created

#### Scenario: Unknown account name returns candidates
- **GIVEN** an owner with the account "BCA Harian" and no account named "BCA"
- **WHEN** a client posts an expense with `account_name = "BCA"`
- **THEN** the server SHALL return HTTP 422 with `reason = "not_found"` and "BCA Harian" among the candidates
- **AND** no account or pocket SHALL be created

#### Scenario: Unknown category name is not created
- **WHEN** a client posts an expense with `category_name = "Kopi Sore"` and the owner has no such category
- **THEN** the server SHALL return HTTP 422 with `code = "category_name_unresolved"` and SHALL NOT create the category

#### Scenario: Another owner's account is never matched
- **GIVEN** another owner has an account "Rekening Rahasia" and the requesting owner has none
- **WHEN** the requesting owner posts an expense with `account_name = "Rekening Rahasia"`
- **THEN** the server SHALL return HTTP 422 with `reason = "not_found"` and the other owner's account SHALL NOT appear among the candidates

#### Scenario: Instrument symbol identifies a position
- **GIVEN** an owner with the investment position "BBRI Stock" whose instrument symbol is `BBRI.JK`
- **WHEN** a client posts a buy trade with `target_account_name = "BBRI"`
- **THEN** the trade SHALL be recorded against "BBRI Stock"
