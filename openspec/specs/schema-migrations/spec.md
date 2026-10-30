# schema-migrations Specification

## Purpose
Keep one owner of the database schema, the versioned Flyway migrations, so every environment gets the same schema before the API starts and no schema or data change runs outside a reviewed migration.

## Requirements

### Requirement: Versioned Migrations Own the Schema
The database schema SHALL be created and changed only by the versioned Flyway migrations in `db/migrations`. The API SHALL NOT run DDL or data repair at startup, and the release SHALL run the migrations to completion before it starts or replaces the API.

#### Scenario: API starts on a migrated database
- **GIVEN** a database migrated from V1 to the latest version
- **WHEN** the API starts
- **THEN** it SHALL serve health and authenticated requests without creating or altering any table, column, index or constraint.

#### Scenario: Migration failure stops the release
- **WHEN** a migration fails during a release
- **THEN** the release SHALL stop before replacing the running API.

### Requirement: Startup-Only Schema Adopted by a Migration
A migration SHALL create every schema element that only the retired startup block created: the nullable `accounts.default_funding_account_id` reference and the unique index on `transactions (user_id, idempotency_key)` for non-null keys, and SHALL archive debts whose remaining amount is zero or less. It SHALL be a no-op where they already exist.

#### Scenario: Fresh database
- **WHEN** an empty database is migrated from V1 to the latest version
- **THEN** its schema SHALL match the previous migrations plus startup block, except for retired tables
- **AND** a second transaction with the same owner and idempotency key SHALL be rejected.

#### Scenario: Paid debt left active
- **GIVEN** an active debt with remaining amount 0
- **WHEN** the migration runs
- **THEN** the debt SHALL be archived and debts with a positive remaining amount SHALL stay active.

### Requirement: Merchant Category Rules Retired Without Data Loss
The unused `merchant_category_rules` table SHALL be dropped only when it holds no rows; when it holds rows the migration SHALL fail with an error naming the table and row count and SHALL change nothing. Notification context and the deterministic interpretation SHALL NOT read merchant category rules; merchant memory is the only learned classification source.

#### Scenario: Empty table
- **WHEN** the migration runs and `merchant_category_rules` is empty
- **THEN** the table SHALL no longer exist.

#### Scenario: Table holding rows
- **GIVEN** `merchant_category_rules` holds one row
- **WHEN** the migration runs
- **THEN** it SHALL fail with an error naming `merchant_category_rules` and 1 row
- **AND** the table and its row SHALL remain.

### Requirement: Guarded Applied-Migration Checksum Repair
The approved release path SHALL repair an intentionally changed applied migration checksum only when Flyway validation identifies the V14 checksum mismatch and no other unexpected issue.

#### Scenario: Required behavior and constraints
- **WHEN** the approved release preflight encounters an applied migration checksum mismatch
- **THEN** the following required behavior and constraints hold:

  The approved release path SHALL repair an intentionally changed applied migration checksum only when Flyway validation identifies the V14 checksum mismatch and no other unexpected issue. Validation entries for not-yet-applied versioned migrations MAY coexist with that mismatch only when each version and description exactly matches a `Pending` entry in Flyway history. History SHALL contain no failed, missing, deleted, or out-of-order migrations. The repair SHALL align V14 only, SHALL be checked afterward for no remaining unexpected validation issue, and SHALL NOT run during ordinary application startup or when V14 needs no repair.

#### Scenario: Repairing the expected V14 checksum only
- **WHEN** an existing database has successfully applied V14 and validation reports its checksum mismatch, with no other issue except versioned migrations independently confirmed as pending in Flyway history
- **THEN** the release preflight repairs the history checksum for V14
- **AND** confirms the repair changed no other history entry and leaves no unexpected validation issue before normal migration proceeds

#### Scenario: Accepting only verified pending versions
- **WHEN** validation reports a not-yet-applied migration whose version or description does not match a `Pending` Flyway history entry
- **THEN** the release preflight stops without repairing V14 or starting normal migration

#### Scenario: Refusing to repair unrelated migration history
- **WHEN** validation reports another mismatch or history includes a failed, missing, deleted, or out-of-order migration
- **THEN** the release preflight stops without running Flyway repair or normal migration
