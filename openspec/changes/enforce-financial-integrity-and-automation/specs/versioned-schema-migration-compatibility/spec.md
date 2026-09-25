# Spec Delta

## Purpose

Defines clean-install compatibility and safe history correction for the versioned PostgreSQL migrations.

## ADDED Requirements

### Requirement: Clean Flyway Migration Path
The system SHALL apply all versioned migrations successfully to an empty PostgreSQL database. Before converting legacy transfers, V14 SHALL idempotently establish `transactions.idempotency_key` when absent, preserving the existing conversion behavior; later migrations SHALL retain the final column contract.

#### Scenario: Migrating a clean database
- **WHEN** Flyway applies all migrations to an empty PostgreSQL database
- **THEN** the transfer conversion completes without relying on application startup schema creation
- **AND** the database reaches the latest migration version with `transactions.idempotency_key` present

### Requirement: Guarded Applied-Migration Checksum Repair
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
