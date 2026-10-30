## REMOVED Requirements

### Requirement: Execution Engine (Auto-Post & 1-Tap Confirmation)
**Reason**: Posting planned transactions can duplicate real-world transactions captured independently.
**Migration**: Convert schedules to non-posting expectations and link confirmed actual transactions, with manual entry available for missed events.

### Requirement: Recurring Transaction Rules Definition
**Reason**: Automatic posting and payroll bundles are retired in favor of non-posting plans.
**Migration**: Preserve rule data and occurrence history while converting schedules into expectations.

### Requirement: Complete Recurring Rule Management Interface
**Reason**: The old interface exposes automatic transaction posting and payroll allocation.
**Migration**: Provide plan management and historical visibility without triggering a ledger entry.

## ADDED Requirements

### Requirement: Non-posting expectations
The system SHALL preserve useful recurring schedules as expectations or reminders and SHALL NOT create ledger entries merely because they are due.

#### Scenario: A schedule is due
- **WHEN** an expectation reaches its due date
- **THEN** it SHALL remain an unpaid or unconfirmed expectation without changing account or obligation balances

#### Scenario: Actual payment found
- **WHEN** a recorded transaction is linked to a scheduled obligation payment
- **THEN** the obligation SHALL be updated once without creating a second ledger transaction

#### Scenario: Tracker misses a payment
- **WHEN** a payment was not captured
- **THEN** the owner SHALL be able to explicitly record it and link it to the obligation

#### Scenario: Ambiguous match
- **WHEN** multiple existing transactions could match an expectation
- **THEN** the system SHALL request review rather than silently link or create a duplicate

#### Scenario: Owner skips an occurrence
- **WHEN** the owner states that a due occurrence did not happen
- **THEN** the occurrence SHALL be recorded as skipped, the next due date SHALL advance and no ledger entry SHALL be created

#### Scenario: Manual record would duplicate a capture
- **WHEN** the owner asks to record a missed occurrence while a recorded transaction with the same amount exists in its matching window
- **THEN** the request SHALL be refused with `possible_duplicate` and the candidates until the owner states that none of them is this payment

### Requirement: Retired posting compatibility
Endpoints and fields that posted scheduled transactions SHALL fail explicitly for older clients and SHALL NOT create ledger entries.

#### Scenario: Old execution request
- **WHEN** an older client calls `POST /api/recurring/execute`, `POST /api/recurring/process-due` or `POST /api/recurring/payroll/execute`
- **THEN** the server SHALL respond 410 with code `scheduled_posting_retired` or `payroll_execution_retired` and SHALL change no balance

#### Scenario: Old rule options
- **WHEN** a client creates or updates a plan with automatic posting, payroll allocation or a new fixed-payday schedule
- **THEN** the server SHALL reject it with 422 and code `auto_post_retired`, `payroll_allocation_retired` or `payday_schedule_retired`

#### Scenario: No background posting
- **WHEN** the service runs its background tasks
- **THEN** none of them SHALL post scheduled transactions

### Requirement: Non-destructive expectation migration
The schema migration SHALL convert every existing rule into a non-posting expectation, keep its previous automatic-posting choice for audit, keep occurrence history and ledger links, and prevent automatic posting from being re-enabled without a deliberate schema change.

#### Scenario: Migration converts automatic rules
- **WHEN** a rule had automatic posting enabled before the migration
- **THEN** it SHALL become non-posting with its previous choice retained, and its occurrences and linked transactions SHALL be unchanged

#### Scenario: Interrupted or repeated migration
- **WHEN** the migration is interrupted or applied again
- **THEN** no partial change SHALL remain and a repeated run SHALL leave the converted rules unchanged

#### Scenario: Re-enabling automatic posting
- **WHEN** any client or statement tries to set automatic posting on a rule
- **THEN** the database SHALL refuse it

#### Scenario: Plan with history is deleted
- **WHEN** the owner deletes a plan that has occurrences or linked transactions
- **THEN** the server SHALL refuse with `expectation_has_history` and keep the history; deactivation remains available
