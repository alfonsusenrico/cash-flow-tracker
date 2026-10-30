# automated-transactions Specification

## Purpose
Define scheduled payment rules, automatic execution where configured, and explicit confirmation of due payments.

## Requirements

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

### Requirement: Plan References Are Owned and Validated
Creating or updating a recurring plan SHALL accept only active accounts, categories and obligations owned by the same user. An update SHALL validate the final combined state of the plan, and an optional relationship SHALL be cleared only when the request sets it to null explicitly.

#### Scenario: Updating a plan without crossing tenant boundaries
- **WHEN** a user creates or updates a plan with another user's account
- **THEN** the request is rejected and the stored plan is unchanged

#### Scenario: Clearing an optional relationship
- **WHEN** a user updates a plan and sets its obligation to null
- **THEN** the link is removed, while omitted fields keep their stored values

### Requirement: Plan Endpoints Match the Plan Type
A transfer plan SHALL use two distinct liquid accounts and SHALL reject an investment position as either endpoint. An investment top-up plan SHALL target an eligible amount-tracked product and SHALL reject a category or an obligation.

#### Scenario: Rejecting a transfer plan to an investment position
- **WHEN** a user creates a transfer plan whose source or target is an investment position
- **THEN** the request is rejected with 422 and no plan is stored

#### Scenario: Rejecting forbidden options on an investment top-up plan
- **WHEN** a user creates or updates an investment top-up plan with a category or an obligation
- **THEN** the request is rejected with 422 and the stored plan is unchanged

### Requirement: Explicit Occurrence Recording Is Atomic and Idempotent
Recording a plan occurrence through `POST /api/recurring/{id}/occurrences/record` SHALL commit the ledger effect, the occurrence record and the advanced due date in one database transaction. A repeated or concurrent record of the same occurrence SHALL return the existing result, and a failed record SHALL leave the occurrence pending with the ledger and schedule unchanged.

#### Scenario: Concurrent recording of one occurrence
- **WHEN** two requests record the same occurrence at the same time
- **THEN** exactly one occurrence and one ledger effect are committed

#### Scenario: Recording fails
- **WHEN** recording an occurrence fails validation, for example for insufficient funds
- **THEN** no ledger row is created, the occurrence stays pending and the due date is unchanged

### Requirement: Monthly Investment Top-up Waits for an Explicit Record
A monthly investment top-up plan SHALL move money only when the owner records its occurrence; its due date alone SHALL NOT create a contribution.

#### Scenario: Recording a monthly investment top-up once
- **WHEN** the top-up plan becomes due and the owner then records its occurrence twice
- **THEN** nothing moves before the record, exactly one contribution is created, the second request reports the existing result, and the next due date advances

### Requirement: Automatic linking of one certain payment
A due occurrence of an active expense or income plan SHALL be linked automatically when exactly one unlinked recorded transaction has the same owner and type, the plan's account and the identical amount, is dated from three days before to seven days after the due date and not in the future, is not an internal movement leg or fee, was not rejected for this plan, and is not a candidate for any other due plan. Transfer and top-up plans SHALL NOT be linked automatically.

#### Scenario: Exactly one candidate
- **WHEN** one matching payment was recorded the day after a due date
- **THEN** the occurrence SHALL be linked once with link method `auto`, no transaction SHALL be created, and an audit event with identifiers and actions only SHALL be written

#### Scenario: Concurrent passes
- **WHEN** several automatic-linking passes run at the same time for the same owner
- **THEN** the occurrence SHALL be linked once and debt progress SHALL change once

#### Scenario: Two candidates
- **WHEN** two matching payments were recorded in the window
- **THEN** nothing SHALL be linked and the occurrence SHALL stay due with both candidates for the owner

#### Scenario: Wrong account, amount or window
- **WHEN** the only payments differ in account or amount, or fall four days before or eight days after the due date
- **THEN** nothing SHALL be linked

#### Scenario: A payment two plans could claim
- **WHEN** one payment matches due occurrences of two plans
- **THEN** neither occurrence SHALL be linked automatically

#### Scenario: When linking runs
- **WHEN** a transaction is created or updated, including from a notification or text capture, or a plan is created or updated
- **THEN** the owner's due plans SHALL be checked shortly afterwards without delaying the recording, and every due plan SHALL also be checked by the periodic background pass

### Requirement: Undoing a plan link
The owner SHALL be able to undo a link from a due-plan view where automatic links are labelled "Ditautkan otomatis". Undoing SHALL return the occurrence to due, reverse debt progress that the link applied exactly once, and record the pair so it is never linked automatically again.

#### Scenario: Undo an automatic debt payment link
- **WHEN** the owner selects "Batalkan tautan" on an automatic link that paid off a debt
- **THEN** the debt SHALL be restored and reactivated once, the occurrence SHALL be due again, and a repeated undo SHALL change nothing

#### Scenario: No automatic relink
- **WHEN** automatic linking runs again after an undo
- **THEN** the same transaction SHALL NOT be linked to that plan automatically, while the owner MAY still link it explicitly

#### Scenario: Resolving after an undo
- **WHEN** the owner resolves an undone occurrence whose following occurrence was already resolved
- **THEN** the plan's next due date SHALL move to the first later occurrence that is not resolved
