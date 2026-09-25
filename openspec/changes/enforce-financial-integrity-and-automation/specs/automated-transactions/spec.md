# Spec Delta

## MODIFIED Requirements

### Requirement: Recurring Transaction Rules Definition
The system SHALL support creating, querying, updating, deactivating, and deleting recurring rules:
1. Each rule SHALL specify valid transaction, amount, account, classification, schedule, automation, payroll, and notes fields required by its type.
2. Every referenced account, category, and obligation SHALL be active and owned by the authenticated user on both creation and update.
3. Transfers SHALL require distinct source and target accounts; category kind SHALL match income or expense rule type. A generic transfer source and target SHALL NOT be an investment position; investing automation requires a dedicated trade rule and is outside this change.
4. Weekly rules SHALL identify a weekday, monthly rules SHALL identify a day from 1 through 31, and payday rules SHALL use the user's payday setting.
5. Updates SHALL distinguish omitted fields from explicit clearing of optional fields and SHALL remove fields incompatible with a changed rule type.
6. The system SHALL compute `next_due_date` from the validated schedule.

#### Scenario: Updating a rule without crossing tenant boundaries
- **WHEN** a user attempts to update a recurring rule with an account, category, or obligation owned by another user
- **THEN** the system returns not found and preserves the existing rule

#### Scenario: Clearing an optional relationship
- **WHEN** a user edits an expense rule and explicitly removes its obligation
- **THEN** the rule persists with no obligation instead of retaining the previous relationship

#### Scenario: Creating a monthly debt payment rule
- **WHEN** a user creates an automated expense rule for "Cicilan Mobil" of 2,500,000 IDR on monthly day 25 with auto-post enabled and a valid obligation
- **THEN** the validated rule is saved with its next due date calculated

#### Scenario: Creating a weekly pocket transfer rule
- **WHEN** a user creates a recurring 300,000 IDR transfer from BCA to a distinct Jago pocket and selects a weekday
- **THEN** the validated rule is saved with its next due date on the upcoming selected weekday

#### Scenario: Rejecting a generic recurring transfer to an investment position
- **WHEN** a user creates or updates a recurring transfer with an investment position as its source or destination
- **THEN** the server rejects the rule and preserves any existing rule unchanged

### Requirement: Execution Engine (Auto-Post & 1-Tap Confirmation)
The system SHALL execute due recurring rules through a server-owned scheduler and explicit manual confirmation:
1. Active `auto_post` rules SHALL be evaluated without requiring a browser session.
2. Each rule occurrence SHALL have a stable occurrence identity and SHALL execute at most once across retries or concurrent workers.
3. Each execution SHALL validate current ownership, active status, type constraints, liquid-account roles for generic transfers, and available source balance while holding the required locks.
4. Successful execution SHALL atomically write linked ledger records, apply obligation effects, store the recurring rule ID and occurrence identity, set `last_executed_at`, and advance `next_due_date` beyond the processed occurrence.
5. Failed execution SHALL create no financial mutation, SHALL NOT advance the schedule, and SHALL expose an actionable failure state for the user.
6. Manual execution SHALL reject inactive rules.
7. Overdue occurrences SHALL follow an explicit bounded catch-up policy and SHALL NOT be replayed merely because a page reloads.

#### Scenario: Auto-posting without a browser session
- **WHEN** an active auto-post rule becomes due while no user is signed in
- **THEN** the server scheduler executes the occurrence once or records an actionable failure

#### Scenario: Retrying one scheduled occurrence
- **WHEN** two workers or retries process the same occurrence identity
- **THEN** at most one ledger operation is committed and the schedule advances once

#### Scenario: Insufficient funds for a recurring payment
- **WHEN** an expense rule is due but its source balance is below the rule amount
- **THEN** no transaction is created, the due date is not advanced, and the rule reports insufficient funds

#### Scenario: Auto-posting a due scheduled bill
- **WHEN** the server scheduler processes an active due monthly bill with auto-post enabled and sufficient funds
- **THEN** it records the transaction once, applies linked effects, and advances the next due date by one interval

#### Scenario: Confirming a pending scheduled payment
- **WHEN** a user confirms an active due rule that requires manual confirmation
- **THEN** the system records it once, applies any obligation payment, and advances the rule beyond that occurrence
