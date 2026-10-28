# Spec Delta

## MODIFIED Requirements

### Requirement: Recurring Transaction Rules Definition
The system SHALL support creating, querying, updating, deactivating, and deleting recurring rules.

#### Scenario: Required behavior and constraints
- **WHEN** an owner manages recurring transaction rules
- **THEN** the following required behavior and constraints hold:

  The system SHALL support creating, querying, updating, deactivating, and deleting recurring rules:
  1. Each rule SHALL specify valid transaction, amount, account, classification, schedule, automation, payroll, and notes fields required by its type.
  2. Every referenced account, category, and obligation SHALL be active and owned by the authenticated user on both creation and update.
  3. Transfers SHALL require distinct source and target accounts; category kind SHALL match income or expense rule type. A generic transfer source and target SHALL NOT be an investment position; investing automation requires a dedicated trade rule and is outside this change.
  4. Weekly rules SHALL identify a weekday, monthly rules SHALL identify a day from 1 through 31, and payday rules SHALL use the user's payday setting.
  5. Updates SHALL distinguish omitted fields from explicit clearing of optional fields and SHALL remove fields incompatible with a changed rule type.
  6. The system SHALL compute `next_due_date` from the validated schedule.
  7. Investment-top-up rules (`investment_topup`, requiring a target) SHALL validate the liquid funding account and amount-tracked mutual-fund leaf target, use `auto_post = false`, and reject category, obligation, and payroll-allocation linkage. Their creation SHALL NOT itself record a contribution.

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

#### Scenario: Define monthly Bibit contribution
- **WHEN** the owner schedules Rp112.590 monthly from BCA RDN to a specific eligible Bibit product
- **THEN** the system saves a manually confirmed investment-top-up rule with the selected due day and makes no ledger or valuation changes

#### Scenario: Reject automatic investment logging
- **WHEN** a create or update request enables automatic posting or payroll allocation for an investment-top-up rule
- **THEN** the request fails and the saved rule remains unchanged

### Requirement: Execution Engine (Auto-Post & 1-Tap Confirmation)
The system SHALL execute due recurring rules through a server-owned scheduler and explicit manual confirmation.

#### Scenario: Required behavior and constraints
- **WHEN** recurring rules become due or the owner requests their execution
- **THEN** the following required behavior and constraints hold:

  The system SHALL execute due recurring rules through a server-owned scheduler and explicit manual confirmation:
  1. Active `auto_post` rules SHALL be evaluated without requiring a browser session.
  2. Each rule occurrence SHALL have a stable occurrence identity and SHALL execute at most once across retries or concurrent workers.
  3. Each execution SHALL validate current ownership, active status, type constraints, liquid-account roles for generic transfers, and available source balance while holding the required locks.
  4. Successful execution SHALL atomically write linked ledger records, apply obligation effects, store the recurring rule ID and occurrence identity, set `last_executed_at`, and advance `next_due_date` beyond the processed occurrence.
  5. Failed execution SHALL create no financial mutation, SHALL NOT advance the schedule, and SHALL expose an actionable failure state for the user.
  6. Manual execution SHALL reject inactive rules.
  7. Overdue occurrences SHALL follow an explicit bounded catch-up policy and SHALL NOT be replayed merely because a page reloads.
  8. Investment-top-up rules SHALL never execute through automatic processing. Confirmation SHALL use the same contribution semantics as a manual top-up and commit the occurrence, both ledger legs, capital/value adjustment, and schedule advancement atomically. Repeated or concurrent confirmation of one occurrence SHALL record it once. Failure SHALL leave that occurrence pending and present its error to the owner.

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

#### Scenario: Confirm the actual monthly debit
- **WHEN** an investment-top-up rule is due and the owner confirms after the successful Rp112.590 debit
- **THEN** one contribution is recorded from the specified RDN to the specified product, the occurrence is fulfilled, and the next due date advances one month

#### Scenario: Date alone does not confirm a debit
- **WHEN** a monthly investment-top-up rule becomes due and automatic processing runs
- **THEN** it remains pending without any ledger or valuation changes until the owner confirms

#### Scenario: Funding validation fails during confirmation
- **WHEN** the owner confirms a due investment top-up but funds are insufficient or its product is no longer eligible
- **THEN** the rule stays pending, its next due date is unchanged, no contribution is recorded, and the interface reports the failure

#### Scenario: Concurrent confirmation
- **WHEN** two confirmation requests refer to the same investment rule occurrence
- **THEN** only one contribution is recorded and the schedule advances once
