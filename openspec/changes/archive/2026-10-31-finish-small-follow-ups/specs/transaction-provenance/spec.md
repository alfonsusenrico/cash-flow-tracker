## MODIFIED Requirements

### Requirement: Provenance Written by Every Recording Path
Each path that creates a transaction SHALL write its provenance explicitly: manual create → manual, reviewed, owner; movement → movement, reviewed, system; reconcile → reconcile, reviewed, system; investment trade or top-up → investment, reviewed, system; a notification as "Provenance of Notification Records" states; a plan or payroll row as "Provenance of Plan and Payroll Records" states.

#### Scenario: Manual expense from the web
- **WHEN** the owner records an expense through the transactions API
- **THEN** its source SHALL be manual, its review status reviewed and its classifier owner

#### Scenario: Both legs of a movement
- **WHEN** the owner records a movement between pockets
- **THEN** both legs SHALL have movement source, reviewed status and system classifier

#### Scenario: Scheduled recurring payment
- **WHEN** a historical transaction was posted by a recurring rule
- **THEN** its recurring source and system classifier SHALL remain intact and the schedule SHALL not create a new transaction

#### Scenario: Payroll allocation
- **WHEN** a historical transaction was created by payroll allocation
- **THEN** its payroll source and system classifier SHALL remain intact and no new allocation batch SHALL be posted

#### Scenario: Reconcile adjustment
- **WHEN** the owner reconciles an account and an adjustment is recorded
- **THEN** the adjustment SHALL have reconcile source, reviewed status and system classifier

#### Scenario: Investment top-up and trade
- **WHEN** the owner records an investment top-up or trade
- **THEN** its legs SHALL have investment source, reviewed status and system classifier

#### Scenario: Notification classified by the model
- **WHEN** a notification is recorded from an AI interpretation with confidence 0.82 and no review reason
- **THEN** it SHALL have notification source, auto review, AI classifier and confidence 0.82

#### Scenario: Notification classified by deterministic rules
- **WHEN** a notification is recorded by the deterministic path with a category or as a broker trade
- **THEN** each row SHALL have notification source, auto review, rule classifier and no confidence

#### Scenario: Owner resolves an unresolved notification
- **WHEN** the owner records a needs-review notification with their own type, account and category
- **THEN** the row SHALL have notification source, reviewed status and owner classifier

#### Scenario: Notification recorded with a review reason
- **WHEN** a notification is recorded with an unresolved review reason
- **THEN** it SHALL have notification source and needs-review status

#### Scenario: Notification classified from merchant memory
- **WHEN** a notification is recorded from an owner merchant-memory entry
- **THEN** it SHALL have notification source, auto review, rule classifier and no confidence

#### Scenario: Notification recorded from a habit
- **WHEN** a merchant-less notification is recorded from a habit
- **THEN** it SHALL have notification source, reviewed status, habit classifier and no confidence

## ADDED Requirements

### Requirement: Provenance of Notification Records
A transaction recorded from a notification SHALL have source notification and SHALL be written as auto or needs_review with classifier ai or rule, or as reviewed with classifier habit when it is recorded from a habit.

#### Scenario: Model interpretation with a review reason
- **WHEN** a notification is recorded from an AI interpretation that carries an unresolved review reason
- **THEN** it SHALL have notification source, needs-review status and AI classifier

### Requirement: Provenance of Plan and Payroll Records
Existing recurring and payroll provenance SHALL remain intact; an owner-requested record of one missed plan occurrence SHALL keep the plan link and SHALL be written as recurring, reviewed, system.

#### Scenario: Recording one missed plan occurrence
- **WHEN** the owner records one missed occurrence of a plan
- **THEN** the new row SHALL keep the plan link and SHALL have recurring source, reviewed status and system classifier
