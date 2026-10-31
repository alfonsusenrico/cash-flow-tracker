## MODIFIED Requirements

### Requirement: Transaction Provenance Fields
Every transaction SHALL carry `source` (manual, notification, recurring, payroll, movement, reconcile, investment, import or capture), `review_status` (auto, needs_review or reviewed), `classified_by` (rule, ai, owner, system, habit or empty), `classification_confidence` (empty or 0 to 1), `owner_edited_at` (empty or a time) and `merchant` (empty or a display name of 1–120 characters). The database SHALL reject any other value. `notes` SHALL remain the human description.

#### Scenario: Unknown source is rejected by the database
- **WHEN** any writer stores a transaction with `source = 'bot'`
- **THEN** the database SHALL reject the write
- **AND** no row SHALL change.

#### Scenario: Confidence outside the range is rejected
- **WHEN** any writer stores `classification_confidence = 1.5`
- **THEN** the database SHALL reject the write.

#### Scenario: Notes keep the description
- **GIVEN** a notification-recorded expense described "Bayar parkir"
- **WHEN** the owner sets its merchant to "Parkir Mal Contoh"
- **THEN** `notes` SHALL still be "Bayar parkir"
- **AND** `merchant` SHALL be "Parkir Mal Contoh".

#### Scenario: Capture source accepted
- **WHEN** a transaction is recorded from the owner's typed text
- **THEN** its `source` SHALL be `capture` and the database SHALL accept it.

#### Scenario: Habit classifier accepted
- **WHEN** a transaction is recorded from a habit
- **THEN** its `classified_by` SHALL be `habit` and the database SHALL accept it.

### Requirement: Provenance Written by Every Recording Path
Each path that creates a transaction SHALL write its provenance explicitly: manual create → manual, reviewed, owner; movement → movement, reviewed, system; reconcile → reconcile, reviewed, system; investment trade or top-up → investment, reviewed, system; notification → notification, auto or needs_review, ai or rule, or notification, reviewed, habit when recorded from a habit. Existing recurring and payroll provenance SHALL remain intact; an owner-requested record of one missed plan occurrence keeps the plan link and is written as recurring, reviewed, system.

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
