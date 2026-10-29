## MODIFIED Requirements

### Requirement: Transaction Provenance Fields
Every transaction SHALL carry `source` (manual, notification, recurring, payroll, movement, reconcile, investment, import or capture), `review_status` (auto, needs_review or reviewed), `classified_by` (rule, ai, owner, system or empty), `classification_confidence` (empty or 0 to 1), `owner_edited_at` (empty or a time) and `merchant` (empty or a display name of 1–120 characters). The database SHALL reject any other value. `notes` SHALL remain the human description.

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
