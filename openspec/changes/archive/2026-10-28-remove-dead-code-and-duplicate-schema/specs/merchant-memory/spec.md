## MODIFIED Requirements

### Requirement: Append-Only Classification Feedback
The system SHALL store one feedback row for each owner correction, each purpose answer and each accepted history suggestion that changed the row.

#### Scenario: Owner recategorises a notification row
- **GIVEN** an AI-classified row in "Belanja" recorded from a myBCA notification on a Wednesday at 19:40 local time
- **WHEN** the owner changes its notes to "makan malam" and its category to "Makanan & Minuman"
- **THEN** one feedback row SHALL record both before and after values, weekday 3, hour 19, the amount, the account, `bca` and the signature.

#### Scenario: Update rejected
- **WHEN** any statement tries to update a feedback row
- **THEN** the database SHALL reject it and the row SHALL be unchanged.

#### Scenario: Edit without classification change
- **WHEN** the owner changes only the amount or date of a row
- **THEN** no feedback row SHALL be written.

#### Scenario: Accepted history suggestion
- **WHEN** the owner accepts a history suggestion that moves a "Kedai Awan" row from "Makanan & Minuman" to "Jajan & Nongkrong"
- **THEN** one feedback row SHALL have kind `accepted_suggestion`, the suggestion's id, and the before and after values.

## ADDED Requirements

### Requirement: Feedback Row Contents
Each feedback row SHALL record the transaction, the values before and after (merchant, notes, category, pillar, type, offset category), and context features: local weekday and hour in the configured time zone, amount, account, institution, text signature and transaction source.

#### Scenario: Purpose answer feedback
- **GIVEN** a merchant-less payment recorded on a Monday at 12:10 local time
- **WHEN** the owner answers its purpose question with "makan siang"
- **THEN** the feedback row SHALL hold the notes before and after, weekday 1, hour 12, the amount, the account and the transaction source.

### Requirement: Accepted Suggestion Feedback Kind
A row written for an accepted history suggestion SHALL have kind `accepted_suggestion` and carry the suggestion id.

#### Scenario: Direct correction is not a suggestion
- **WHEN** the owner recategorises a row directly in the ledger
- **THEN** its feedback row SHALL have a kind other than `accepted_suggestion` and no suggestion id.

### Requirement: Feedback Is Append-Only
Feedback rows SHALL never be updated; an attempt SHALL fail.

#### Scenario: Two corrections keep two examples
- **WHEN** the owner corrects the same row twice
- **THEN** there SHALL be two feedback rows and the first SHALL keep its original values.

### Requirement: Feedback Outlives Its Row
Feedback SHALL survive deletion of the transaction or category it describes.

#### Scenario: Deleted transaction
- **GIVEN** a feedback row for an owner correction
- **WHEN** the owner deletes the corrected transaction
- **THEN** the feedback row SHALL remain with its before and after values.
