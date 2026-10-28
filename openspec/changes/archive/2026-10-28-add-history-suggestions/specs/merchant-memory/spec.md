## MODIFIED Requirements

### Requirement: Append-Only Classification Feedback
The system SHALL store one feedback row for each owner correction, each purpose answer and each accepted history suggestion that changed the row, with the transaction, the values before and after (merchant, notes, category, pillar, type, offset category), and context features: local weekday and hour in the configured time zone, amount, account, institution, text signature and transaction source. A row written for an accepted history suggestion SHALL have kind `accepted_suggestion` and carry the suggestion id. Feedback rows SHALL never be updated; an attempt SHALL fail. Feedback SHALL survive deletion of the transaction or category it describes.

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
