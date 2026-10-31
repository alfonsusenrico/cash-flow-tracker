## MODIFIED Requirements

### Requirement: Classification Order
For an expense or income, an owner memory entry with the notification's merchant key and an active category of that direction SHALL be applied first: the row is recorded from memory with `classified_by = rule` and `review_status = auto`, the hit is counted and the model SHALL NOT be called. Otherwise a qualifying habit, else the model SHALL classify when AI is enabled, and the deterministic path when it is not or the model's answer is unusable. Signature keys SHALL never short-circuit.

#### Scenario: Memory hit skips the model
- **GIVEN** an owner memory entry for "kedai awan" in "Jajan & Nongkrong"
- **WHEN** AI is enabled and "You've paid Rp40.000 to Kedai Awan" is processed
- **THEN** the provider SHALL NOT be called and the row SHALL be recorded in "Jajan & Nongkrong" with `classified_by = rule` and `review_status = auto`.

#### Scenario: Archived memory category
- **GIVEN** the memory entry's category was archived
- **WHEN** the merchant pays again
- **THEN** memory SHALL NOT apply and normal classification SHALL run.

#### Scenario: Signature memory does not auto-classify
- **GIVEN** an owner entry for the signature of myBCA "at Groceries" expenses and fewer than three owner labels for the payment's habit key
- **WHEN** another "at Groceries" notification arrives
- **THEN** the model or deterministic path SHALL classify it and it SHALL get a purpose question.

#### Scenario: Habit skips the model
- **GIVEN** three agreeing owner answers for the payment's habit key and AI enabled
- **WHEN** another payment of that key is processed
- **THEN** the provider SHALL NOT be called and the row SHALL be recorded with `classified_by = habit`.

### Requirement: Feedback Row Contents
Each feedback row SHALL record the transaction, the values before and after (merchant, notes, category, pillar, type, offset category), the classifier before and after and the review status after, and context features: local weekday and hour in the configured time zone, amount, account, institution, text signature and transaction source.

#### Scenario: Purpose answer feedback
- **GIVEN** a merchant-less payment recorded on a Monday at 12:10 local time
- **WHEN** the owner answers its purpose question with "makan siang"
- **THEN** the feedback row SHALL hold the notes before and after, weekday 1, hour 12, the amount, the account and the transaction source.

#### Scenario: Correction of a habit record
- **WHEN** the owner changes the category of a row recorded from a habit
- **THEN** the correction feedback row SHALL hold the classifier `habit` before and `owner` after, and the review status after.
