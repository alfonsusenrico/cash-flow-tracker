# merchant-memory Specification

## Purpose
Learn from every owner correction and answer: derive a stable match key per notification, keep
append-only labelled examples, remember the owner's classification per key, apply exact owner merchant
memory before the model, and give the model the owner's memory and similar history.

## Requirements

### Requirement: Notification Match Keys
The system SHALL derive one match key per recorded expense or income notification: a merchant key from the named merchant or counterparty, normalised for case and whitespace, or, when the notification names none, a signature key from the institution, the direction, the bank's category or channel and any sender mask. Owner names and owned-account transfers SHALL NOT produce a merchant key.

#### Scenario: Named merchant
- **WHEN** the notification "You've paid Rp125.000 to Kedai Awan" is recorded
- **THEN** its match key SHALL be the merchant key for "kedai awan".

#### Scenario: Bank category only
- **WHEN** "You spent IDR 187,500.00 at Groceries." from myBCA is recorded
- **THEN** its match key SHALL be a signature key made of `bca`, `expense` and `groceries`, and it SHALL be treated as merchant-less.

#### Scenario: Masked sender
- **WHEN** "You received IDR 57,000.00 from ***AB ****CD *E at Account Transfer category." is recorded
- **THEN** its signature key SHALL include `bca`, `income`, the transfer category and the normalised mask.

### Requirement: Append-Only Classification Feedback
The system SHALL store one feedback row for each owner correction and each purpose answer, with the transaction, the values before and after (merchant, notes, category, pillar, type, offset category), and context features: local weekday and hour in the configured time zone, amount, account, institution, text signature and transaction source. Feedback rows SHALL never be updated; an attempt SHALL fail. Feedback SHALL survive deletion of the transaction or category it describes.

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

### Requirement: Merchant Memory Learned From the Owner
The system SHALL keep one memory entry per owner and match key with display merchant, notes, category, pillar, offset, source (`owner` or `ai`), hit count and last use. Each owner correction or purpose answer of a categorised plain expense or income SHALL upsert its key's entry as `owner`. A gated model classification of a named merchant SHALL create an `ai` entry only when no owner entry exists. Movement legs SHALL write feedback but no memory.

#### Scenario: Owner correction teaches memory
- **WHEN** the owner sets the category of a row recorded from "You've paid Rp125.000 to Kedai Awan" to "Jajan & Nongkrong" with pillar want
- **THEN** the memory entry for "kedai awan" SHALL hold that category and pillar with source `owner`.

#### Scenario: AI never overwrites the owner
- **GIVEN** an owner entry for "kedai awan"
- **WHEN** the model later classifies a Kedai Awan payment differently
- **THEN** the entry SHALL keep the owner's values.

#### Scenario: Movement rename
- **WHEN** the owner renames a movement
- **THEN** each leg SHALL get a feedback row and no memory entry SHALL change.

### Requirement: Classification Order
For an expense or income, an owner memory entry with the notification's merchant key and an active category of that direction SHALL be applied first: the row is recorded from memory with `classified_by = rule` and `review_status = auto`, the hit is counted and the model SHALL NOT be called. Otherwise the model SHALL classify when AI is enabled, and the deterministic path when it is not or the model's answer is unusable. Signature keys SHALL never short-circuit.

#### Scenario: Memory hit skips the model
- **GIVEN** an owner memory entry for "kedai awan" in "Jajan & Nongkrong"
- **WHEN** AI is enabled and "You've paid Rp40.000 to Kedai Awan" is processed
- **THEN** the provider SHALL NOT be called and the row SHALL be recorded in "Jajan & Nongkrong" with `classified_by = rule` and `review_status = auto`.

#### Scenario: Archived memory category
- **GIVEN** the memory entry's category was archived
- **WHEN** the merchant pays again
- **THEN** memory SHALL NOT apply and normal classification SHALL run.

#### Scenario: Signature memory does not auto-classify
- **GIVEN** an owner entry for the signature of myBCA "at Groceries" expenses
- **WHEN** another "at Groceries" notification arrives
- **THEN** the model or deterministic path SHALL classify it and it SHALL get a purpose question.

### Requirement: Model Context From Memory and Similar History
The model input SHALL include the categories with default pillars, up to 30 memory entries (exact key first, owner before AI) and up to the history limit of similar past rows (same key first, then same institution and direction at half to double the amount; owner-confirmed first) with their final labels. It SHALL stay within 60 000 bytes by dropping history and then memory from the end, and stay redacted.

#### Scenario: Similar history preferred
- **GIVEN** 25 unrelated recent rows and two earlier "Kedai Awan" rows, one corrected by the owner
- **WHEN** a Kedai Awan payment is interpreted
- **THEN** the history SHALL start with the owner-corrected Kedai Awan row, then the other one, and SHALL NOT contain unrelated rows of other institutions.

#### Scenario: Input over budget
- **WHEN** memory and history would make the input exceed 60 000 bytes
- **THEN** history and then memory entries SHALL be dropped until it fits, and the model SHALL still be called.

### Requirement: Interpretation Prompt Names Merchants
The interpretation prompt SHALL forbid generic placeholder descriptions when the notification names a merchant or an owner memory entry matches, SHALL ask for the best guess with a confidence below 0.8 when the notification names no merchant, and SHALL return a `merchant` field. Each processing attempt SHALL store the prompt version `notification-interpretation-9`.

#### Scenario: Prompt version stored
- **WHEN** an event is claimed for AI processing
- **THEN** its stored prompt version SHALL be `notification-interpretation-9`.

#### Scenario: Merchant stored
- **WHEN** the model records "You've paid Rp125.000 to Kedai Awan" with merchant "Kedai Awan"
- **THEN** the row's `merchant` SHALL be "Kedai Awan".
