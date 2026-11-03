# Spec Delta

## MODIFIED Requirements

### Requirement: Habit Evidence Is the Owner's Own Labels
Habit evidence SHALL come only from the owner's feedback rows: purpose answers that classified the row as reviewed, corrections, and captures the owner classified. Automatic classifications, accepted history suggestions, unclassified text answers, rows recorded from a habit, undone labels, undo rows and other owners' rows SHALL never count. Only the latest remaining feedback row of each transaction SHALL count.

#### Scenario: Automatic and unconfirmed labels
- **GIVEN** three purpose answers whose text the normaliser could not classify, three AI-classified captures and three accepted history suggestions for one habit key
- **WHEN** another payment with that key is recorded
- **THEN** it SHALL be asked and no habit guess SHALL be offered

#### Scenario: Habit records are not evidence
- **GIVEN** a key with three agreeing owner answers whose next payment was recorded from the habit
- **WHEN** a further payment with that key is recorded
- **THEN** its basis SHALL still be "3 dari 3 jawaban sebelumnya" and no feedback row SHALL have been written by the habit record

#### Scenario: Another owner's labels
- **GIVEN** another owner's three agreeing labels with the same account id, signature and amounts
- **WHEN** the owner's payment is recorded
- **THEN** it SHALL be asked and no habit guess SHALL be offered

#### Scenario: Answer later corrected
- **WHEN** a transaction has a purpose answer and a later correction
- **THEN** only the correction SHALL count for that transaction

#### Scenario: Labels written before the migration
- **GIVEN** feedback rows written before V39 without review state
- **WHEN** habit evidence is read
- **THEN** a purpose answer SHALL count only when its event recorded the same reply as reviewed, and a capture SHALL NOT count

#### Scenario: Undone answers
- **GIVEN** three agreeing owner answers for one key, the third of which the owner undid in Tinjau
- **WHEN** another payment with that key is recorded
- **THEN** its basis SHALL be "2 dari 2 jawaban sebelumnya" and it SHALL be asked, because support is below 3

### Requirement: Weekly Question Rate
`GET /api/dashboard/question-rate?weeks=1..26` (default 8) SHALL report, per local week oldest first with the current one in progress, the owner's notification and capture expenses and incomes that are not movement legs or fees: recorded, asked, answered, recorded by memory, habit, AI or other rules, and owner corrections (of which on habit rows). Undone corrections SHALL not count. Only real counts SHALL be reported and an out-of-range `weeks` SHALL be rejected with 422.

#### Scenario: Outcomes counted
- **GIVEN** in one week three answered questions, one open question, two habit records, one rule-recorded merchant payment and one capture recorded from memory
- **WHEN** the rate is requested for that week
- **THEN** it SHALL report 8 recorded, 4 asked, 3 answered, 1 by memory, 2 by habit, 0 by AI and 1 by other rules

#### Scenario: Correction week
- **WHEN** the owner corrects a habit-recorded row
- **THEN** the correction SHALL be counted in the week it was made, also as a habit correction

#### Scenario: Invalid range
- **WHEN** `weeks` is 0 or 27
- **THEN** the response SHALL be 422

#### Scenario: Undone answer and correction
- **GIVEN** in one week an answered question whose answer the owner undid, and a correction the owner undid
- **WHEN** the rate is requested for that week
- **THEN** the question SHALL count as asked but not answered, and no correction SHALL be counted
