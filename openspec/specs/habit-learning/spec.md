# habit-learning Specification

## Purpose
Learn the owner's habits from their own answers about merchant-less payments, so that a payment the owner has classified the same way again and again is recorded without a question, a weaker pattern is offered first, and the question rate visibly falls.

## Requirements

### Requirement: Habit Evidence Is the Owner's Own Labels
Habit evidence SHALL come only from the owner's feedback rows: purpose answers that classified the row as reviewed, corrections, and captures the owner classified. Automatic classifications, accepted history suggestions, unclassified text answers, rows recorded from a habit and other owners' rows SHALL never count. Only the latest feedback row of each transaction SHALL count.

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

### Requirement: Habit Key and Amount Band
A habit SHALL be keyed by institution, bank category signature and observed account. A label SHALL belong to a payment's habit only when its amount is within ±25% as a ratio, that is when each amount is at most 1.25 times the other. Local time and weekday SHALL NOT be part of the key.

#### Scenario: Amount outside the band
- **GIVEN** three agreeing answers for Groceries payments of Rp 11.500 to Rp 12.500 on BCA Harian
- **WHEN** a Rp 16.000 Groceries payment on BCA Harian is recorded
- **THEN** it SHALL be asked and no habit guess SHALL be offered

#### Scenario: Band edges
- **WHEN** a Rp 50.000 payment is compared with labels of Rp 40.000 and Rp 39.999
- **THEN** the first SHALL be in the band and the second SHALL NOT

### Requirement: Habit Agreement and Weighting
Over the latest 10 labelled payments of the key, the system SHALL record automatically only when at least 3 payments are labelled and at least 90% of their weight agrees on one category and Kakeibo pillar. An answer SHALL weigh 1 and a correction 2. Time of day and weekday SHALL only break ties and choose the wording.

#### Scenario: Three of three
- **WHEN** three answers agree on "Makanan & Minuman" with pillar need
- **THEN** the next payment of the key SHALL be recorded automatically

#### Scenario: Two answers
- **WHEN** only two answers exist
- **THEN** the next payment SHALL be asked with the habit's guess first

#### Scenario: Three of four
- **WHEN** three answers agree and one answer differs (75%)
- **THEN** the next payment SHALL be asked with the habit's guess first

#### Scenario: Correction outweighs an answer
- **WHEN** nine answers agree and one correction differs
- **THEN** the agreement SHALL be 9 of 11 and the next payment SHALL be asked, whereas nine agreeing answers and one differing answer SHALL be recorded automatically

#### Scenario: Changed habit
- **WHEN** a key has five older labels in one category and ten newer labels in another
- **THEN** only the ten newer labels SHALL count

### Requirement: Correction Re-Asks
An owner edit that changes the classification of a habit-recorded row SHALL write a `correction` feedback row whose previous classifier is `habit`, and the habit SHALL be re-evaluated from the stored feedback for the next payment, without any cached state.

#### Scenario: Correction of a habit record
- **GIVEN** a key with three agreeing answers and a payment recorded from the habit
- **WHEN** the owner changes that payment's category and then another payment of the key is recorded
- **THEN** the other payment SHALL be asked, with a basis of 3 agreeing of 4 labelled payments

### Requirement: Payments a Habit Never Records
A habit SHALL record only an unpaired merchant-less expense that would otherwise get a purpose question, on the account the habit was learned for, in an active non-movement expense category. Income, movements, own-account or transfer-shaped payments, payments naming the owner, ambiguous movement legs and payments whose account still needs confirmation SHALL never be recorded from a habit.

#### Scenario: Income
- **GIVEN** three agreeing owner labels for masked incoming transfers
- **WHEN** another such transfer is recorded
- **THEN** it SHALL NOT be recorded from a habit

#### Scenario: Transfer-shaped payment
- **GIVEN** three agreeing labels for "You spent IDR 50,000.00 at Transfer." payments
- **WHEN** another one is recorded
- **THEN** it SHALL NOT be recorded from a habit

#### Scenario: Movement category labels
- **GIVEN** three labels in "Internal Movement" for one key
- **WHEN** another payment of the key is recorded
- **THEN** no habit guess SHALL be offered and it SHALL NOT be recorded from a habit

### Requirement: Recording From a Habit
A payment recorded from a habit SHALL take the habit's category, pillar, notes and merchant, `classified_by = habit`, `review_status = reviewed`, no confidence and no purpose question, SHALL keep the notification's amount, date and account, SHALL NOT call the model, and SHALL store the basis on its event. Re-sending the same notification SHALL change nothing.

#### Scenario: Recorded without a question
- **GIVEN** three agreeing answers "makan siang" in "Makanan & Minuman" for Rp 12.000 Groceries payments on BCA Harian
- **WHEN** another Rp 12.000 Groceries payment is recorded
- **THEN** the row SHALL have that category, pillar need and notes "makan siang", `classified_by = habit` and `review_status = reviewed`, the notification's amount, date and account, and SHALL appear neither in the review queue nor in the attention count

#### Scenario: Model not called
- **GIVEN** the notification AI is enabled and a habit applies
- **WHEN** the event is processed
- **THEN** the provider SHALL NOT be called and the row SHALL be recorded from the habit

#### Scenario: Re-processing
- **WHEN** the same notification payload is sent again
- **THEN** the same event and result SHALL be returned, one transaction SHALL exist and no feedback row SHALL be added

#### Scenario: Paired later as a movement
- **WHEN** a habit-recorded expense is linked to an income as a movement
- **THEN** its classifier SHALL become `system`

### Requirement: Habit Guess First
When a payment with habit labels is asked, its first suggestion SHALL be the habit's guess, kind `habit`, labelled "Sepertinya <kategori>?", carrying the guessed notes, category, pillar and its basis; an identical history suggestion SHALL NOT be repeated.

#### Scenario: Guess after two answers
- **GIVEN** two answers "makan siang" in "Makanan & Minuman"
- **WHEN** a third payment of the key is asked
- **THEN** its first suggestion SHALL be "Sepertinya Makanan & Minuman?" with notes "makan siang" and basis 2 of 2

### Requirement: Habit Kill Switch
With `HABIT_AUTO_RECORD_ENABLED=false` no payment SHALL be recorded from a habit, and the guess SHALL still be offered first. The default SHALL be true.

#### Scenario: Switched off
- **GIVEN** three agreeing answers and the setting false
- **WHEN** another payment of the key is recorded
- **THEN** it SHALL be asked with "Sepertinya Makanan & Minuman?" first and `classified_by = rule`

### Requirement: Habit Explanation
A habit-recorded row SHALL be explained everywhere it is shown: the companion result (1.8.0 and later) SHALL carry `habit` with the label "Dicatat dari kebiasaan: <kategori>" and the basis "<n> dari <m> jawaban sebelumnya"; the ledger list and detail SHALL carry `habit_basis`; the web ledger row and edit dialog SHALL show "Dicatat dari kebiasaan" with the basis, and the classifier SHALL read "Kebiasaan". After an owner edit the explanation SHALL disappear.

#### Scenario: Companion result
- **WHEN** a 1.8.0 companion reads a habit-recorded result
- **THEN** it SHALL include `habit.label` "Dicatat dari kebiasaan: Makanan & Minuman" and `habit.basis` "3 dari 3 jawaban sebelumnya"

#### Scenario: Ledger
- **WHEN** the owner views a habit-recorded row in the ledger and opens it
- **THEN** the row SHALL show "Dicatat dari kebiasaan · 3 dari 3" and the dialog "Dicatat dari kebiasaan: <kategori>" with "3 dari 3 jawaban sebelumnya"

#### Scenario: Owner classified the row
- **WHEN** the owner changed the row's category
- **THEN** no habit label SHALL be shown

### Requirement: Weekly Question Rate
`GET /api/dashboard/question-rate?weeks=1..26` (default 8) SHALL report, per local week oldest first with the current one in progress, the owner's notification and capture expenses and incomes that are not movement legs or fees: recorded, asked, answered, recorded by memory, habit, AI or other rules, and owner corrections (of which on habit rows). Only real counts SHALL be reported and an out-of-range `weeks` SHALL be rejected with 422.

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

### Requirement: Seberapa Sering Ditanya Panel
Analisis SHALL show a "Seberapa sering ditanya" panel listing the weeks newest first with "Ditanya <n> dari <m> (<p>%)", the shares by memory, habit, AI, other rules and answered, and the corrections; a week without recorded payments SHALL say so, no data SHALL give an honest empty state, and a failed load SHALL show no figures.

#### Scenario: Weekly figures
- **WHEN** the current week has 20 recorded payments, 3 asked and 5 by habit
- **THEN** it SHALL be marked "berjalan" and show "Ditanya 3 dari 20 (15%)" and "Kebiasaan 5 (25%)"

#### Scenario: No data
- **WHEN** no notification or capture payment was recorded in the eight weeks
- **THEN** the panel SHALL say so and show no figures
