## MODIFIED Requirements

### Requirement: Purpose Question for Merchant-Less Notifications
When a notification is recorded as a single unpaired expense or income with a signature key, the system SHALL attach an open purpose question (new `question_id`, transaction id, Indonesian prompt naming amount and account, up to five suggestions, free text allowed) and flag the row `needs_review` with the reason `purpose_question`. Named merchants, self or owned-account transfers, movements, memory hits and payments recorded from a habit (habit-learning) SHALL NOT get one.

#### Scenario: myBCA category-only payment
- **WHEN** "You spent IDR 187,500.00 at Groceries." is recorded
- **THEN** the event SHALL have an open purpose question whose prompt starts with "Apa ini?" and names Rp187.500.

#### Scenario: Masked incoming transfer after the sender flow
- **GIVEN** the owner named the masked sender earlier
- **WHEN** a transfer from that mask is recorded as income
- **THEN** a purpose question SHALL be attached.

#### Scenario: Named merchant
- **WHEN** "You've paid Rp125.000 to Kedai Awan" is recorded
- **THEN** no purpose question SHALL be attached.

#### Scenario: Leg paired as movement
- **WHEN** a merchant-less leg is paired automatically with its counterpart
- **THEN** no purpose question SHALL remain open on either event.

#### Scenario: Recorded from a habit
- **GIVEN** three agreeing owner answers for Groceries payments of about Rp 12.000 on BCA Harian
- **WHEN** another Rp 12.000 Groceries payment on BCA Harian is recorded
- **THEN** no purpose question SHALL be attached and the row SHALL NOT be flagged `needs_review`.

### Requirement: Ranked Purpose Suggestions
Suggestions SHALL start with the habit's guess when the payment has habit labels (habit-learning), then rank the owner's feedback of the same direction with an active category by signature, account, amount band, local hour and weekday, as scored in the design, summing identical classifications and leaving out the one the guess already offers. Then the current classification SHALL follow when it has a category, and incoming transfers SHALL include a "Patungan & Ganti Uang" suggestion when the owner has that category. At most five suggestions SHALL be offered.

#### Scenario: Time and amount ranking
- **GIVEN** owner answers "makan kafe senja" twice on Wednesday evenings around Rp200.000 and "beli roti bakar" three times on Monday mornings around Rp30.000, all for the same signature
- **WHEN** a Rp187.500 payment with that signature is recorded on a Wednesday at 19:00
- **THEN** the first suggestion SHALL be the habit's guess "Sepertinya Jajan & Nongkrong?" with notes "makan kafe senja", followed by "beli roti bakar".

#### Scenario: Repayment suggestion for incoming transfer
- **WHEN** a masked incoming transfer gets a question and the owner has "Patungan & Ganti Uang"
- **THEN** one suggestion SHALL use that category.

#### Scenario: No history
- **WHEN** the owner has no feedback yet
- **THEN** the suggestions SHALL contain only the current classification and, for incoming transfers, the repayment suggestion.
