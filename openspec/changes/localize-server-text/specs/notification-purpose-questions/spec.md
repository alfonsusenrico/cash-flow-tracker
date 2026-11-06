## MODIFIED Requirements

### Requirement: Purpose Question for Merchant-Less Notifications
When a notification is recorded as a single unpaired expense or income with a signature key, the system SHALL attach an open purpose question (new `question_id`, transaction id, a prompt in the account's language naming amount and account, up to five suggestions, free text allowed) and flag the row `needs_review` with the reason `purpose_question`. Named merchants, self or owned-account transfers, movements, memory hits and payments recorded from a habit (habit-learning) SHALL NOT get one.

#### Scenario: myBCA category-only payment
- **WHEN** "You spent IDR 187,500.00 at Groceries." is recorded
- **THEN** the event SHALL have an open purpose question whose prompt starts with "Apa ini?" and names Rp187.500.

#### Scenario: myBCA payment for an English account
- **WHEN** the same notification is recorded for an English account
- **THEN** the prompt SHALL start with "What was this for?", name Rp187.500, and carry its code and parameters.

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
