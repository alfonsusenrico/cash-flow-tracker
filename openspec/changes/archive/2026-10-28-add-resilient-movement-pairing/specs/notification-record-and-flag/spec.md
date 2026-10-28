## MODIFIED Requirements

### Requirement: Record Settled Notifications With a Review Flag
The system SHALL record a ledger row for a notification whose amount, currency, direction, settlement and owned account are proven, even when its category, name, pillar or movement pairing is uncertain. Such a row SHALL have `review_status = needs_review`, and the event SHALL keep the review reasons. The reasons are `uncertain_category`, `model_fallback`, `low_confidence`, `model_flagged`, `ambiguous_movement`, `awaiting_counterpart` and `purpose_question`.

#### Scenario: Deterministic category is uncertain
- **GIVEN** AI processing is disabled and a settled myBCA text "You spent IDR 187,500.00 at Groceries." resolves to the owner's BCA account, but no category matches "Groceries"
- **WHEN** the notification is ingested
- **THEN** an expense of Rp 187.500 SHALL be recorded on the BCA account without a category and with `review_status = needs_review`
- **AND** the event SHALL be `recorded` with the reason `uncertain_category`.

#### Scenario: Model asks for review but the deterministic path resolves
- **GIVEN** a settled notification whose account and category the deterministic path resolves
- **WHEN** the model answers `needs_review`
- **THEN** the row SHALL be recorded from the deterministic interpretation with `review_status = needs_review` and the reason `model_fallback`.

#### Scenario: Model output unusable
- **WHEN** the model's answer fails validation twice and the deterministic path resolves the account
- **THEN** the row SHALL be recorded with `review_status = needs_review` and the reason `model_fallback`.

#### Scenario: Ambiguous counterpart
- **GIVEN** two recorded legs that are both compatible counterparts of a new leg
- **WHEN** the new leg is recorded
- **THEN** it SHALL be recorded unpaired with `review_status = needs_review` and the reason `ambiguous_movement`
- **AND** neither existing leg SHALL be linked
- **AND** each existing leg still classified automatically SHALL also be flagged `needs_review` with the reason `ambiguous_movement`.

#### Scenario: Automatic pairing clears classification doubt
- **WHEN** a new leg is linked automatically to a unique counterpart as a movement, immediately or by the re-pairing pass
- **THEN** both legs SHALL have `review_status = auto` unless the owner classified one of them
- **AND** an open purpose question on either leg's event SHALL be closed.

#### Scenario: One-sided self-transfer awaits its counterpart
- **GIVEN** a GoPay notification "Rp500.000 udah dikirim ke BCA Raka Purnama." from the owner Raka Purnama and no matching BCA credit
- **WHEN** the notification is ingested
- **THEN** a Rp 500.000 GoPay expense SHALL be recorded in "Internal Movement", unlinked, with `review_status = needs_review` and the reason `awaiting_counterpart`, without a purpose question
- **AND** it SHALL NOT count as spending or income in the reports
- **AND** it SHALL remain a candidate for automatic pairing.
