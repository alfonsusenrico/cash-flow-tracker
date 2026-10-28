# notification-record-and-flag Specification

## Purpose
Keep settled money in the ledger: record every notification whose amount, direction and owned account are
proven, flag classification doubts with a review status, hold only events whose money facts or account are
uncertain, and gate model classifications by confidence.

## Requirements

### Requirement: Record Settled Notifications With a Review Flag
The system SHALL record a ledger row for a notification whose amount, currency, direction, settlement and owned account are proven, even when its category, name, pillar or movement pairing is uncertain. Such a row SHALL have `review_status = needs_review`, and the event SHALL keep the review reasons. The reasons are `uncertain_category`, `model_fallback`, `low_confidence`, `model_flagged`, `ambiguous_movement` and `purpose_question`.

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
- **AND** neither existing leg SHALL be linked.

#### Scenario: Automatic pairing clears classification doubt
- **WHEN** a new leg is linked automatically to a unique counterpart as a movement
- **THEN** both legs SHALL have `review_status = auto` unless the owner classified one of them.

### Requirement: Hold Events Whose Money Facts or Account Are Uncertain
The system SHALL NOT create a ledger row, and SHALL keep the event in `needs_review` or `needs_confirmation`, when the amount, currency, direction, settlement or timestamp is unproven, when the owned account cannot be resolved or needs the owner's mapping, source-pocket or sender confirmation, when the event may be a second report of an existing movement, or when the context is incomplete.

#### Scenario: Two amounts in one notification
- **WHEN** a notification contains two different transaction amounts
- **THEN** no row SHALL be created and the event SHALL be `needs_review` with `ambiguous_amount`.

#### Scenario: Unknown pocket name
- **GIVEN** the notification names a pocket that matches no account or alias
- **WHEN** it is processed
- **THEN** no row SHALL be created and the existing mapping confirmation or review SHALL apply.

#### Scenario: Jago transfer without source pocket
- **WHEN** a Jago outbound transfer without a source pocket is ingested
- **THEN** the existing source-pocket confirmation SHALL apply and no row SHALL be created before the answer.

#### Scenario: Possible second report of a movement
- **WHEN** a leg may confirm an existing complete movement but the match is not unique
- **THEN** no row SHALL be created and the event SHALL be `needs_review` with `ambiguous_existing_movement`.

### Requirement: Confidence Gate for Model Classifications
A row recorded from a model interpretation SHALL have `review_status = needs_review` with the reason `low_confidence` when the model's confidence is below the configured threshold, and with the reason `model_flagged` when the model's review reason is not `none`. The threshold SHALL default to 0.8, SHALL be configurable between 0.5 and 1.0, and an invalid value SHALL disable AI processing with `processor_configuration_invalid`.

#### Scenario: Confident model classification
- **WHEN** the model records a named merchant with confidence 0.92
- **THEN** the row SHALL have `review_status = auto` and `classification_confidence = 0.92`.

#### Scenario: Unsure model classification
- **WHEN** the model records a row with confidence 0.6
- **THEN** the row SHALL be recorded with `review_status = needs_review` and the reason `low_confidence`.

#### Scenario: Invalid threshold setting
- **WHEN** `NOTIFICATION_REVIEW_CONFIDENCE_THRESHOLD` is `1.5`
- **THEN** the AI configuration error SHALL be `processor_configuration_invalid`.
