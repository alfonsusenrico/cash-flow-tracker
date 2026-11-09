# notification-record-and-flag Specification

## Purpose
Keep settled money in the ledger: record every notification whose amount, direction and owned account are
proven, flag classification doubts with a review status, hold only events whose money facts or account are
uncertain, and gate model classifications by confidence.

## Requirements

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

### Requirement: Incomplete Model Records Are Model Uncertainty
A model `record` that lacks a reference the backend requires (a category or observed account for an expense or income, distinct source and target for a movement, a direction, a description or amount evidence) SHALL be treated as model uncertainty. The system SHALL NOT write it, SHALL NOT ask the model again for that event, and SHALL continue as when the model answers `needs_review`. Only the deterministic path MAY record a row without a category.

#### Scenario: Model records without a category
- **GIVEN** AI processing is enabled and a settled myBCA text "You spent IDR 500000.00 at Payment." resolves to the owner's BCA account
- **WHEN** the model replies `record` with the BCA account and `category_id` null
- **THEN** the model is called once for the event
- **AND** the row SHALL be recorded from the deterministic interpretation with `review_status = needs_review` and the reason `model_fallback`
- **AND** no field of the row SHALL come from the model reply

#### Scenario: Model records without an observed account
- **WHEN** the model replies `record` for an expense with `account_id` null
- **THEN** the model is called once for the event and no field of a row SHALL come from the reply
- **AND** the deterministic path SHALL record the row with the reason `model_fallback` when it resolves the account, otherwise the event SHALL be held in `needs_review` with the backend's hold code

#### Scenario: Model proposes a movement with one endpoint
- **WHEN** the model replies `record` with direction `internal_movement` and `target_account_id` null
- **THEN** the reply SHALL NOT be written and the deterministic movement path SHALL record the movement only if it resolves both pockets from the notification, otherwise the event SHALL be held for review

#### Scenario: Incomplete record for an unresolved account name
- **GIVEN** the notification names a pocket that matches no account or alias
- **WHEN** the model replies `record` without the observed account
- **THEN** no row SHALL be created, no alias SHALL be learned, and the existing mapping confirmation or review SHALL apply

#### Scenario: Structural failure is still asked again once
- **WHEN** the model's first reply violates the sent schema and the second reply is a complete valid record
- **THEN** the row SHALL be recorded from the second reply, as before this change
