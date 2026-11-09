## ADDED Requirements

### Requirement: Incomplete Model Records Are Model Uncertainty
A model `record` that lacks a reference the backend requires (a category or observed account for an expense or income, distinct source and target for a movement, a direction, a description or amount evidence) SHALL be treated as model uncertainty. The system SHALL NOT write it, SHALL NOT ask the model again for that event, and SHALL continue as when the model answers `needs_review`. Only the deterministic path MAY record a row without a category.

#### Scenario: Model records without a category
- **GIVEN** AI processing is enabled and a settled myBCA text "You spent IDR 500000.00 at Payment." resolves to the owner's BCA account
- **WHEN** the model replies `record` with the BCA account and `category_id` null
- **THEN** the model is called once for the event
- **AND** the row SHALL be recorded from the deterministic interpretation with `review_status = needs_review` and the reason `model_fallback`
- **AND** no field of the row SHALL come from the model reply

#### Scenario: Model records without an observed account
- **WHEN** the model replies `record` for an expense with `account_id` null and the deterministic path cannot resolve the account
- **THEN** no row SHALL be created and the event SHALL be held in `needs_review` with the backend's hold code

#### Scenario: Model proposes a movement with one endpoint
- **WHEN** the model replies `record` with direction `internal_movement` and `target_account_id` null
- **THEN** the reply SHALL NOT be written and the deterministic movement path SHALL record the movement only if it resolves both pockets, otherwise the event SHALL be held for review

#### Scenario: Incomplete record for an unresolved account name
- **GIVEN** the notification names a pocket that matches no account or alias
- **WHEN** the model replies `record` without the observed account
- **THEN** no row SHALL be created and the existing mapping confirmation or review SHALL apply

#### Scenario: Structural failure is still asked again once
- **WHEN** the model's first reply violates the sent schema and the second reply is a complete valid record
- **THEN** the row SHALL be recorded from the second reply, as before this change
