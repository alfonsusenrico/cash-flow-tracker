# notification-account-mapping Specification

## Purpose
Map bank-notification account and pocket names that the backend cannot resolve by name to the owner's accounts using AI proposals with confidence, automatic acceptance above a threshold, learned aliases, and owner confirmation below it.

## ADDED Requirements

### Requirement: AI-Proposed Mapping for Unresolved Names
When a candidate notification names an account or pocket that neither a learned alias nor name matching resolves, the system SHALL ask the AI to propose one eligible owned account for each unresolved name with a mapping confidence between 0 and 1. An eligible account SHALL be active, owned, and liquid; for an institution-level name it SHALL belong to that institution's account hierarchy or share a word with the bank text. Amounts, directions, and evidence SHALL remain backend-proven.

#### Scenario: Proposal limited to eligible accounts
- **WHEN** the AI proposes an investment position or another owner's account for "GoPay Tabungan"
- **THEN** the proposal is rejected and the event is `needs_review`

#### Scenario: No proposal
- **WHEN** the AI returns no mapping for an unresolved name
- **THEN** the event is `needs_review` with an uncertain-mapping reason, as today

### Requirement: Automatic Acceptance Above Threshold
The system SHALL record the event with the proposed accounts when the lowest mapping confidence in the event is at or above the configured threshold (default 0.85), SHALL label the result as automatically mapped, and SHALL remember each name → account pair as an AI-learned alias.

#### Scenario: Confident mapping of a renamed pocket
- **WHEN** Jago reports a move "to your GoPay Tabungan Pocket", no account has that name, and the AI maps it to the owner's "GoPay" account with confidence 0.93
- **THEN** a movement from Main Pocket to GoPay is recorded, the result says it was mapped automatically, and the next such notification maps without an AI mapping call

### Requirement: Owner Confirmation Below Threshold
When the lowest mapping confidence is below the threshold, the system SHALL create no transaction, SHALL set the event to `needs_confirmation`, and SHALL expose the proposal (bank name, proposed account, confidence, up to three alternatives) in the event result. Confirming or correcting the account SHALL store an owner alias and record the event through the normal path, including movement pairing. Confirmation SHALL be owner-scoped and SHALL be rejected for events not awaiting confirmation.

#### Scenario: Low-confidence mapping awaits one tap
- **WHEN** the AI maps "Tabungan Liburan" to "Dana Darurat" with confidence 0.55
- **THEN** the event is `needs_confirmation` with that proposal and no transaction exists

#### Scenario: Owner picks an alternative
- **WHEN** the owner confirms a different eligible account for the same event
- **THEN** the event is recorded with that account and later notifications naming "Tabungan Liburan" map to it without confirmation

#### Scenario: Confirming someone else's event
- **WHEN** a user confirms a mapping for an event they do not own
- **THEN** the backend returns not found and reveals nothing

### Requirement: Learned Names as Mapping Evidence
The system SHALL give the AI, for each account, the bank names previously mapped to it and whether the owner confirmed each one or it was accepted automatically. The AI SHALL treat an owner-confirmed name for the same product or pocket as strong evidence for a similar unresolved name, SHALL treat an automatically accepted name as weaker evidence, and SHALL NOT treat a shared generic word as evidence. Owner names in learned names SHALL be redacted like other context.

#### Scenario: Similar name after an owner confirmation
- **WHEN** the owner confirmed "GoPay Tabungan" as GoPay and a later notification names "Tabungan GoPay"
- **THEN** the AI maps it to GoPay with confidence at or above the threshold and it is recorded automatically

#### Scenario: Only a generic word in common
- **WHEN** the owner confirmed "Tabungan Rumah" for one pocket and a notification names "Tabungan Liburan"
- **THEN** the learned name does not raise confidence above the threshold

### Requirement: Learned Alias Management
The owner SHALL be able to list learned aliases with their source (AI or owner) and remove any of them. Removing an alias SHALL affect only future notifications.

#### Scenario: Removing a wrong automatic alias
- **WHEN** the owner removes the alias "GoPay Tabungan → GoPay"
- **THEN** the next notification naming "GoPay Tabungan" goes through proposal and threshold again, and recorded transactions are unchanged

### Requirement: Compatibility With Older Companion Builds
The system SHALL emit `needs_confirmation` only to companion builds that declare support; older builds SHALL receive `needs_review` for the same situation.

#### Scenario: Old phone build
- **WHEN** a low-confidence mapping occurs for an event submitted by companion 1.2.0
- **THEN** its result status is `needs_review`
