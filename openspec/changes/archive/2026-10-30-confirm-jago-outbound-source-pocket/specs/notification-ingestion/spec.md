# Spec Delta

## MODIFIED Requirements

### Requirement: Bank Jago Child Pocket Transfer Resolution
The system SHALL resolve Jago movement endpoints from the user's active registered accounts and child pockets under the same Jago parent.

#### Scenario: Required behavior and constraints
- **WHEN** a Jago internal movement notification is ingested
- **THEN** the following required behavior and constraints hold:

  The system SHALL resolve Jago movement endpoints from the user's active registered accounts and child pockets under the same Jago parent. Exact and normalized names, including redundant `Pocket`/`Kantong` terms and supported aliases, SHALL be considered; substring or semantic matching SHALL NOT resolve an ambiguous name arbitrarily. An implicit main endpoint SHALL resolve only to that parent's explicitly configured valid default pocket, uniquely identified registered main pocket, or valid parent main balance. Unknown named pockets SHALL NOT be created or replaced with a parent fallback. A verified complete movement SHALL atomically create the canonical linked expense/income pair, preserving amount and original timestamp, and apply the canonical movement Kakeibo rule rather than a legacy `transfer` transaction type. The processor SHALL NOT wait for a second notification for an explicit Jago pocket movement. A verified settled Jago outbound transfer without source-pocket evidence SHALL enter the source-pocket confirmation flow before any Jago balance or movement record is written; the owner's choice SHALL be scoped to that event, and an unanswered, stale, invalid, or conflicting choice SHALL NOT fall back to the main pocket.

#### Scenario: Moving funds between Main Pocket and GoPay Tabungan Pocket
- **WHEN** Jago reports `Rp500.000 has been moved from your Main Pocket Pocket to your GoPay Tabungan Pocket` and both endpoints resolve uniquely within the registered Jago hierarchy
- **THEN** the backend records exactly one linked bilateral movement for 500000 IDR between the effective main endpoint and the registered GoPay Tabungan pocket

#### Scenario: Moving funds between custom pockets
- **WHEN** Jago reports movement from the registered Jajan pocket to the registered Tabungan pocket for 50000 IDR
- **THEN** both records share a movement identifier, both endpoints belong to the same Jago parent, and the account balances reflect only that one movement

#### Scenario: Rejecting an unknown named pocket
- **WHEN** a named Jago movement endpoint is absent or ambiguous in the user's registered hierarchy
- **THEN** no liquid pocket is created and no guessed movement is recorded
- **AND** the event waits for an account-mapping confirmation or review

#### Scenario: Unnamed outbound transfer is held before recording
- **WHEN** a verified Jago notification says the owner transferred Rp125.000 to an external recipient and contains no source-pocket evidence
- **THEN** the event SHALL be `needs_confirmation` with a `source_pocket` question
- **AND** the question SHALL carry the event question ID, proven amount/currency and Jago parent context, and `GET /api/ingest/notifications/{event_id}/source-pockets` SHALL list every current eligible Jago pocket
- **AND** no transaction, movement, reconciliation flag, or consumed counterpart role SHALL be written for the Jago event
- **AND** no pocket SHALL be preselected

#### Scenario: Owner selects a non-main pocket for an unnamed transfer
- **WHEN** the owner submits a valid event question ID, unique reply ID, and the active Jago child pocket "Dana Darurat"
- **THEN** the answer SHALL be accepted idempotently and the event SHALL resume normal processing
- **AND** the recorded expense SHALL use "Dana Darurat" as its account
- **AND** the answer SHALL not change Jago defaults or aliases used by a later unnamed transfer

#### Scenario: Unnamed transfer is paired only after source confirmation
- **WHEN** an independently observed BCA leg with the same proven transfer evidence arrives before the owner answers the Jago source question
- **THEN** the BCA event MAY record its own observed leg through its normal path
- **AND** the Jago event SHALL remain unrecorded and SHALL not create or link a bilateral movement
- **WHEN** the owner subsequently selects the eligible Jago source
- **THEN** the system SHALL pair the two legs only if all existing uniqueness, timing, reference, and ownership checks pass
- **AND** the movement source SHALL be the selected Jago pocket

#### Scenario: Source answer is stale or no longer eligible
- **WHEN** a reply references a superseded question, another owner's event, an archived pocket, a non-Jago account, or a pocket outside the event's owner
- **THEN** the API SHALL reject it with a bounded conflict or authorization response
- **AND** the event SHALL remain pending or enter review
- **AND** the system SHALL not debit the main pocket or any other account

#### Scenario: Existing named Jago movement remains automatic
- **WHEN** a Jago notification explicitly names an active source pocket and target pocket
- **THEN** the existing exact or normalized name matching SHALL continue to resolve those named endpoints
- **AND** the new source-pocket question SHALL not be shown
