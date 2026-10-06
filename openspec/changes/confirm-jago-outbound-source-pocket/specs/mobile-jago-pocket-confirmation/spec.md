# Spec Delta

## Purpose

This capability lets the Android companion ask the owner to identify the Jago pocket used by an otherwise valid outbound transfer, with durable offline delivery and truthful pending state until the backend records the result.

## ADDED Requirements

### Requirement: Mobile source-pocket confirmation
The companion SHALL present a typed source-pocket question for a `needs_confirmation` result whose confirmation type is `source_pocket`.

#### Scenario: Notification opens the pocket chooser
- **WHEN** a source-pocket question is delivered for a Jago event
- **THEN** the notification SHALL identify the event/question and offer `Pilih kantong`
- **AND** activating it SHALL open the event detail with the current pocket options
- **AND** private notification content SHALL show only the bounded proven financial context allowed by the existing notification policy

#### Scenario: Full current Jago options are shown
- **WHEN** the detail view loads a source-pocket question
- **THEN** it SHALL display every current eligible pocket under the event's owned Jago parent, including the main pocket
- **AND** it SHALL exclude unrelated institutions, archived accounts, investment instruments, and another owner's accounts
- **AND** it SHALL show no preselected option

#### Scenario: Explicit selection enables confirmation
- **WHEN** the owner selects one eligible pocket
- **THEN** the UI SHALL enable confirmation for that selected account only
- **AND** confirming SHALL persist the event ID, question ID, stable reply ID, selected account ID, and pairing context before scheduling network delivery

#### Scenario: Offline confirmation resumes later
- **WHEN** the owner confirms using a cached eligible option while the device is offline
- **THEN** the app SHALL show a pending delivery state and retain the typed answer across process death and restart
- **AND** WorkManager SHALL retry network/server-unavailable failures with bounded backoff
- **AND** the app SHALL not submit a raw notification again merely to deliver the answer

#### Scenario: Stale or invalid option is rejected truthfully
- **WHEN** the server reports that the question or selected pocket is stale, archived, ineligible, or already answered differently
- **THEN** the app SHALL stop the stale reply, refresh the event/options, and show a corrective pending/review state
- **AND** it SHALL not show a recorded success alert or silently choose the main pocket

#### Scenario: Authoritative recording completes the flow
- **WHEN** the server accepts the answer but continues processing the event
- **THEN** the app SHALL show processing/pending state and continue existing completion polling
- **WHEN** the server returns a recorded result whose payload identity matches the local event
- **THEN** the app SHALL consume the result idempotently and show the normal recorded notification once

#### Scenario: Existing sender and account confirmations remain independent
- **WHEN** the companion receives a sender question or legacy account mapping proposal
- **THEN** it SHALL continue using its existing controls and payload validation
- **AND** a source-pocket answer SHALL never be encoded as a sender name or generic account alias

#### Scenario: Pairing and credential boundaries are preserved
- **WHEN** pairing changes, credentials are unavailable, or authentication fails while a source answer is queued
- **THEN** the app SHALL suspend or classify the answer using the existing delivery states
- **AND** it SHALL resume only under the matching pairing context after recovery
- **AND** logs SHALL contain delivery identifiers/statuses only, never raw notification text, account keys, or credentials
