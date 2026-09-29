# mobile-notification-review Specification

## Purpose
The companion app lets the owner understand and resolve backend processing outcomes for captured notifications without leaving the phone.

## ADDED Requirements

### Requirement: Human-Readable Processing Reasons
The companion app SHALL display an Indonesian explanation for every backend `error_code` on events in `needs_review` or `failed`, distinguishing automatic retry (`failed`) from owner review (`needs_review`), and SHALL fall back to showing the raw code for unknown values.

#### Scenario: Model uncertainty is explained
- **WHEN** an event's backend result is `needs_review` with `error_code = model_uncertain`
- **THEN** the inbox badge reads "Perlu tinjau" and the detail sheet shows an explanation that the automatic interpreter could not decide and offers retry or manual recording

#### Scenario: Provider outage is shown as retrying
- **WHEN** an event's backend result is `failed` with `error_code = provider_unavailable`
- **THEN** the badge reads "Diulang otomatis" and the detail sheet explains the server will retry without owner action

### Requirement: Owner Retry From the Companion App
The companion app SHALL offer a Retry action for owned events in `needs_review` or `failed` that calls the backend retry endpoint, and SHALL reflect the returned state and schedule completion polling.

#### Scenario: Retrying a reviewable event
- **WHEN** the owner taps Retry on a `needs_review` event
- **THEN** the app calls the retry endpoint, updates the local status to the returned state, and polls for completion

#### Scenario: Retry rejected by the backend
- **WHEN** the backend rejects the retry (for example AI processing unavailable)
- **THEN** the app keeps the previous state and shows the rejection reason

### Requirement: Manual Recording of Unresolved Events
The companion app SHALL let the owner record an unresolved event as an expense or income by choosing an owned liquid account and a category of matching kind, using the backend resolve endpoint, and SHALL then treat the event as recorded with an editable transaction identity.

#### Scenario: Manually recording a stuck event
- **WHEN** the owner selects account and category for a `needs_review` event and confirms
- **THEN** the app calls the resolve endpoint with the event's amount, the event becomes `recorded`, and a confirmation alert is delivered like an automatic record

#### Scenario: Manual recording is refused for resolved events
- **WHEN** the event is already `recorded` or `ignored`
- **THEN** the manual record action is not offered
