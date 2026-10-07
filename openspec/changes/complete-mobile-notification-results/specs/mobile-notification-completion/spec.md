# Spec Delta

## Purpose

Keep the Android companion's stored notification lifecycle aligned with authoritative backend completion, with recoverable, privacy-preserving success alerts and no duplicate financial submission.

## ADDED Requirements

### Requirement: Durable Event Completion Tracking
The companion SHALL distinguish local capture, backend acceptance, processing, and committed recording.

#### Scenario: Required behavior and constraints
- **WHEN** a captured notification advances through acceptance and processing
- **THEN** the following required behavior and constraints hold:

  The companion SHALL distinguish local capture, backend acceptance, processing, and committed recording. It SHALL persist accepted event identifiers and states independently of transport success, correlate results by submitted payload hash, and preserve queued/processing/review/failed events through retention. Missing or invalid per-event results SHALL NOT make a submitted event appear recorded. Only accepted queued/processing events SHALL be automatically polled for completion; review/failed/ignored outcomes SHALL NOT trigger automatic backend retries or resubmission. Tracking SHALL be scoped to the server pairing that accepted the event.

#### Scenario: Queued acknowledgement
- **WHEN** ingestion returns a queued result and event identifier
- **THEN** the companion retains the event, schedules completion lookup, and posts no recorded alert

#### Scenario: Partial or reordered batch result
- **WHEN** a successful HTTP response omits one event or returns results in a different order
- **THEN** each supplied result updates only its matching captured event, and missing results remain visibly unresolved rather than being marked complete

#### Scenario: Restart or offline interval
- **WHEN** the app process restarts or loses network while an accepted event is processing
- **THEN** its persisted completion tracking resumes after connectivity returns without re-ingesting it merely to poll

#### Scenario: Changed server pairing
- **WHEN** the configured server or credentials change
- **THEN** previously accepted events are not polled or replayed against the new pairing, and their stored state is preserved

### Requirement: Verified Logical Record Alerts
The companion SHALL post a financial success alert only for a complete authoritative recorded result.

#### Scenario: Required behavior and constraints
- **WHEN** the companion receives a result that may represent a committed financial record
- **THEN** the following required behavior and constraints hold:

  The companion SHALL post a financial success alert only for a complete authoritative recorded result. Expense content SHALL identify its owned source; income SHALL identify its owned receiving target; movement SHALL identify both endpoints. Logical record keys SHALL identify alert deliveries separately from payload hashes. Repeated delivery and notifications sharing a movement key SHALL use the same active notification and persist deduplication independently of raw-event pruning. Updates to an already delivered logical record SHALL NOT deliberately produce another sound or recreate a dismissed alert.

#### Scenario: Asynchronous recording completes
- **WHEN** completion lookup returns a valid recorded result after a queued acknowledgement
- **THEN** the companion persists it and posts the concise verified description, endpoint information, amount, and currency

#### Scenario: Paired movement or redelivery
- **WHEN** two notification events resolve to the same logical record key, or a recorded event is redelivered
- **THEN** they do not create two active alerts, and persisted delivery state prevents deliberate repeat alerts

#### Scenario: Malformed recorded result
- **WHEN** a recorded result lacks a logical key, valid type, amount, currency, or the required owned endpoint
- **THEN** the companion retains a safe protocol error and does not substitute a zero amount or an invented expense alert

#### Scenario: Notification permission disabled
- **WHEN** recorded completion arrives while Android notifications are disabled
- **THEN** the record remains committed in local tracking, alert delivery is not marked successful, and any later permitted delivery uses the same logical key without re-ingestion

### Requirement: Safe Ledger Edit Identity
The companion SHALL keep backend event identifiers, logical record keys, and actual ledger transaction identifiers separate. Only a valid actual identifier for an ordinary editable record SHALL be sent to the transaction update endpoint. Movement/trade or unverified legacy key references SHALL NOT be edited as ordinary single-leg transactions. Editing captured labels SHALL NOT be presented as modifying a committed ledger record.

#### Scenario: Ordinary expense edit
- **WHEN** a committed ordinary result supplies a valid transaction identifier and the user edits it
- **THEN** the companion calls the ordinary ledger edit endpoint with that identifier, not the logical record key

#### Scenario: Logical key or movement edit
- **WHEN** the stored reference is a logical key or the result is a movement/trade
- **THEN** the companion does not issue an ordinary single-transaction PATCH and provides a truthful unavailable/edit-in-web state

### Requirement: Non-destructive Companion Upgrade
The companion SHALL preserve existing notification rows and configured credentials during upgrade. Historical synced rows SHALL NOT be replayed or retroactively alerted automatically. Existing UUID edit references and logical keys SHALL remain distinguishable; unresolved legacy references SHALL remain visible without fabricated event identifiers. Retention SHALL NOT remove events awaiting completion or alert delivery.

#### Scenario: Upgrade populated storage
- **WHEN** the revised companion upgrades a populated previous database
- **THEN** original raw fields, timestamps, hashes, labels, sync state, and credential configuration remain unchanged, with additive completion state initialized safely

#### Scenario: More than fifteen tracked records
- **WHEN** retained events include queued or review states beyond the existing latest-fifteen display window
- **THEN** pruning leaves those unresolved events intact and keeps logical alert deduplication independently

### Requirement: Private and Restricted Diagnostics
The companion SHALL exclude credentials and stored raw notification data from cloud backup and device-transfer exports. Operational logs/errors SHALL contain only safe metadata or bounded error codes, not message bodies, financial descriptions, API secrets, OTPs, arbitrary response bodies, or sensitive exception traces. Release builds SHALL expose no notification simulation or debug-record purge receiver; local debugging SHALL not grant unrelated apps the ability to inject or purge events.

#### Scenario: Noise or transport error logging
- **WHEN** an OTP/promo notification is dropped or an API response fails
- **THEN** logs contain no notification text, token, or raw backend response body

#### Scenario: Release manifest inspection
- **WHEN** the release manifest is built
- **THEN** no debug simulation/purge receiver is declared

#### Scenario: Backup or device transfer
- **WHEN** Android prepares cloud backup or device-transfer data
- **THEN** the actual credential preference file and raw-notification database are excluded
