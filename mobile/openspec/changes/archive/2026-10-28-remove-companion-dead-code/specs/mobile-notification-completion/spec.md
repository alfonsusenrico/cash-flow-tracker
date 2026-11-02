## MODIFIED Requirements

### Requirement: Durable Event Completion Tracking
The companion SHALL distinguish local capture, backend acceptance, processing, and committed recording. It SHALL persist accepted event identifiers and states independently of transport success, and correlate results by submitted payload hash. Missing or invalid per-event results SHALL NOT make a submitted event appear recorded.

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

## ADDED Requirements

### Requirement: Retention keeps unresolved events
Retention SHALL preserve events whose state is queued, processing, needs review or failed.

#### Scenario: Pruning while an event needs review
- **WHEN** retention runs while an accepted event needs review and many newer events are recorded
- **THEN** the event and its stored state remain

### Requirement: Only accepted in-progress events are polled automatically
Only accepted queued or processing events SHALL be polled automatically for completion. Review, failed and ignored outcomes SHALL NOT trigger automatic backend retries or resubmission.

#### Scenario: Review outcome
- **WHEN** completion lookup returns a needs-review result
- **THEN** no further automatic lookup is scheduled and the event is not resubmitted, and the owner can still retry it by hand

### Requirement: Completion tracking belongs to the accepting pairing
Completion tracking SHALL be scoped to the server pairing that accepted the event.

#### Scenario: Due lookup under another pairing
- **WHEN** an event accepted under one server pairing is due for a lookup while another pairing is configured
- **THEN** it is not looked up with the other pairing's credentials and keeps its stored state

### Requirement: Alerts name the owned accounts
An expense alert SHALL identify its owned source account, an income alert its owned receiving account, and a movement alert both endpoints.

#### Scenario: Movement alert
- **WHEN** a recorded movement from "Contoh Pocket" to "Kantong Utama" is alerted
- **THEN** the alert names both pockets

### Requirement: Alert delivery is deduplicated by logical record key
Logical record keys SHALL identify alert deliveries separately from payload hashes. Repeated delivery and notifications sharing a movement key SHALL use the same active notification, and the deduplication SHALL persist independently of raw-event pruning.

#### Scenario: Redelivery after pruning
- **WHEN** a delivered record's raw notification rows have been pruned and the same record key is delivered again
- **THEN** no second alert is posted

### Requirement: Updates to a delivered record stay quiet
Updates to an already delivered logical record SHALL NOT deliberately produce another sound or recreate a dismissed alert.

#### Scenario: Refresh after dismissal
- **WHEN** the owner has dismissed a record's alert and its result is refreshed
- **THEN** no alert reappears and no sound plays
