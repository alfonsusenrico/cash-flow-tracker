# mobile-notification-completion Specification

## Purpose
Keep the Android companion's stored notification lifecycle aligned with authoritative backend completion, with recoverable, privacy-preserving success alerts and no duplicate financial submission.

## Requirements

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

### Requirement: Retention keeps unresolved events
Retention SHALL preserve events whose state is queued, processing, needs review or failed.

#### Scenario: Pruning while an event needs review
- **WHEN** retention runs while an accepted event needs review and many newer events are recorded
- **THEN** the event and its stored state remain

### Requirement: Only accepted in-progress events are polled automatically
Only accepted queued or processing events, events awaiting confirmation, and recorded events whose purpose question is still open SHALL be polled automatically for completion. Review, failed and ignored outcomes SHALL NOT trigger scheduled backend lookups, retries or resubmission; recent review and failed results are only re-read once each time the app opens.

#### Scenario: Review outcome
- **WHEN** completion lookup returns a needs-review result
- **THEN** no further automatic lookup is scheduled and the event is not resubmitted, and the owner can still retry it by hand

#### Scenario: Recorded event with an open purpose question
- **WHEN** completion lookup returns a recorded result that carries a valid open purpose question
- **THEN** a further lookup is scheduled, and it stops once a result no longer carries the question

#### Scenario: Event awaiting confirmation
- **WHEN** completion lookup returns a `needs_confirmation` result with a valid question
- **THEN** a further lookup is scheduled, and it stops once a result is no longer awaiting that confirmation

#### Scenario: Recorded event without a question
- **WHEN** completion lookup returns a recorded result without a purpose question
- **THEN** no further automatic lookup is scheduled

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

### Requirement: Open confirmations are re-checked until they close
While a row awaits confirmation (an account-mapping, sender or Jago source-pocket question), the companion SHALL keep one result check scheduled on the same schedule as an open purpose question: 2 minutes, 10 minutes, 30 minutes, 2 hours and 6 hours after successive results that show the same question, then every 12 hours, none later than 7 days after capture. A different question SHALL start the schedule over.

#### Scenario: Source-pocket question stored
- **WHEN** a `needs_confirmation` result with a valid Jago source-pocket question is stored
- **THEN** its confirmation notification is posted once and the next result check is scheduled 2 minutes later

#### Scenario: Same confirmation still open
- **WHEN** a check returns the same confirmation question
- **THEN** no second confirmation alert is posted and the next check is 10 minutes later

#### Scenario: Backend asks a new confirmation question
- **WHEN** a check returns a confirmation with a different question ID
- **THEN** the next check is 2 minutes later

### Requirement: A confirmation settled elsewhere clears it on the phone
When a refreshed result for a row awaiting confirmation is `recorded`, `ignored`, or no longer the same question, the companion SHALL store that result, cancel the confirmation notification, drop cached source-pocket choices for the old question, and deliver any recorded alert exactly as after an answer given on the phone.

#### Scenario: Jago source pocket confirmed on the server
- **WHEN** a follow-up check of a row awaiting a source-pocket confirmation returns the recorded movement
- **THEN** the row is `recorded` with no next check and no cached pocket choices, the source-pocket notification is gone, and the recorded-transaction alert is posted

#### Scenario: Event ignored on the server
- **WHEN** a check of a row awaiting confirmation returns `ignored`
- **THEN** the row is `ignored`, its checks stop, and the confirmation notification is cancelled

### Requirement: A failed re-check leaves an open question unchanged
A failed result check of a row with an open purpose question or an open confirmation SHALL NOT change the row's status, error, question or stored result. A network failure, 429 or 5xx SHALL move the check one step along the schedule; any other failure SHALL stop the checks until the next refresh.

#### Scenario: Server unavailable during a follow-up
- **WHEN** a follow-up check of a recorded row with an open purpose question gets 503
- **THEN** the row stays `recorded` with its question and the next check moves one step along the schedule

#### Scenario: Credentials rejected during a refresh
- **WHEN** a refresh of a recorded row with an open purpose question gets 401
- **THEN** the row stays `recorded` with its question, shows no error, and has no next check

#### Scenario: Credentials rejected while a confirmation is open
- **WHEN** a check of a row awaiting a source-pocket confirmation gets 401
- **THEN** the row stays `needs_confirmation` with its question and its error code, and has no next check

### Requirement: Opening the app checks open questions
When the app comes to the foreground with a server address and API key set, the companion SHALL at once check the result of every row of the current pairing that has an open question (a recorded row with an open purpose question, answered on the phone or not, or a row awaiting confirmation) and no other row. It SHALL run whatever the auto-sync switch says: it sends no capture and only reads results of accepted events.

#### Scenario: App opened with open and settled rows
- **WHEN** the app opens with one unanswered purpose question, one purpose question with a queued answer, one row awaiting confirmation, a recorded row without a question, a queued row, a needs-review row, and a purpose question from another pairing
- **THEN** exactly the three open questions of the current pairing are checked

#### Scenario: App opened without credentials
- **WHEN** the app opens while no API key is set
- **THEN** no result is checked

### Requirement: Opening the app re-reads recent review results
When the app comes to the foreground with a server address and API key set, the companion SHALL re-read once the result of at most 50 rows, newest first, that are `needs_review` or `failed`, belong to the current pairing, have a backend event, were captured in the last 30 days, have no owner label set on the phone and no unsent reply. The refreshed result SHALL be applied as any other result.

#### Scenario: Noise ignored on the server later
- **WHEN** the app opens and a row stored as `needs_review` now has an `ignored` result on the server
- **THEN** the row becomes `ignored` and leaves the main list and the review filter like any other ignored row

#### Scenario: Review settled as a record on the server
- **WHEN** the app opens and a row stored as `needs_review` now has a recorded result
- **THEN** the row becomes `recorded` with that result, and its record alert follows the usual alert rules

#### Scenario: Server outcome unchanged
- **WHEN** the re-read returns the same `needs_review` result
- **THEN** the row keeps its status and error code, and no further lookup is scheduled

#### Scenario: Rows that are not re-read
- **WHEN** the app opens with review rows that are older than 30 days, belong to another pairing, carry an owner label, or have an unsent reply
- **THEN** those rows are not re-read

#### Scenario: More than fifty review rows
- **WHEN** 55 review rows qualify
- **THEN** only the 50 newest are re-read

### Requirement: A failed re-read leaves a review row as it is
A failed result read of a `needs_review` or `failed` row, whether 401, 403, 404, 429, 5xx or a network error, SHALL NOT change its status, error code, next check or attempt count.

#### Scenario: Credentials rejected while re-reading a review row
- **WHEN** the re-read of a `needs_review` row gets 401, 403, 404 or 503
- **THEN** the row stays `needs_review` with its original error code and no scheduled lookup

### Requirement: Refused account confirmations from the notification stay answerable

When an account-proposal confirmation sent from its notification does not go through, because the server refuses it, the network is unavailable, the result is invalid, or the question changed, the companion SHALL show the event's open proposal again in the same slot with its actions and a statement that the confirmation was not saved. When the event no longer awaits that confirmation, it SHALL show only the message.

#### Scenario: Server unavailable for a confirmation
- **WHEN** the owner taps the proposed account in the notification and the server answers 503
- **THEN** the event stays awaiting confirmation and the notification still offers the proposed account and "Pilih lain"

#### Scenario: Confirmation while offline
- **WHEN** the owner confirms from the notification without a connection
- **THEN** the notification still offers the confirmation and says it was not saved, and a later lookup leaves it answerable

#### Scenario: Confirmation for a changed question
- **WHEN** a confirmation for an earlier question of the event arrives
- **THEN** nothing is sent and the event's open proposal is offered with its actions

### Requirement: Account confirmation attempts are traced privately

The companion SHALL record each step of every account confirmation attempt from the notification or the app with the same private, bounded reason-code record that sender answers use, with the answer kind `account`. Tanya SHALL state that a confirmation from the notification was not saved while the newest settled attempt for the shown proposal is a refused notification confirmation.

#### Scenario: Refused confirmation is traced
- **WHEN** a confirmation from the notification receives a 503
- **THEN** the record holds a received step and a `server_refused` step for that event's question

#### Scenario: Tanya shows an unsaved confirmation
- **WHEN** the owner opens Tanya on an account proposal whose newest settled attempt is a refused notification confirmation
- **THEN** the card says the confirmation from the notification was not saved and keeps the account choice available
