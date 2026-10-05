# Spec Delta

## Purpose

Allow the owner to answer a pending sender question from an Android notification or the companion detail sheet, with durable delivery and accurate recording status.

## ADDED Requirements

### Requirement: Inline sender reply and explicit unknown action

The companion SHALL display supported sender questions with the masked sender, proven amount, receiving account, and actions `Balas nama` and `Catat tanpa nama`. The reply action SHALL open an inline text input and submit the name without requiring the app to be opened. Tapping the notification SHALL open the corresponding detail sheet, where equivalent name and unknown-sender controls SHALL be available. Financial details SHALL have a private lock-screen presentation. Account-selection questions SHALL retain their existing controls.

#### Scenario: Name supplied from notification
- **WHEN** the owner taps `Balas nama`, enters `Andra`, and sends
- **THEN** the reply is durably associated with that event and question and delivery starts without navigating to another app screen

#### Scenario: Record with no known name
- **WHEN** the owner taps `Catat tanpa nama`
- **THEN** an explicit unknown-sender answer is queued for that question without manufacturing a name

#### Scenario: Notification unavailable or dismissed
- **WHEN** notification permission is disabled or the owner dismisses the question notification
- **THEN** the pending event remains accessible and answerable in the app

### Requirement: Question-specific validation and routing

The companion SHALL validate supported question types and required fields before displaying or answering them. It SHALL bind each action to the event, question identifier, captured row, and originating server pairing. Separate simultaneous events and sequential questions for one event SHALL not overwrite or suppress each other's actions. A stale action SHALL not answer a newer question, and a credential/server change SHALL not send an old reply to the new pairing.

#### Scenario: Two simultaneous transfers
- **WHEN** two sender questions are displayed and each receives a different reply
- **THEN** each name is delivered to its intended event only

#### Scenario: Sequential account and sender questions
- **WHEN** account confirmation leads to a sender question for the same event
- **THEN** the sender question is displayed independently of the earlier account alert receipt

#### Scenario: Unknown or malformed question
- **WHEN** a result contains an unsupported question type or missing question identity/context
- **THEN** the app displays a safe recoverable protocol error and sends no guessed answer

#### Scenario: Pairing changes with an unsent reply
- **WHEN** the owner changes server or credentials before reply delivery
- **THEN** the old reply is retained but suspended for its original pairing and is not replayed under the new credentials

### Requirement: Durable offline reply delivery

The companion SHALL persist a reply before reporting it queued and SHALL recover its delivery after process restart or temporary network failure. Transport retries SHALL resend the same logical answer, not re-ingest the financial notification. Blank/invalid input SHALL remain correctable. Transient failures SHALL retry with bounded backoff; authentication failures SHALL wait for pairing recovery; conflicts or permanent validation failures SHALL stop automatic replay and expose a recoverable in-app state.

#### Scenario: Reply while offline
- **WHEN** the owner replies without connectivity and the app process subsequently restarts
- **THEN** the pending answer survives and is delivered under the original pairing after connectivity returns

#### Scenario: Backend accepted reply but response lost
- **WHEN** delivery loses its response after backend acceptance
- **THEN** the same answer is retried and the companion consumes the backend's idempotent current result

#### Scenario: Superseded question
- **WHEN** delivery returns a stale-question conflict
- **THEN** automatic replay stops, the latest event result is refreshed, and the owner can address its current state

### Requirement: Truthful status and completion

The companion SHALL distinguish a queued local reply, backend acceptance, processing, unresolved review, and committed recording. It SHALL display `Balasan menunggu koneksi` or an equivalent truthful pending state while offline and SHALL post a transaction success alert only for a complete authoritative `recorded` result. Normal completion polling and logical record alert deduplication SHALL remain in force. Repeated question lookup SHALL not deliberately generate repeated alerts, and unanswered events or undelivered replies SHALL be preserved by retention.

#### Scenario: Reply accepted for processing
- **WHEN** the backend accepts an answer and returns `queued` or `processing`
- **THEN** the app shows processing, schedules normal completion lookup, and does not claim the transaction is recorded

#### Scenario: Recording completed
- **WHEN** the backend returns a complete recorded income result
- **THEN** the app displays the confirmed name and original amount/account and delivers one logical transaction alert

#### Scenario: Sender answered but financial review still needed
- **WHEN** the accepted answer leads to an unresolved financial reference
- **THEN** the app shows the backend review state rather than a financial success alert

### Requirement: Restricted reply handling and safe upgrade

Reply actions SHALL be restricted to the companion's receiver and require device unlock before accepting a financial confirmation on the lock screen. Replies SHALL NOT be exposed through logs or backup exports. An app upgrade SHALL preserve pairing, existing captured events, completion and alert receipts, and unrelated local data, while adding durable answer storage. The detail controls SHALL retain usable labels, focus, touch targets, and layout with enlarged text.

#### Scenario: Locked device
- **WHEN** the owner attempts a sender reply or unknown-sender confirmation from a locked device
- **THEN** authentication is required before that financial confirmation is accepted

#### Scenario: Populated app upgrade
- **WHEN** the revised app upgrades the current populated companion database
- **THEN** pairing and original records are preserved, answer storage is added safely, and historical events are not automatically replayed or alerted

#### Scenario: Large text and long name
- **WHEN** the owner views the detail sheet with enlarged text and a long valid sender name
- **THEN** labels, input, pending/error states, and both actions remain readable and reachable
