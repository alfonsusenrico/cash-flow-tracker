# mobile-sender-replies Specification

## Purpose
Allow the owner to answer a pending sender question from an Android notification or the companion's Tanya screen, with durable delivery and accurate recording status.

## Requirements

### Requirement: Inline sender reply and explicit unknown action

The companion SHALL display supported sender questions with the masked sender, proven amount, receiving account, and actions `Balas nama` and `Catat tanpa nama`. The reply action SHALL open an inline text input and submit the name without requiring the app to be opened.

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

The companion SHALL persist a reply before reporting it queued and SHALL recover its delivery after process restart or temporary network failure. Transport retries SHALL resend the same logical answer, not re-ingest the financial notification.

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

The companion SHALL distinguish a queued local reply, backend acceptance, processing, unresolved review, and committed recording, and SHALL post a transaction success alert only for a complete authoritative `recorded` result. Normal completion polling and logical record alert deduplication SHALL remain in force.

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

Reply actions SHALL be restricted to the companion's receiver and require device unlock before accepting a financial confirmation on the lock screen. Replies SHALL NOT be exposed through logs or backup exports. An app upgrade SHALL preserve pairing, existing captured events, completion and alert receipts, and unrelated local data, while adding durable answer storage. The question controls in Tanya SHALL retain usable labels, focus, touch targets, and layout with enlarged text.

#### Scenario: Locked device
- **WHEN** the owner attempts a sender reply or unknown-sender confirmation from a locked device
- **THEN** authentication is required before that financial confirmation is accepted

#### Scenario: Populated app upgrade
- **WHEN** the revised app upgrades the current populated companion database
- **THEN** pairing and original records are preserved, answer storage is added safely, and historical events are not automatically replayed or alerted

#### Scenario: Large text and long name
- **WHEN** the owner views a sender question in Tanya with enlarged text and a long valid sender name
- **THEN** labels, input, pending/error states, and both actions remain readable and reachable

### Requirement: The detail sheet answers sender questions

Tapping a sender question notification SHALL open that event's question in Tanya, below the original notification, where equivalent name and unknown-sender controls SHALL be available. The event's detail page SHALL link to the same question.

#### Scenario: Notification body tapped
- **WHEN** the owner taps the body of a sender question notification
- **THEN** the app opens Tanya at that event's question with a name field and a `Catat tanpa nama` action

### Requirement: Sender questions are private on the lock screen

The financial details of a sender question notification SHALL have a private lock-screen presentation.

#### Scenario: Locked device shows the question
- **WHEN** a sender question is shown on a locked device
- **THEN** the lock-screen version hides the masked sender, amount and account

### Requirement: Account-selection questions keep their controls

Account-selection questions SHALL retain their controls alongside sender questions: in Tanya the proposed account and its alternatives are offered, any other account can be chosen, and the owner confirms the choice.

#### Scenario: Account question arrives
- **WHEN** a result asks the owner to confirm an account
- **THEN** Tanya offers the account-confirmation controls, not the sender reply actions

### Requirement: Invalid reply input stays correctable

Blank or invalid reply input SHALL NOT be queued and SHALL remain correctable. A reply from the question notification that the companion refuses or fails to store SHALL leave that question answerable from its notification, with both actions and a statement that the reply was not saved, unless an answer for that question is already stored or the question is no longer open.

#### Scenario: Blank name
- **WHEN** the owner submits a blank name
- **THEN** nothing is queued and the owner can enter a name and send again

#### Scenario: Refused notification reply stays answerable
- **WHEN** the owner replies to a sender question from its notification and the reply is not stored, for example because the device was locked or the reply text did not arrive
- **THEN** nothing is queued, the notification still offers `Balas nama` and `Catat tanpa nama` and says the reply was not saved, and a later lookup of the same question leaves it answerable

#### Scenario: Refused reply after a stored answer
- **WHEN** a second notification action arrives for a question whose answer is already stored
- **THEN** the stored answer is kept and the notification reports it without offering the actions again

### Requirement: Reply delivery failures are handled by kind

Transient reply delivery failures SHALL retry with bounded backoff. Authentication failures SHALL wait for pairing recovery. Conflicts and permanent validation failures SHALL stop automatic replay and expose a recoverable in-app state.

#### Scenario: Server unavailable
- **WHEN** reply delivery receives a 503 or loses the network
- **THEN** the same answer is retried after a bounded, growing delay

#### Scenario: Credentials rejected
- **WHEN** reply delivery receives a 401 or 403
- **THEN** the reply waits for the pairing to work again and is not retried in a loop

#### Scenario: Validation rejected
- **WHEN** reply delivery receives a 422
- **THEN** automatic replay stops and the app shows that the reply can be corrected

### Requirement: Pending replies say they are waiting

While a reply waits for connectivity, the companion SHALL display `Balasan menunggu koneksi` or an equivalent truthful pending state.

#### Scenario: Reply queued offline
- **WHEN** the owner replies while offline
- **THEN** the app shows that the reply is stored and waiting, not that the transaction is recorded

### Requirement: Question lookups do not repeat alerts

Repeated lookup of the same sender question SHALL NOT deliberately generate repeated alerts.

#### Scenario: Same question refreshed
- **WHEN** the same open sender question is looked up again
- **THEN** its notification is updated without a new sound

### Requirement: Retention keeps unanswered sender questions

Retention SHALL preserve events with unanswered sender questions and rows whose replies are not yet delivered.

#### Scenario: Pruning with an undelivered reply
- **WHEN** retention runs while a row has a reply that the backend has not accepted
- **THEN** the row and its reply remain

### Requirement: Sender answer attempts are recorded privately

The companion SHALL record each step of every sender answer attempt, from the notification or the app, in bounded local storage that is excluded from backup: that the attempt was received, then that it was queued or why it was refused. A record SHALL hold only reason codes, channel, identifiers, time and app version, never a name, typed text or amount.

#### Scenario: Refused attempt is recorded
- **WHEN** a notification reply is refused because the device is locked
- **THEN** the record holds a received step and a locked step for that question, without the typed name

#### Scenario: Attempt that never completes
- **WHEN** an attempt is received but the app stops before storing or refusing it
- **THEN** the record holds only the received step, which distinguishes it from an attempt that never reached the app

#### Scenario: Bounded record
- **WHEN** more than 200 attempt steps have been recorded
- **THEN** only the newest 200 are kept

### Requirement: Tanya shows an unsaved notification reply

Tanya SHALL state that a notification reply was not saved while the newest settled attempt for the shown sender question is a refused notification reply, and SHALL keep the question answerable.

#### Scenario: Tanya shows an unsaved notification reply
- **WHEN** the owner opens Tanya on a sender question whose newest settled attempt is a refused notification reply
- **THEN** the card says the reply from the notification was not saved and keeps the name field and both actions available

#### Scenario: Newer answer replaces the notice
- **WHEN** a newer attempt for the same question settles after the refused notification reply
- **THEN** Tanya no longer shows the unsaved-reply notice
