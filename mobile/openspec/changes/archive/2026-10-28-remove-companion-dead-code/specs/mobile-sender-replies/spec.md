## MODIFIED Requirements

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

## ADDED Requirements

### Requirement: The detail sheet answers sender questions

Tapping a sender question notification SHALL open the corresponding detail sheet, where equivalent name and unknown-sender controls SHALL be available.

#### Scenario: Notification body tapped
- **WHEN** the owner taps the body of a sender question notification
- **THEN** the app opens that event's detail sheet with a name field and a `Catat tanpa nama` action

### Requirement: Sender questions are private on the lock screen

The financial details of a sender question notification SHALL have a private lock-screen presentation.

#### Scenario: Locked device shows the question
- **WHEN** a sender question is shown on a locked device
- **THEN** the lock-screen version hides the masked sender, amount and account

### Requirement: Account-selection questions keep their controls

Account-selection questions SHALL retain their existing controls alongside sender questions.

#### Scenario: Account question arrives
- **WHEN** a result asks the owner to confirm an account
- **THEN** the existing account-confirmation controls are offered, not the sender reply actions

### Requirement: Invalid reply input stays correctable

Blank or invalid reply input SHALL NOT be queued and SHALL remain correctable.

#### Scenario: Blank name
- **WHEN** the owner submits a blank name
- **THEN** nothing is queued and the owner can enter a name and send again

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
