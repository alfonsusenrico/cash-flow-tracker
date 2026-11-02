## ADDED Requirements

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

## MODIFIED Requirements

### Requirement: Only accepted in-progress events are polled automatically
Only accepted queued or processing events, events awaiting confirmation, and recorded events whose purpose question is still open SHALL be polled automatically for completion. Review, failed and ignored outcomes SHALL NOT trigger automatic backend retries or resubmission.

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
