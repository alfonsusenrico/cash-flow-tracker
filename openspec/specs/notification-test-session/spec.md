# notification-test-session Specification

## Purpose
Let an owner run a time-boxed test session in which notifications from their phone are stored and labelled for evaluation but never interpreted, booked, paired, asked about or learned from, so real-world capture data can be gathered without changing the ledger.

## Requirements

### Requirement: Test Session Window
An owner SHALL be able to start a test session with a duration of 15 minutes to 6 hours, default 2 hours, and end it early. At most one session per owner SHALL be open. A session SHALL count as ended once its planned end has passed, without any background job.

#### Scenario: Starting a session
- **WHEN** the owner chooses "Mulai sesi" with the default duration at 16.30
- **THEN** a session SHALL run until 18.30, and every page SHALL show a banner with that end time and "Akhiri sesi"

#### Scenario: Second open session
- **GIVEN** an open session
- **WHEN** another session is started
- **THEN** the request SHALL fail with 409 `test_session_open`

#### Scenario: Automatic end
- **GIVEN** a session planned until 18.30
- **WHEN** a notification posted at 18.31 arrives
- **THEN** it SHALL be processed normally

### Requirement: Capture-Only Ingest
A notification whose `post_time` falls inside one of its owner's session windows SHALL be stored with its raw fields, tagged with the session and set to `ignored` with `error_code` `test_session`. It SHALL NOT be interpreted, booked, paired, asked about or learned from, now or later, and the database SHALL reject any transaction or movement on a tagged event.

#### Scenario: Movement during a session
- **GIVEN** an open session
- **WHEN** myBCA and GoPay report the same Rp 5.000 transfer
- **THEN** both SHALL be stored and tagged, and no transaction, movement, Tinjau item, question, merchant memory or habit SHALL be created

#### Scenario: Late upload of a session notification
- **GIVEN** a session that ended at 18.30
- **WHEN** a notification posted at 18.10 arrives at 19.00
- **THEN** it SHALL be stored as capture-only and never booked

#### Scenario: Nothing processes a tagged event later
- **GIVEN** a tagged event
- **WHEN** the worker, the deferred pairing pass, the linked-report rule, retry, resolve or a confirmation runs
- **THEN** none SHALL change it, and an attempt to set its transaction SHALL fail the database check

#### Scenario: Normal capture resumes
- **WHEN** a notification posted after the session's end arrives
- **THEN** it SHALL follow normal processing

### Requirement: Companion Contract During a Session
The ingest response for a capture-only notification SHALL be a valid result with status `ignored`, `error_code` `test_session` and its event id, so the companion settles the row without polling, retrying or alerting, and no companion release is needed.

#### Scenario: Phone during a session
- **WHEN** the companion submits a session notification
- **THEN** the response SHALL count it as received with `created_transactions` 0, and the companion SHALL show it as "Diabaikan"

### Requirement: Session Report
The owner SHALL see each session's notifications with app, post and arrival time, and the deterministic parse of direction and amount. The owner SHALL be able to set a case number and a note per notification, download the report as CSV, and delete the session's data. All session endpoints SHALL require a signed-in web session and SHALL answer 404 for another owner's session.

#### Scenario: Labelling a case
- **WHEN** the owner sets "5" as the case number of the Jago notification in the report
- **THEN** the CSV SHALL list it under case 5

#### Scenario: Another owner's session
- **WHEN** a signed-in owner requests another owner's session or event
- **THEN** the response SHALL be 404

#### Scenario: Companion key
- **WHEN** the companion's Bearer key calls a session endpoint
- **THEN** the response SHALL be 403 `session_required`

#### Scenario: Deleting session data
- **WHEN** the owner confirms "Hapus data sesi"
- **THEN** the session and its notifications SHALL be deleted, and the ledger SHALL be unchanged

### Requirement: Offline Session Replay
The repository SHALL provide scripts that export one session read-only from production to a file outside the repository, replay it into a loopback-only local database through the real ingest pipeline on a chosen revision, optionally without one app's reports, and score the outcome per case against an expected-results file. The scripts SHALL carry no owner data.

#### Scenario: Replay refuses a remote database
- **WHEN** the replay runner is given a database URL whose host is not loopback
- **THEN** it SHALL exit with an error before connecting

#### Scenario: Simulating a one-sided income
- **WHEN** a session is replayed with `--drop-app` set to myBCA
- **THEN** a BCA to Jago transfer SHALL appear as a single Jago income in the replay database
