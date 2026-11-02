## ADDED Requirements

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

## MODIFIED Requirements

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
