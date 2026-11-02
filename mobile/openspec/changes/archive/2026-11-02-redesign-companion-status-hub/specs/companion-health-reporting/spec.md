## ADDED Requirements

### Requirement: The phone reports its health
The phone SHALL send `POST /api/devices/health` with its own Bearer key and no device id after setup and the self-test, when the app opens, when the listener connects or disconnects, and at most every 15 minutes while running. The body SHALL carry every field of the device health contract with its JSON type, and no names, amounts or notification text.

#### Scenario: Ready phone
- **WHEN** the listener is connected, access is granted and the self-test passed
- **THEN** the report has `self_test.status` `passed`, a null `failed_step`, an `at` with its offset, and booleans for every permission

#### Scenario: Never captured
- **WHEN** the phone has not captured anything yet
- **THEN** `last_capture_at` is null

#### Scenario: Field types
- **WHEN** any report is sent
- **THEN** `app_version` holds only `0-9A-Za-z.+_-` and at most 32 characters, the four permission fields are JSON booleans, times are ISO-8601 with an offset, and `queue_pending` is an integer of 0 or more

### Requirement: The self-test result follows the contract
`self_test.status` SHALL be `passed`, `failed` or `not_run`. A `failed` test SHALL name its step, `post`, `capture` or `server`, and its time; a `passed` test SHALL have a time and a null step; a `not_run` test SHALL have a null step and a null time. A stored result that breaks these rules SHALL be reported as `not_run`.

#### Scenario: Failed at capture
- **WHEN** the self-test failed because the listener did not catch its notification
- **THEN** the report says `failed`, `capture`, and the time of the test

#### Scenario: Inconsistent stored result
- **WHEN** a stored result says `failed` without a step
- **THEN** it is reported as `not_run`

### Requirement: Server answers are handled by kind
A 2xx answer SHALL record the report as sent. A 404 from an older tracker SHALL be quiet and retried with backoff. A 401 or 403 SHALL stop reports for that key and show the re-pair problem. A 429 SHALL wait for its `Retry-After`. A network or server error SHALL be retried with backoff; a 422 SHALL not be repeated.

#### Scenario: Older server
- **WHEN** the tracker answers 404
- **THEN** nothing is shown to the owner, the report is tried again with backoff, and periodic reports pause for 6 hours

#### Scenario: Revoked key
- **WHEN** the tracker answers 401 `device_key_invalid`
- **THEN** no further report is sent with that key and the hub shows "Sambungan ke akunmu putus"
- **AND** reports resume after a new pairing or when "Tes Koneksi" finds the key accepted again

#### Scenario: Rate limited
- **WHEN** the tracker answers 429 with `Retry-After: 60`
- **THEN** no report is sent in the next 60 seconds, and the hub shows no server problem for it

#### Scenario: Network failure
- **WHEN** the tracker cannot be reached
- **THEN** the report is retried with backoff and the hub shows "Server lagi nggak kejangkau"

### Requirement: The self-test notification is answered by the phone's own listener
The self-test SHALL post a notification in its own channel ("Tes pendengar"), tagged with the self-test tag and a one-time token in its extras. The listener SHALL recognise only this app's notification with that tag and token, before any capture code reads it, answer the waiting test, and drop it. The test SHALL wait at most 10 seconds and remove its notification afterwards.

#### Scenario: Caught
- **WHEN** the listener receives the marked notification
- **THEN** the waiting test continues and no row is stored for it

#### Scenario: Another app's notification with the same tag
- **WHEN** a notification from another package carries the self-test tag and token
- **THEN** it is not treated as the self-test
