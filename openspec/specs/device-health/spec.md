# device-health Specification

## Purpose
Let a paired companion phone report whether it can capture notifications, so the web can confirm pairing with a live checklist and show each phone's readiness and problems in Settings.

## Requirements

### Requirement: Phone Health Report
`POST /api/devices/health` SHALL accept only a live Bearer API key and SHALL attribute the report to that key's device, never to an id in the body. A request without a Bearer key, or with an unknown or revoked key, SHALL get 401. A browser session alone SHALL NOT be accepted.

#### Scenario: A paired phone reports
- **WHEN** a phone sends a valid report with its own key
- **THEN** the response SHALL be 200 with `ok: true` and the stored `health`, including `reported_at` and `ready`.

#### Scenario: Bad or revoked key
- **WHEN** a report carries an unknown key, or the key of a device the owner revoked
- **THEN** the response SHALL be 401 and nothing SHALL be stored.

### Requirement: Health Report Validation
The report SHALL carry `app_version` (1–32 characters of letters, digits, `.`, `+`, `_` or `-`), the booleans `listener_connected`, `notification_access`, `post_notifications` and `battery_unrestricted`, `self_test` with `status` in `passed`, `failed` or `not_run`, and an integer `queue_pending` of 0 or more. Wrong types, unknown status values and inconsistent self-test fields SHALL get 422 and store nothing. Unknown fields SHALL be ignored.

#### Scenario: Wrong types
- **WHEN** `listener_connected` is the string `"true"`, or `queue_pending` is -1
- **THEN** the response SHALL be 422.

#### Scenario: Unknown self-test status
- **WHEN** `self_test.status` is `"skipped"`
- **THEN** the response SHALL be 422.

#### Scenario: Failed self-test without its step
- **WHEN** `self_test.status` is `failed` and `failed_step` is not one of `post`, `capture` or `server`
- **THEN** the response SHALL be 422.

#### Scenario: Step on a self-test that did not fail
- **WHEN** `self_test.status` is `passed` or `not_run` and `failed_step` is not null
- **THEN** the response SHALL be 422.

#### Scenario: Unknown fields
- **WHEN** a valid report also carries a field the server does not know
- **THEN** the response SHALL be 200 and the field SHALL be neither stored nor returned.

### Requirement: Health Timestamps
`self_test.at` SHALL be an ISO-8601 timestamp with a time zone when the self-test passed or failed, and MAY be null only when it was not run. `last_capture_at` SHALL be such a timestamp or null. `reported_at` SHALL be the server's receive time.

#### Scenario: Timestamp without a zone
- **WHEN** `last_capture_at` is `"2026-11-02T10:00:00"`
- **THEN** the response SHALL be 422.

#### Scenario: Self-test result without a time
- **WHEN** `self_test.status` is `passed` and `at` is null
- **THEN** the response SHALL be 422.

### Requirement: Health Report Rate Limit
The server SHALL accept at most 30 health reports per minute for one key. Further reports in that minute SHALL get 429 with a `Retry-After` header and SHALL NOT change the stored report.

#### Scenario: Thirty-first report in a minute
- **WHEN** a phone sends a 31st report within one minute
- **THEN** the response SHALL be 429 and the stored report SHALL be the 30th.

### Requirement: Latest Report Only
The server SHALL keep exactly one health report per device, replacing it on every accepted report, with no history. It SHALL NOT store notification content or any other personal data in it, and health logs SHALL hold only key ids and codes.

#### Scenario: A second report replaces the first
- **WHEN** a phone reports twice
- **THEN** the device's health SHALL show only the second report.

#### Scenario: Logs stay free of personal data
- **WHEN** a phone named "Synthetic Secret Phone" reports
- **THEN** the log SHALL name the key id and the codes, and SHALL NOT contain the device name or the key.

### Requirement: Readiness Rule
`ready` SHALL be true exactly when `notification_access` is true, `listener_connected` is true and `self_test.status` is `passed`. `battery_unrestricted` and `post_notifications` SHALL NOT affect `ready`.

#### Scenario: Every combination
- **WHEN** a report carries any combination of notification access, listener state, self-test status and battery state
- **THEN** `ready` SHALL be true only for access on, listener connected and self-test passed, whatever the battery state.

### Requirement: Health in the Device List
`GET /api/devices` SHALL add `health` to every listed device: the latest stored report with `reported_at` and `ready`, or `null` when that device has never reported. Each owner SHALL see only health of their own devices. Revoking a device SHALL delete its health, and re-pairing a device SHALL start without health.

#### Scenario: Never reported
- **WHEN** the owner lists devices before a phone has reported
- **THEN** that phone's `health` SHALL be `null`.

#### Scenario: Another owner's phone
- **WHEN** Alice's phone reports and Bob lists his devices
- **THEN** Bob's list SHALL contain neither Alice's phone nor its health.

### Requirement: Live Pairing Checklist
After a code is issued, the pairing dialog SHALL poll the device list every 3 seconds for at most 10 minutes. When a phone pairs, it SHALL show four steps: Terhubung, Izin notifikasi, Jalan di latar belakang and Tes tangkap notifikasi, each marked in words as done, failed, in progress on the phone, or not yet. Polling SHALL stop when the phone is ready, when 10 minutes pass, or when the dialog closes.

#### Scenario: Phone pairs and finishes setup
- **WHEN** the phone pairs, then reports access on, listener connected and a passed self-test
- **THEN** each step SHALL turn done in turn and the dialog SHALL say "HP siap. Transaksi dari notifikasi bank kini tercatat otomatis." ("The phone is ready. Transactions from bank notifications are now recorded automatically.")
- **AND** polling SHALL stop.

#### Scenario: Closing the dialog stops polling
- **WHEN** the owner closes the dialog while it is polling
- **THEN** no further device list request SHALL be made by the dialog.

#### Scenario: Steps without tinted fills
- **WHEN** the checklist shows a done, a failed and a pending step
- **THEN** each SHALL carry its status word and icon on the dialog surface, with no tinted background

### Requirement: Pairing Checklist Problems
A step SHALL show as failed when the phone reports it off after its self-test ran, and the dialog SHALL name the first failed step. A failed self-test SHALL show which step failed: showing the test notification, catching it, or reaching the server. When `battery_unrestricted` is false, the background step SHALL carry a non-blocking hint that HiOS and MIUI phones usually need an extra permission.

#### Scenario: Self-test fails at capture
- **WHEN** the phone reports a failed self-test with `failed_step` `capture`
- **THEN** "Tes tangkap notifikasi" SHALL show as failed with an explanation that the test notification was not caught.

#### Scenario: Battery restricted but ready
- **WHEN** a ready phone reports `battery_unrestricted` false
- **THEN** the dialog SHALL say the phone is ready and SHALL still show the battery hint.

### Requirement: Pairing Checklist Announcements
The dialog SHALL announce each checklist change once through a polite live region, without moving focus.

#### Scenario: A step completes
- **WHEN** notification access turns on during pairing
- **THEN** the live region SHALL say "Izin notifikasi beres." and focus SHALL stay where it was.

### Requirement: Device Readiness in Settings
Pengaturan › HP & perangkat SHALL show, in its "Status HP" box, for each paired phone whether it is ready, when it last reported, its app version and its checks: notification access, running in the background, the self-test (with its failed step) and reports within 24 hours. A phone that never reported SHALL say so. "Perangkat terhubung" SHALL list each phone's name, pairing date, last activity, version and "Cabut".

#### Scenario: Ready phone
- **WHEN** a phone reported ready an hour ago
- **THEN** the box SHALL show it as "siap" with the report time, the app version and every check as "Baik" ("Good")

#### Scenario: Silent phone
- **WHEN** a phone's last report is more than 24 hours old
- **THEN** the box SHALL show it as "perlu dicek" with the problem that the phone has not reported for over 24 hours

#### Scenario: Two phones
- **WHEN** two phones are paired and one has notification access off
- **THEN** the box SHALL show one block per phone, and only the second SHALL show the access problem
