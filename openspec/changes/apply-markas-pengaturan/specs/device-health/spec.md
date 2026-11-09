## MODIFIED Requirements

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
