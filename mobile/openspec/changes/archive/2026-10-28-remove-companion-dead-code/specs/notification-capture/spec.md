## ADDED Requirements

### Requirement: The auto-sync switch governs every automatic send
While "Auto-Sync Notifikasi" is off, the companion SHALL NOT send captures on its own: no periodic sync, no send after a capture and no send when the listener reconnects. Turning the switch off SHALL cancel the scheduled periodic sync and any queued automatic send. Turning it on SHALL schedule the periodic sync and send what is waiting. App start SHALL bring scheduled work in line with the switch. Owner actions SHALL still send while it is off.

#### Scenario: Switch turned off
- **WHEN** the owner turns auto-sync off while the periodic sync is scheduled and a send is queued
- **THEN** both are cancelled and no capture is sent until the owner asks

#### Scenario: Listener reconnects while off
- **WHEN** the listener reconnects or a new capture is stored while auto-sync is off
- **THEN** no send starts and the rows stay pending

#### Scenario: Switch turned on
- **WHEN** the owner turns auto-sync on
- **THEN** the periodic sync is scheduled and a send of pending rows is queued

#### Scenario: Upgrade with leftover periodic work
- **WHEN** the app starts with auto-sync off and a periodic sync left scheduled by an older build
- **THEN** that periodic sync is cancelled

#### Scenario: Owner syncs by hand while off
- **WHEN** the owner taps "Sinkronkan sekarang" or sends a kept row while auto-sync is off
- **THEN** the send is queued

#### Scenario: No account connected
- **WHEN** auto-sync is on but no server address and API key are set
- **THEN** a capture or reconnect starts no send

### Requirement: Sending an unsent notification offers only what the send carries
The inbox sheet for a notification that has not been sent SHALL offer the type, amount and note, and SHALL NOT offer account or category pickers, because the ingest request carries no account or category. It SHALL say that the tracker picks the account and category when it records the notification. Sending SHALL store the type, amount and note as the row's labels and SHALL join a kept row to the send pipeline.

#### Scenario: Unsent row opened
- **WHEN** the owner opens an unsent notification
- **THEN** the sheet shows no "Rekening" or "Kategori" picker and explains that the tracker picks them

#### Scenario: Unsent row sent
- **WHEN** the owner taps "Kirim Notifikasi ke Tracker" with amount 45000, type expense and note "Kedai Contoh"
- **THEN** exactly those three values are stored and the row is queued for sending

#### Scenario: Recorded or reviewable row opened
- **WHEN** the owner opens a recorded ordinary transaction or a row that needs review
- **THEN** the account and category pickers are offered, because the update or manual record carries them

#### Scenario: Already sent row
- **WHEN** the send is requested for a row that has been sent meanwhile
- **THEN** nothing is relabelled and the owner is told the notification was already sent

### Requirement: Owner edits keep the captured labels they do not change
When the owner sends an unsent notification or the server accepts a ledger edit from the sheet, the companion SHALL store the type, amount and note, SHALL keep the captured counterparty, and SHALL set the direction from the type only for expense ("out") and income ("in"). Any other type, such as a pocket move, SHALL keep its captured direction.

#### Scenario: Pocket move sent unchanged
- **WHEN** an unsent Jago pocket move labelled transfer, direction internal and counterparty "Contoh Pocket → Kantong Utama" is sent from the sheet
- **THEN** the row keeps the counterparty and the internal direction, so it is submitted as a transfer

#### Scenario: Type changed to income
- **WHEN** an unsent expense with counterparty "Kedai Contoh" is sent as income
- **THEN** the direction becomes "in" and the counterparty stays "Kedai Contoh"

#### Scenario: Ledger edit accepted
- **WHEN** the server accepts an edit of a recorded expense to Rp47.500
- **THEN** the row's amount label becomes 47500 and its counterparty is unchanged
