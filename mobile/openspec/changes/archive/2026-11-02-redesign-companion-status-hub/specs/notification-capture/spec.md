## ADDED Requirements

### Requirement: The self-test notification is never captured
The listener SHALL decide, before reading any text for capture, whether a posted notification is this app's self-test (its own package, the self-test tag and a token), an app that is not an enabled registered financial app, or a capture. The self-test notification SHALL never be classified, stored in Room, synced or sent to the tracker as a notification; it only answers the waiting self-test.

#### Scenario: Self-test while a bank notification arrives
- **WHEN** the listener receives the self-test notification and a GoPay notification
- **THEN** only the GoPay notification is stored, and the self-test leaves no row of any outcome

## MODIFIED Requirements

### Requirement: Owner edits keep the captured labels they do not change
When the owner sends an unsent notification from Rincian, the companion SHALL store the type, amount and note, SHALL keep the captured counterparty, and SHALL set the direction from the type only for expense ("out") and income ("in"); any other type keeps its captured direction. A "Perbaiki label" correction SHALL change only the tracker's category, pillar or merchant, never the row's stored labels.

#### Scenario: Pocket move sent unchanged
- **WHEN** an unsent Jago pocket move labelled transfer, direction internal and counterparty "Contoh Pocket → Kantong Utama" is sent from the detail page
- **THEN** the row keeps the counterparty and the internal direction, so it is submitted as a transfer

#### Scenario: Type changed to income
- **WHEN** an unsent expense with counterparty "Kedai Contoh" is sent as income
- **THEN** the direction becomes "in" and the counterparty stays "Kedai Contoh"

#### Scenario: Ledger edit accepted
- **WHEN** the tracker accepts a "Perbaiki label" correction of a recorded expense
- **THEN** the row's amount label and counterparty are unchanged and its result is read again from the tracker
