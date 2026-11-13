## ADDED Requirements

### Requirement: No Assumed Pocket for an Outgoing Payment
An expense notification for an account with pockets that names no pocket SHALL be recorded on a pocket only from a learned decision or the owner's answer. Otherwise it SHALL ask the source-pocket question with a preselected suggestion and write nothing before the answer. Incoming money SHALL keep the default pocket.

#### Scenario: First payment to a merchant asks
- **WHEN** Jago reports "You have paid Rp150.000 to SPBU No ID 54 601 08" and the owner has no pocket choices for that merchant
- **THEN** the event SHALL be `needs_confirmation` with a `source_pocket` question whose suggestion is the configured default pocket
- **AND** no transaction SHALL be written

#### Scenario: Incoming money keeps the default pocket
- **WHEN** Jago reports money sent to the owner without naming a pocket
- **THEN** it SHALL be recorded on the default pocket as before, without a question

#### Scenario: Older companion
- **WHEN** the event comes from a companion older than 1.6.0
- **THEN** it SHALL be `needs_review` and no pocket SHALL be assumed

### Requirement: Pocket Choices Are the Owner's Own Labels
The system SHALL count as pocket choices the owner's answers to such questions, the accounts they chose when resolving an event by hand, and every notification-recorded expense they moved to another account (a correction). A correction SHALL weigh twice an answer, and an event SHALL count once, its correction before its answer. Automatic records, model output and defaults SHALL never be evidence.

#### Scenario: A web correction teaches
- **WHEN** the owner moves a fuel payment recorded from a notification from "Main Pocket" to "Transportasi" on the web
- **THEN** it SHALL count as one correction choice of "Transportasi" for that merchant key

#### Scenario: An answer then a correction of the same payment
- **WHEN** the owner answers "Main Pocket" and later moves the same payment to "Transportasi"
- **THEN** only the correction SHALL count for that payment

### Requirement: Learned Pocket Recording
Over a key's latest 10 choices, with corrections weighing 2, the payment SHALL be recorded on pocket P without a question, marked with its basis, when (weight agreeing on P + 1) ÷ (total weight + 2) is at least 0.75. Otherwise the top pocket SHALL only be the preselection. A kill switch SHALL turn learned recording into asking.

#### Scenario: Two agreeing choices record automatically
- **WHEN** the owner chose "Transportasi" for two earlier fuel payments and none differently
- **THEN** the next fuel payment SHALL be recorded on "Transportasi" with `pocket_habit` basis "2 dari 2"

#### Scenario: Thin evidence with an exception keeps asking
- **WHEN** the latest choices are three "Transportasi" and one "Main Pocket"
- **THEN** the payment SHALL be asked with "Transportasi" preselected

#### Scenario: A rare exception does not undo a habit
- **WHEN** the latest choices are five "Transportasi" and one "Main Pocket"
- **THEN** the payment SHALL be recorded on "Transportasi"

#### Scenario: Learned pocket no longer exists
- **WHEN** the learned pocket is archived
- **THEN** the payment SHALL be asked and no row SHALL be written before the answer

#### Scenario: Kill switch
- **WHEN** learned recording is turned off
- **THEN** every such payment SHALL be asked with the learned favourite preselected

### Requirement: Pocket Learning Key
The key SHALL be the institution with the first word of the merchant or recipient name, lower-cased and letters only; for a payment that names no merchant, the institution with the text signature; for money added into a pocket with no named source, the target pocket.

#### Scenario: One station chain shares a key
- **WHEN** payments go to "SPBU 54.602.45 HR M", "SPBU No ID 54 602 62 MBL" and "SPBU 54.60262,RY.MENGN-"
- **THEN** all three SHALL use the key "spbu" for the same institution

#### Scenario: Debit card payments share a key
- **WHEN** Jago reports "You've paid Rp45.000 using your debit card" twice
- **THEN** both SHALL use the key of the text signature `jago|expense|payment`

### Requirement: Learning From Earlier Corrections
From the first deploy, notification-recorded expenses that now sit on another account than they were recorded on SHALL count as correction choices, without a migration. Uncorrected payments SHALL NOT be evidence.

#### Scenario: Earlier fuel corrections count
- **WHEN** two earlier fuel payments were recorded on "Main Pocket" and the owner moved both to "Transportasi"
- **THEN** the next fuel payment SHALL be recorded on "Transportasi"

### Requirement: The Same Learning for Every Unnamed Source
Unnamed transfers and money added into a pocket with no named source SHALL use the same choices, rule and suggestion, keyed by recipient and by target pocket.

#### Scenario: Mixed sources keep asking
- **WHEN** money added into "Bayar Kredit" came from "Makan-Jalan" three times and from two other pockets once each
- **THEN** the next one SHALL be asked with "Makan-Jalan" preselected
