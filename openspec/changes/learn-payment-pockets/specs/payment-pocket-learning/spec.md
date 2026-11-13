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
The system SHALL record a pocket choice for each owner answer to such a question and for each owner edit that moves a notification-recorded expense to another pocket of the same account. A correction SHALL weigh twice an answer, and the latest choice of a transaction SHALL replace its earlier ones. Automatic records, model output and defaults SHALL never be evidence.

#### Scenario: A web correction teaches
- **WHEN** the owner moves a fuel payment recorded from a notification from "Main Pocket" to "Transportasi" on the web
- **THEN** one correction choice for that merchant key and "Transportasi" SHALL be stored

#### Scenario: An answer then a correction of the same payment
- **WHEN** the owner answers "Main Pocket" and later moves the same payment to "Transportasi"
- **THEN** only the correction SHALL count for that payment

### Requirement: Learned Pocket Recording
When the latest choices of a pocket key reach the support and agreement thresholds (defaults 3 weighted support and 90% agreement, the latest 10 choices), the payment SHALL be recorded on that pocket without a question and marked with its basis. Below the thresholds the top pocket SHALL only be the preselection. A kill switch SHALL turn learned recording into asking.

#### Scenario: Three agreeing choices record automatically
- **WHEN** the owner chose "Transportasi" for three earlier payments to "SPBU No ID …" stations and none differently
- **THEN** the next such payment SHALL be recorded on "Transportasi" with `pocket_habit` basis "3 dari 3"

#### Scenario: A disagreeing choice keeps asking
- **WHEN** the latest choices are two "Transportasi" and one "Main Pocket"
- **THEN** the payment SHALL be asked with "Transportasi" preselected

#### Scenario: Learned pocket no longer exists
- **WHEN** the learned pocket is archived
- **THEN** the payment SHALL be asked and no row SHALL be written before the answer

#### Scenario: Kill switch
- **WHEN** learned recording is turned off
- **THEN** every such payment SHALL be asked with the learned favourite preselected

### Requirement: Pocket Learning Key
The key SHALL be the institution with the merchant name lower-cased and stripped of digits and punctuation, or, for a payment that names no merchant, the institution with the text signature.

#### Scenario: Station numbers share a key
- **WHEN** payments go to "SPBU No ID 54 601 08" and "SPBU No ID 31 102 02"
- **THEN** both SHALL use the key "spbu no id" for the same institution

#### Scenario: Debit card payments share a key
- **WHEN** Jago reports "You've paid Rp45.000 using your debit card" twice
- **THEN** both SHALL use the key of the text signature `jago|expense|payment`
