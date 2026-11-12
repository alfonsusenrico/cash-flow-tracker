## ADDED Requirements

### Requirement: Edits Are Checked by the Extra Money They Take
An edit to an expense or an internal movement SHALL apply the sufficient-funds check to a liquid account only when the edit takes more money from that account than the record already did: the new amount minus the old amount on the same account, or the whole new amount on another account. An edit with no extra SHALL save whatever the account's balance is, including a balance below zero.

#### Scenario: Correcting the pillar and note of an expense on a below-zero account
- **WHEN** a liquid account is at −189,667 IDR after a settled notification recorded a 189,667 IDR expense, and the owner saves that expense with the same amount and account but another Kakeibo pillar and note
- **THEN** the edit is saved
- **AND** the account balance stays −189,667 IDR

#### Scenario: Lowering the amount on a below-zero account
- **WHEN** the owner lowers that expense to 150,000 IDR
- **THEN** the edit is saved and the account balance becomes −150,000 IDR

#### Scenario: Raising the amount on a below-zero account
- **WHEN** the owner raises that expense to 189,668 IDR
- **THEN** the system refuses with `insufficient_funds`, available 0 IDR and required 189,668 IDR
- **AND** the expense and the balance are unchanged

#### Scenario: Moving the expense to an account that cannot cover it
- **WHEN** the owner moves that expense to another liquid account holding 1,000 IDR
- **THEN** the system refuses with `insufficient_funds` and neither account changes

#### Scenario: Editing a movement whose source is below zero
- **WHEN** a movement of 10,000 IDR leaves its source at −5,000 IDR after a later settled notification, and the owner renames it or lowers it to 8,000 IDR
- **THEN** each edit is saved and the source ends at −3,000 IDR
- **AND** raising it to 8,001 IDR is refused with `insufficient_funds` and changes nothing
