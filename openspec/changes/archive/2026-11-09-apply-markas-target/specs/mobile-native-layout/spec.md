## MODIFIED Requirements

### Requirement: Phone Target & Tagihan Composition
Below 768 px Target & Tagihan SHALL show the hero Ketahanan Dana with two supporting figures, then "Tagihan 30 hari ke depan", "Target tabungan" and "Utang & kredit" in one column. Each goal, debt, credit row and plan SHALL open a bottom sheet, and no row SHALL truncate its amounts.

#### Scenario: Target & Tagihan on a phone
- **WHEN** the owner opens `/goals` at 390 px
- **THEN** Ketahanan Dana SHALL come first and each goal row SHALL show its progress text without truncating the amounts

#### Scenario: Phone order
- **WHEN** the owner opens `/goals` at 390 px and at 320 px
- **THEN** the order SHALL be the hero with Dana darurat and Target, Tagihan, Target tabungan, then Utang & kredit, in visual, reading and focus order, with no horizontal overflow and every row at least 44 px tall
