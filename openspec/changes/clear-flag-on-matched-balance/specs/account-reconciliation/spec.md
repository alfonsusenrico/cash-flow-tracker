## MODIFIED Requirements

### Requirement: Matched Balance Point
"Saldo cocok" SHALL record a matched point only when the typed balance equals the recorded balance at that moment, and SHALL otherwise be refused with `balance_mismatch` and the difference. The next check of that account SHALL start from its newest matched point. A recorded matched point SHALL clear the checked account's own reconciliation-required state in the same database transaction; the pockets of a checked parent SHALL keep theirs.

#### Scenario: Next check starts at the match
- **GIVEN** a matched point recorded on 3 November
- **WHEN** the owner checks the account again on 5 November
- **THEN** only rows and reports after the matched point SHALL be considered, and the basis SHALL read "Sejak saldo cocok"

#### Scenario: Matching a flagged pocket clears its warning
- **GIVEN** the pocket "Tabungan" shows "Perlu dicocokkan" with a recorded balance of Rp 82.515
- **WHEN** the owner types Rp 82.515 in "Cocokkan saldo" and chooses "Saldo cocok"
- **THEN** a matched point SHALL be recorded and the pocket's reconciliation-required state SHALL be cleared, so the tile no longer shows "Perlu dicocokkan"

#### Scenario: A refused match keeps the warning
- **WHEN** the typed balance differs from the recorded one
- **THEN** the match SHALL be refused with `balance_mismatch` and the reconciliation-required state SHALL be unchanged

#### Scenario: A matching total keeps a pocket's warning
- **GIVEN** a flagged pocket under an account with pockets
- **WHEN** the owner matches the account's total
- **THEN** the pocket SHALL keep "Perlu dicocokkan"
