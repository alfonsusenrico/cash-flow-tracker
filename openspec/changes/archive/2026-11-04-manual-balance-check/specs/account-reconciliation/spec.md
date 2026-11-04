## ADDED Requirements

### Requirement: Manual Balance Check
The web app SHALL offer "Cocokkan saldo" for every liquid account, pocket, and parent whose pockets are all liquid. It runs only when the owner asks, never on a schedule. It SHALL compare the typed balance with the recorded balance without changing the ledger. A parent SHALL be compared as its own rows plus its pockets' rows, each counted once.

#### Scenario: Exact match
- **GIVEN** a pocket recorded at Rp 1.250.000
- **WHEN** the owner checks it with Rp 1.250.000
- **THEN** the difference SHALL be Rp 0, no row SHALL be written, and "Saldo cocok" SHALL be offered

#### Scenario: Parent checked as a group
- **GIVEN** a parent with Rp 100.000 of its own and two pockets with Rp 400.000 and Rp 500.000
- **WHEN** the owner checks the parent with Rp 1.000.000
- **THEN** the recorded balance SHALL be Rp 1.000.000, and "Sesuaikan saldo" SHALL NOT be offered for the parent

#### Scenario: Not a liquid account
- **WHEN** the owner checks an investment product or a parent holding one
- **THEN** the check SHALL be refused with `balance_check_not_liquid`, and another owner's account SHALL answer 404

### Requirement: Balance Check Causes
The check SHALL list likely causes found deterministically, with no AI, in the window since the account's last matched point or adjustment, or the last 30 days without one:
- a possible double booking;
- an unpaired movement leg;
- a held, skipped or unrecorded report;
- a report still being processed.

Each cause SHALL link to its row or Tinjau item. When at most three causes sum exactly to the difference, the check SHALL name them as the explanation.

#### Scenario: Difference explained by a double booking
- **GIVEN** a pocket with a Rp 500.000 income from a Jago notification, and a Rp 500.000 GoPay income on another account two seconds apart
- **WHEN** the owner checks the pocket with a balance Rp 500.000 lower than recorded
- **THEN** the explanation SHALL be that income, as a possible double booking, linked to its row

#### Scenario: Difference explained by an unpaired leg
- **GIVEN** a Rp 75.000 row filed as Internal Movement without a partner
- **WHEN** the check's difference is −Rp 75.000
- **THEN** the explanation SHALL be that unpaired leg

#### Scenario: Unexplained difference
- **WHEN** no set of at most three causes sums to the difference
- **THEN** the check SHALL show the difference and the causes without an explanation line

### Requirement: Matched Balance Point
"Saldo cocok" SHALL record a matched point only when the typed balance equals the recorded balance at that moment, and SHALL otherwise be refused with `balance_mismatch` and the difference. The next check of that account SHALL start from its newest matched point.

#### Scenario: Next check starts at the match
- **GIVEN** a matched point recorded on 3 November
- **WHEN** the owner checks the account again on 5 November
- **THEN** only rows and reports after the matched point SHALL be considered, and the basis SHALL read "Sejak saldo cocok"

### Requirement: Balance Check With Adjustment
"Sesuaikan saldo" SHALL keep its request, response and adjustment row unchanged. When it is opened from the balance check and succeeds, the web app SHALL record a matched point at the adjusted balance.

#### Scenario: Adjusting from the check
- **WHEN** the owner checks a pocket, sees a difference of +Rp 20.000 and chooses "Sesuaikan saldo"
- **THEN** one "Penyesuaian Saldo" income of Rp 20.000 SHALL be recorded as before, followed by a matched point at the typed balance
