## ADDED Requirements

### Requirement: Calendar Days at Full Price
A day's `spend` SHALL be that local day's reportable spending and its `spending` that day's Pengeluaran rows, both at full price, with no reimbursement subtracted. The days' `spend` SHALL add up to `totals.gross_spend`, and their `spending` to `totals.spending_gross`.

#### Scenario: Reimbursement on a later day
- **GIVEN** Rp 200.000 spent on 3 November in "Makan di Luar" and a Rp 50.000 reimbursement on 6 November for that category
- **WHEN** the calendar is requested
- **THEN** 3 November SHALL show Rp 200.000, 6 November no spending for it, `spending_gross` SHALL include Rp 200.000, `spending_reimbursed` SHALL be Rp 50.000, and `spending` SHALL be the period's Pengeluaran

#### Scenario: A day matches its rows
- **GIVEN** 21 November holds a Rp 190.000 meal and Rp 10.000 of parking, and Transportasi has unlinked reimbursements in the period
- **WHEN** the calendar is requested
- **THEN** 21 November's `spending` SHALL be Rp 200.000

#### Scenario: Waiting rows stay counted
- **GIVEN** a Rp 45.000 expense waiting for a purpose answer on 2 November
- **WHEN** the calendar is requested
- **THEN** 2 November's spending SHALL include Rp 45.000 and its waiting count SHALL be 1

#### Scenario: Movement counted once
- **WHEN** a linked Pindah saldo of Rp 500.000 happened on 2 November
- **THEN** 2 November SHALL report one movement of Rp 500.000, not two legs, and no spending for it

### Requirement: Calendar Reimbursed Totals
The calendar `totals` SHALL return `spending` (the period's Pengeluaran, net of reimbursements), `spending_gross` and `spending_reimbursed`, with `spending_gross − spending_reimbursed = spending`, and SHALL keep `spend`, `gross_spend` and `offset_applied` for Belanja. A response whose figures break these sums SHALL fail rather than be shown.

#### Scenario: Totals reconcile
- **GIVEN** Rp 200.000 spent on 3 November in "Makan di Luar" and a Rp 50.000 reimbursement for that category
- **WHEN** the calendar is requested
- **THEN** `spending_gross` SHALL be Rp 200.000, `spending_reimbursed` Rp 50.000 and `spending` Rp 150.000

### Requirement: Kalender Names the Reimbursed Amount
When `spending_reimbursed` is above zero, Kalender's hero SHALL show under the period's Pengeluaran the line "{spending_gross} dibelanjakan · −{spending_reimbursed} diganti" ("spent", "reimbursed" in English), masked when balances are hidden. With nothing reimbursed the line SHALL not appear.

#### Scenario: Line with reimbursements
- **GIVEN** a period with Rp 1.250.000 of full-price days and Rp 200.000 reimbursed
- **WHEN** Kalender is shown
- **THEN** the hero SHALL read Rp 1.050.000 with the line "Rp 1.250.000 dibelanjakan · −Rp 200.000 diganti"

#### Scenario: No reimbursement
- **WHEN** nothing was reimbursed in the period
- **THEN** the hero SHALL show no such line

## REMOVED Requirements

### Requirement: Calendar Days Add Up to Belanja
**Reason**: Days are now at full price (owner decision 2026-11-24), so they add up to the full-price total, not to net Belanja.
**Migration**: Replaced by "Calendar Days at Full Price"; the response keeps every field and adds `spending_gross` and `spending_reimbursed`.
