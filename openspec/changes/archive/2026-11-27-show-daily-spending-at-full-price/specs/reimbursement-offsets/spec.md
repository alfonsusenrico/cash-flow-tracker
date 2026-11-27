## ADDED Requirements

### Requirement: Daily Series at Full Price
Dashboard daily series (analytics `daily_cadence`, overview `cumulative_trendline` and `burn_cadence`) and the analytics day-of-week heatmap SHALL report each local day at the full price of its rows, without subtracting reimbursements. The window's applied offsets SHALL be subtracted only from the window's totals, so the days add up to the gross outflow and the gross outflow less `reimbursement_offset` is `total_outflow`.

#### Scenario: Days add up to the gross outflow
- **GIVEN** the "Shared dinner partly repaid" cycle (Rp 500.000 spent, Rp 200.000 reimbursed)
- **WHEN** dashboard analytics and overview are requested
- **THEN** the sum of `daily_cadence[].expense` and of the heatmap `total_spent` SHALL be Rp 500.000, the last `cumulative_outflow` SHALL be Rp 500.000, and `total_outflow` SHALL stay Rp 300.000

#### Scenario: An unlinked reimbursement leaves the days alone
- **GIVEN** Rp 10.000 of Transportasi on 21 November, other Transportasi days, and Rp 200.000 of reimbursements naming Transportasi without links on 16 November
- **WHEN** the cycle's analytics are requested
- **THEN** 21 November SHALL show Rp 10.000 of Transportasi, and the cycle's Pengeluaran SHALL be the same as before this change

#### Scenario: A linked reimbursement leaves its day alone
- **GIVEN** a Rp 210.000 meal on 31 October with two Rp 70.000 reimbursements linked to it
- **WHEN** the cycle's analytics are requested
- **THEN** 31 October SHALL count the meal as Rp 210.000, and the meal's category and pillar SHALL still be lowered by Rp 140.000 in the cycle's totals

### Requirement: Net Ledger Summary
The unfiltered ledger summary SHALL report the window's outflow net of applied reimbursement offsets, with `gross_outflow` and `reimbursement_offset` beside it, and a category filter SHALL net only that category's offset.

#### Scenario: Ledger summary follows the dashboard
- **GIVEN** the "Shared dinner partly repaid" cycle
- **WHEN** the unfiltered ledger is listed
- **THEN** the cumulative `outflow` SHALL be Rp 300.000 with `gross_outflow` Rp 500.000 and `reimbursement_offset` Rp 200.000
- **AND** filtered to "Makan Bareng" the `outflow` SHALL be Rp 100.000

#### Scenario: Filters that no report figure covers stay gross
- **WHEN** the same ledger is filtered by account
- **THEN** the `outflow` SHALL be Rp 500.000 and `reimbursement_offset` 0

## REMOVED Requirements

### Requirement: Reimbursement-Net Series and Ledger Summary
**Reason**: The owner chose on 2026-11-24 to show every day at full price, so the daily series are no longer net of reimbursements; the ledger part is unchanged.
**Migration**: The daily part is replaced by "Daily Series at Full Price" and the ledger part by "Net Ledger Summary". API field names are unchanged; daily values become full price.

### Requirement: Offset Attribution to Spending Days
**Reason**: Days no longer carry any share of a reimbursement, so no attribution to days remains.
**Migration**: None. Period, category and pillar offsets are computed as before; days are full price ("Daily Series at Full Price").
