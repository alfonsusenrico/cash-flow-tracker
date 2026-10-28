## MODIFIED Requirements

### Requirement: Reimbursement-Net Series and Ledger Summary
Dashboard daily series (analytics `daily_cadence`, overview `cumulative_trendline` and `burn_cadence`) and the analytics day-of-week heatmap SHALL subtract applied reimbursement offsets.

#### Scenario: Series add up to the net total
- **GIVEN** the "Shared dinner partly repaid" cycle
- **WHEN** dashboard analytics and overview are requested
- **THEN** the sum of `daily_cadence[].expense` and of the heatmap `total_spent` SHALL equal `total_outflow` Rp 300.000
- **AND** the last `cumulative_outflow` of the overview trendline SHALL be Rp 300.000
- **AND** `projected_30d_outflow` SHALL be Rp 300.000.

#### Scenario: Offset spread over spending days
- **GIVEN** Rp 60.000 and Rp 240.000 spent in "Makan Bareng" on two days and a Rp 150.000 reimbursement offsetting it
- **WHEN** 30-day analytics are requested
- **THEN** the two days SHALL show Rp 30.000 and Rp 120.000.

#### Scenario: Ledger summary follows the dashboard
- **GIVEN** the "Shared dinner partly repaid" cycle
- **WHEN** the unfiltered ledger is listed
- **THEN** the cumulative `outflow` SHALL be Rp 300.000 with `gross_outflow` Rp 500.000 and `reimbursement_offset` Rp 200.000
- **AND** filtered to "Makan Bareng" the `outflow` SHALL be Rp 100.000.

#### Scenario: Filters that no report figure covers stay gross
- **WHEN** the same ledger is filtered by account
- **THEN** the `outflow` SHALL be Rp 500.000 and `reimbursement_offset` 0.

## ADDED Requirements

### Requirement: Offset Attribution to Spending Days
Each category's applied offset SHALL be attributed to the days that category was spent on in the window, in proportion to that day's spending, so that no day is negative and the days sum to the window's applied offset.

#### Scenario: Single spending day
- **GIVEN** Rp 90.000 spent in "Makan Bareng" on one day of the window and an applied offset of Rp 30.000 for that category
- **WHEN** 30-day analytics are requested
- **THEN** that day SHALL show Rp 60.000 and no day SHALL be negative.

### Requirement: Reimbursement-Net Burn Rates
The analytics burn rates SHALL subtract the applied offsets of their own rolling window.

#### Scenario: Offset outside the window
- **GIVEN** one reimbursement offset applied inside a burn rate's rolling window and another applied only before that window
- **WHEN** the burn rate is computed
- **THEN** it SHALL subtract the first offset and not the second.

### Requirement: Reimbursement-Net Ledger Outflow
The ledger summary outflow SHALL subtract the window's applied offsets (all, the filtered category's, or the filtered pillar's, floored at zero) when no account, goal, debt, source, review-status, search or uncategorised filter is set, the type filter is empty or expense, and not both a category and a pillar are filtered; otherwise it SHALL stay gross.

#### Scenario: Category and pillar together stay gross
- **GIVEN** the "Shared dinner partly repaid" cycle
- **WHEN** the ledger is filtered to "Makan Bareng" and pillar `need` at the same time
- **THEN** the `outflow` SHALL be the gross spending of the matching rows.

### Requirement: Ledger Summary Gross and Offset Fields
Ledger summaries SHALL return `gross_outflow` and `reimbursement_offset`.

#### Scenario: Fields present on a gross summary
- **WHEN** the ledger is filtered by search text
- **THEN** the summary SHALL contain `gross_outflow` equal to `outflow` and `reimbursement_offset` 0.
