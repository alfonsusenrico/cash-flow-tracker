## MODIFIED Requirements

### Requirement: Reimbursements in Cycle Spending and Safe-to-Spend
Pulse cycle spending SHALL be reduced by the applied offsets of categories whose pillar is not saving, floored at zero, and safe-to-spend SHALL use the reduced figure. Pulse SHALL return the reduction as `cycle_reimbursed`. Pulse today's spending, the pulse insights histogram and the emergency-fund runway SHALL remain gross spending; dashboard daily series, heatmaps and burn rates SHALL follow the requirement "Reimbursement-Net Series and Ledger Summary".

#### Scenario: Repayment restores safe-to-spend
- **GIVEN** a monthly budget of Rp 3.000.000 and the cycle of the "Shared dinner partly repaid" scenario
- **WHEN** pulse is requested with R days remaining in the cycle
- **THEN** `cycle_spent` SHALL be Rp 300.000 and `cycle_reimbursed` Rp 200.000
- **AND** `safe_to_spend_today` SHALL be Rp 2.700.000 divided by R, rounded down.

## ADDED Requirements

### Requirement: Reimbursement-Net Series and Ledger Summary
Dashboard daily series (analytics `daily_cadence`, overview `cumulative_trendline` and `burn_cadence`), the analytics day-of-week heatmap and the analytics burn rates SHALL subtract applied reimbursement offsets. Each category's applied offset SHALL be attributed to the days that category was spent on in the window, in proportion to that day's spending, so that no day is negative and the days sum to the window's applied offset. Burn rates SHALL subtract the offsets of their own rolling window. The ledger summary outflow SHALL subtract the window's applied offsets (all, the filtered category's, or the filtered pillar's, floored at zero) when no account, goal, debt, source, review-status, search or uncategorised filter is set, the type filter is empty or expense, and not both a category and a pillar are filtered; otherwise it SHALL stay gross. Ledger summaries SHALL return `gross_outflow` and `reimbursement_offset`.

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
