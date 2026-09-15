# net-worth-analytics Specification

## Purpose
TBD - created by archiving change personal-finance-dashboard-os. Update Purpose after archive.
## Requirements
### Requirement: Net Worth & Liquidity Aggregation
The system SHALL aggregate liquid assets and outstanding liabilities to compute net worth:
1. `total_assets`: Sum of all liquid account balances (`cash`, `bank`, `wallet`).
2. `total_liabilities`: Sum of all active `obligations.remaining_amount`.
3. `net_worth`: `total_assets - total_liabilities`.
4. The system SHALL expose `GET /api/dashboard/net-worth`.

#### Scenario: Calculating net worth
- **WHEN** a user has 25,000,000 IDR across accounts and 5,000,000 IDR in outstanding debts
- **THEN** the system reports `total_assets: 25,000,000`, `total_liabilities: 5,000,000`, and `net_worth: 20,000,000`

### Requirement: Cash Flow Runway Forecast
The system SHALL compute estimated cash runway in days and months:
1. `daily_burn_rate`: Trailing 30-day average daily outflow.
2. `runway_days`: If `total_assets <= 0`, `runway_days` SHALL be `0`. If `total_assets > 0` and `daily_burn_rate > 0`, `runway_days` SHALL be `round(total_assets / daily_burn_rate)`. If `total_assets > 0` and `daily_burn_rate == 0`, `runway_days` SHALL be `999`.
3. The dashboard and accounts views SHALL render a runway forecast card indicating the status: if liquid balance is zero, it SHALL indicate zero runway without displaying false solvency indicators.

#### Scenario: Computing runway with zero liquid balance
- **WHEN** a user has 0 IDR across liquid accounts and no recent spending
- **THEN** the system reports `runway_days: 0`, `runway_months: 0.0`, and does not display an "Aman" or "> 1 Tahun" solvency badge

#### Scenario: Computing healthy runway
- **WHEN** a user has 30,000,000 IDR liquid and spends an average of 300,000 IDR/day
- **THEN** the system computes a runway of approximately 100 days (~3.3 months) with a positive stability indicator

