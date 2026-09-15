## ADDED Requirements

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
2. `runway_days`: `round(total_assets / max(1, daily_burn_rate))`.
3. The dashboard SHALL render a runway forecast card indicating the months of financial cushion available.

#### Scenario: Computing healthy runway
- **WHEN** a user has 30,000,000 IDR liquid and spends an average of 300,000 IDR/day
- **THEN** the system computes a runway of approximately 100 days (~3.3 months) with a positive stability indicator
