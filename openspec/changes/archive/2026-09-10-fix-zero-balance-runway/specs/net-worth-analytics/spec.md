## MODIFIED Requirements

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
