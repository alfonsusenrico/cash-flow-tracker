## MODIFIED Requirements

### Requirement: Cash Flow Runway Forecast
The system SHALL express the emergency runway in days and months as coverage of the essential monthly cost defined by `emergency-cost-estimation`, without a trailing burn rate or a placeholder value.

#### Scenario: Required behavior and constraints
- **WHEN** the system calculates and displays the runway
- **THEN** the following required behavior and constraints hold:

  The system SHALL compute the runway in days and months from the emergency balance and the essential monthly cost:
  1. `runway_months` and `coverage_months`: the balance divided by the essential monthly cost, rounded to one decimal.
  2. `runway_days`: the balance times 30.4 divided by the essential monthly cost, rounded. If the balance is 0 or less and the cost is known, `runway_days` SHALL be `0`.
  3. When the essential cost is unknown or zero, `runway_days`, `runway_months` and `coverage_months` SHALL be `null` with status `unknown`; the system SHALL NOT return 999 days or 99.9 months.
  4. The accounts view SHALL render the runway with its balance basis and cost basis; if the balance is zero it SHALL indicate zero runway without false solvency indicators, and if the runway is unknown it SHALL say so.

#### Scenario: Computing runway with zero liquid balance
- **WHEN** a user has 0 IDR across liquid accounts, no designated reserve and a known essential cost
- **THEN** the system reports `runway_days: 0`, `runway_months: 0.0`, and does not display an "Aman" or "> 1 Tahun" solvency badge

#### Scenario: Computing healthy runway
- **WHEN** a user has 30,000,000 IDR of emergency balance and an essential monthly cost of 5,000,000 IDR
- **THEN** the system computes a runway of 6.0 months (182 days) with a positive stability indicator

#### Scenario: Runway with an unknown cost
- **WHEN** a user has a positive balance but no completed period and no `need` budget
- **THEN** the runway is `null` with status `unknown` and the accounts view shows that it cannot be calculated yet
