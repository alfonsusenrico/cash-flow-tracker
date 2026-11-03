## MODIFIED Requirements

### Requirement: Net Worth & Liquidity Aggregation
The system SHALL aggregate assets and outstanding liabilities to compute net worth with the same helper that Beranda's cycle summary uses, so every page shows one Kekayaan bersih:
1. `liquid`: the liquid balance (`cash`, `bank`, `wallet`), each account counted once, a parent's balance already holding its pockets, and negative balances that wait for reconciliation left out.
2. `investment`: the investment balance at its last recorded value.
3. `total_assets`: `liquid + investment`.
4. `reconciliation_pending`: the sum of negative balances waiting for reconciliation, as a positive amount that is still owed.
5. `total_liabilities`: the sum of all active `obligations.remaining_amount` (not archived, remaining above zero).
6. `net_worth`: `total_assets - reconciliation_pending - total_liabilities`, equal to `GET /api/dashboard/cycle-summary`'s `net_worth.total` at the same moment.
7. `lines`: the explanation lines of the figure in the cycle summary's format (`key`, `label`, `amount`), one per part and one per active debt.
8. The system SHALL expose `GET /api/dashboard/net-worth` with these fields in addition to its existing `accounts`, `obligations` and `runway` fields. The web SHALL show these server figures and SHALL NOT compute or substitute them; when the request fails it SHALL say that the figure cannot be calculated instead of showing Rp 0.

#### Scenario: Calculating net worth
- **WHEN** a user has 25,000,000 IDR across accounts and 5,000,000 IDR in outstanding debts
- **THEN** the system reports `total_assets: 25,000,000`, `total_liabilities: 5,000,000`, and `net_worth: 20,000,000`

#### Scenario: Investments are assets
- **WHEN** a user has Rp 20.000.000 in liquid accounts, a mutual fund valued at Rp 6.532.590 and no debt
- **THEN** the system reports `liquid: 20000000`, `investment: 6532590`, `total_assets: 26532590` and `net_worth: 26532590`

#### Scenario: Same figure as Beranda
- **GIVEN** a parent with two pockets, an investment product, a pocket at −Rp 50.000 waiting for reconciliation and one active debt
- **WHEN** `GET /api/dashboard/net-worth` and `GET /api/dashboard/cycle-summary` are called at the same moment
- **THEN** `net_worth` SHALL equal `net_worth.total`, `reconciliation_pending` SHALL be 50000, and no pocket balance SHALL be counted twice

#### Scenario: Net worth unavailable on the page
- **WHEN** `GET /api/dashboard/net-worth` fails
- **THEN** Rekening & Kantong SHALL show "Kekayaan bersih belum dapat dihitung" with a retry action and SHALL NOT show Rp 0 or a sum computed in the browser

#### Scenario: Another owner's accounts
- **WHEN** two owners each have accounts and debts
- **THEN** each owner's figures SHALL include only that owner's accounts and debts

### Requirement: Cash Flow Runway Forecast
The system SHALL express the emergency runway in days and months as coverage of the essential monthly cost defined by `emergency-cost-estimation`, without a trailing burn rate or a placeholder value.

#### Scenario: Required behavior and constraints
- **WHEN** the system calculates and displays the runway
- **THEN** the following required behavior and constraints hold:

  The system SHALL compute the runway in days and months from the emergency balance and the essential monthly cost:
  1. `runway_months` and `coverage_months`: the balance divided by the essential monthly cost, rounded to one decimal.
  2. `runway_days`: the balance times 30.4 divided by the essential monthly cost, rounded. If the balance is 0 or less and the cost is known, `runway_days` SHALL be `0`.
  3. When the essential cost is unknown or zero, `runway_days`, `runway_months` and `coverage_months` SHALL be `null` with status `unknown`; the system SHALL NOT return 999 days or 99.9 months.
  4. The Target & Tagihan page SHALL render the runway as "Ketahanan Dana" with its balance basis and cost basis; if the balance is zero it SHALL indicate zero runway without false solvency indicators, and if the runway is unknown it SHALL say so. Rekening & Kantong SHALL NOT repeat the runway card.

#### Scenario: Computing runway with zero liquid balance
- **WHEN** a user has 0 IDR across liquid accounts, no designated reserve and a known essential cost
- **THEN** the system reports `runway_days: 0`, `runway_months: 0.0`, and does not display an "Aman" or "> 1 Tahun" solvency badge

#### Scenario: Computing healthy runway
- **WHEN** a user has 30,000,000 IDR of emergency balance and an essential monthly cost of 5,000,000 IDR
- **THEN** the system computes a runway of 6.0 months (182 days) with a positive stability indicator

#### Scenario: Runway with an unknown cost
- **WHEN** a user has a positive balance but no completed period and no `need` budget
- **THEN** the runway is `null` with status `unknown` and Target & Tagihan shows that it cannot be calculated yet
