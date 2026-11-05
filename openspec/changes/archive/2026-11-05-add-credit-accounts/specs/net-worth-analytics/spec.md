## MODIFIED Requirements

### Requirement: Net Worth & Liquidity Aggregation
Net worth SHALL use Beranda's cycle-summary helper: `liquid` (cash, bank and wallet, each account once, without negative balances awaiting reconciliation), `investment`, `total_assets` = `liquid + investment`, `reconciliation_pending`, `credit_owed` (what credit accounts owe, as a positive amount), `total_liabilities` (active `obligations.remaining_amount` plus `credit_owed`) and `net_worth` = `total_assets - reconciliation_pending - total_liabilities`.

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

#### Scenario: Another owner's accounts
- **WHEN** two owners each have accounts and debts
- **THEN** each owner's figures SHALL include only that owner's accounts and debts

#### Scenario: Credit owed is utang
- **GIVEN** Rp 10.000.000 in liquid accounts, no investment, an obligation of Rp 2.200.200 and SPayLater owing Rp 470.000
- **WHEN** net worth is computed
- **THEN** `credit_owed` SHALL be 470000, `investment` SHALL be 0, `total_liabilities` SHALL be 2670200, `net_worth` SHALL be 7329800, and `lines` SHALL include one line for SPayLater
