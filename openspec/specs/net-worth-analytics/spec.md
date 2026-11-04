# net-worth-analytics Specification

## Purpose
Aggregate net worth and liquidity and expose cash-flow runway forecasts for financial planning.

## Requirements

### Requirement: Net Worth & Liquidity Aggregation
Net worth SHALL use Beranda's cycle-summary helper, so every page shows one Kekayaan bersih: `liquid` (cash, bank and wallet, each account once, without negative balances awaiting reconciliation), `investment` (last recorded values), `total_assets` = `liquid + investment`, `reconciliation_pending` (those negative balances as a positive amount owed), `total_liabilities` (active `obligations.remaining_amount`) and `net_worth` = `total_assets - reconciliation_pending - total_liabilities`.

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

### Requirement: Net Worth Endpoint and Display
`GET /api/dashboard/net-worth` SHALL return `liquid`, `investment`, `total_assets`, `reconciliation_pending`, `total_liabilities`, `net_worth` and `lines` (the cycle summary's `key`, `label`, `amount` lines, one per part and per active debt) besides its existing fields. The web SHALL show these server figures without computing them, and when the request fails it SHALL say the figure cannot be calculated instead of showing Rp 0.

#### Scenario: Net worth unavailable on the page
- **WHEN** `GET /api/dashboard/net-worth` fails
- **THEN** Rekening & Kantong SHALL show "Kekayaan bersih belum dapat dihitung" with a retry action and SHALL NOT show Rp 0 or a sum computed in the browser

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

### Requirement: Asset Composition on Rekening
Rekening & Kantong SHALL show "Komposisi aset": a segmented bar and a legend of every top-level account with its share of Total aset and its amount, from the server's figures, ordered by amount. Debt SHALL NOT appear in this card. Segments SHALL carry the account colours and each legend row SHALL name its account, so the bar is never the only carrier of the values.

#### Scenario: Shares add up
- **GIVEN** Total aset Rp 68.763.549 across eight top-level accounts
- **WHEN** the owner opens Rekening & Kantong
- **THEN** the eight legend amounts SHALL add up to Rp 68.763.549, and no debt line SHALL be shown in the card

#### Scenario: Net worth unavailable
- **WHEN** the net worth request fails
- **THEN** the card SHALL say that the composition cannot be shown and SHALL show no Rp 0

### Requirement: Investment Gain in the Account List
`GET /api/accounts` SHALL add `group_capital_gain` and `group_cost_basis` to a parent account whose children are investments, computed on the server with the per-product capital gain rule. Rekening SHALL show each product's value with its `capital_gain` and `capital_gain_pct`, and each platform with its group gain, marked by a sign and ▲ or ▼, never by colour alone.

#### Scenario: Gain and loss on one platform
- **GIVEN** a platform holding one stock with a loss of Rp 480.000 and one with a gain of Rp 192.000
- **WHEN** the owner opens Rekening & Kantong
- **THEN** the product rows SHALL read "▼ −Rp 480.000 (−8,28%)" and "▲ +Rp 192.000 (+15,24%)", and the platform head SHALL read "▼ −Rp 288.000" with its percentage from `group_cost_basis`

#### Scenario: Missing cost basis
- **WHEN** one product of a platform has no recorded cost basis
- **THEN** that product SHALL say "Modal belum dicatat", `group_capital_gain` SHALL be `null`, and the platform head SHALL say "Modal belum lengkap" instead of a gain
