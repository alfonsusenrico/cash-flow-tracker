## MODIFIED Requirements

### Requirement: Executive KPI Summary Ribbon
The dashboard SHALL display an executive summary ribbon containing:
1. `Liquid Balance (Kas & Bank)`: Total spendable liquid balance across active accounts of type `cash`, `bank`, and `ewallet` (excluding investment assets).
2. `Total Inflow`: Actual income recorded within the active timeframe.
3. `Total Outflow`: Expenses recorded within the active timeframe.
4. `Surplus Arus Kas`: Nominal net cashflow `(Inflow - Outflow)` within the active timeframe.
5. `Ketahanan Dana`: Emergency fund coverage ratio evaluated against the user's configured target multiplier (e.g. 6x or 8x monthly primary living costs).

#### Scenario: Displaying executive KPIs
- **WHEN** the user views the Overview dashboard
- **THEN** the liquid balance card reflects only physical cash and bank accounts, and Ketahanan Dana reflects the user's personalized multiplier target

### Requirement: Burn Cadence Bar Chart with Allowance Benchmark
The dashboard SHALL render an interactive Bar chart displaying daily spending throughout the cycle, with a reference line representing the daily safe-to-spend allowance budget ceiling:
1. When a user has configured a monthly spending budget, the daily allowance SHALL equal `(monthly_spending_budget - current_month_expenses) / remaining_cycle_days`.
2. When only category budgets are defined, the sum of category budgets SHALL be used.
3. When neither is configured, the system SHALL display an unconfigured state prompting the user to set a budget, rather than applying an arbitrary fallback formula.

#### Scenario: Displaying daily allowance with configured budget
- **WHEN** a user with a 15,000,000 IDR monthly budget has spent 3,000,000 IDR with 15 days remaining in the cycle
- **THEN** the daily allowance benchmark line renders at 800,000 IDR/day

## ADDED Requirements

### Requirement: Asset Allocation Breakdown Display Card
The system SHALL provide a dedicated summary display card showing the separation between liquid and investment balances:
1. `Kas & Bank (Likuid Operasional)`: Sum of all non-investment cash, bank, and e-wallet accounts.
2. `Portofolio Investasi`: Sum of all investment instruments (stocks, mutual funds, gold, RDN).
3. `Total Saldo Gabungan`: Total net balance across all accounts.

#### Scenario: Inspecting asset allocation card
- **WHEN** the user views the Asset Breakdown card on the Dashboard or Accounts view
- **THEN** it renders distinct totals for Liquid Cash, Investment Portfolio, and Aggregate Balance
