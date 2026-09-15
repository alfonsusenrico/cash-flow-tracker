# finance-dashboard-cockpit Specification

## Purpose
TBD - created by archiving change personal-finance-dashboard-os. Update Purpose after archive.
## Requirements
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

### Requirement: Cumulative Cash Flow Area Trendline
The dashboard SHALL render an interactive Recharts Area/Line chart visualizing cumulative cash flow across the selected timeframe:
1. An emerald green area curve for cumulative inflow.
2. A coral/rose area curve for cumulative outflow.
3. Hovering over any point SHALL display a detailed tooltip with the date, cumulative values, and net variance.

#### Scenario: Inspecting cumulative trendline point
- **WHEN** the user hovers over a date on the cumulative cash flow chart
- **THEN** the tooltip displays the exact cumulative income, cumulative expense, and net spread on that date

### Requirement: Burn Cadence Bar Chart with Allowance Benchmark
The dashboard SHALL render an interactive Bar chart displaying daily spending throughout the cycle, with a reference line representing the daily safe-to-spend allowance budget ceiling:
1. When a user has configured a monthly spending budget, the daily allowance SHALL equal `(monthly_spending_budget - current_month_expenses) / remaining_cycle_days`.
2. When only category budgets are defined, the sum of category budgets SHALL be used.
3. When neither is configured, the system SHALL display an unconfigured state prompting the user to set a budget, rather than applying an arbitrary fallback formula.

#### Scenario: Displaying daily allowance with configured budget
- **WHEN** a user with a 15,000,000 IDR monthly budget has spent 3,000,000 IDR with 15 days remaining in the cycle
- **THEN** the daily allowance benchmark line renders at 800,000 IDR/day

### Requirement: Category Spend Donut & Variance Matrix
The dashboard SHALL render an interactive Donut chart displaying the proportion of spending by category, coupled with a ranked variance list showing actual spend versus monthly budget ceiling.

#### Scenario: Selecting category slice
- **WHEN** the user hovers over or taps a donut slice
- **THEN** the center label highlights the category name, total amount, and percentage share

### Requirement: Inline Category Budget Editing
The `/insights` Spending Analytics page SHALL allow users to set or update any category's monthly spending limit directly from the Category Variance table:
1. Clicking on a category's budget cell or edit button SHALL open an inline popover or modal.
2. The user can enter a new monthly budget limit (or clear it to "No limit").
3. Saving SHALL call `PATCH /api/categories/{id}` and immediately recalculate variance metrics, budget progress bars, and over-budget counts.

#### Scenario: Setting a category budget limit
- **WHEN** the user clicks "No limit" on the "Food & Dining" category and enters `3000000`
- **THEN** the system sets the category's `monthly_budget` to 3,000,000 IDR and updates the progress bar and remaining variance immediately

### Requirement: Historical Payday Cycle Navigation
The dashboard header and filter controls SHALL support navigating backwards and forwards through historical payday cycles:
1. The user can click previous (`‹`) and next (`›`) cycle chevrons.
2. Navigating cycles SHALL query `/api/dashboard/overview` and `/api/dashboard/analytics` with the corresponding cycle offset.
3. The timeframe label SHALL dynamically update to reflect the specific date range of the active historical cycle (e.g. "Cycle: Jul 25 - Aug 24").

#### Scenario: Viewing the previous pay cycle
- **WHEN** the user clicks the previous cycle button (`‹`)
- **THEN** all dashboard charts, KPIs, and cadence bars update to reflect the cash flow data of the previous cycle window

### Requirement: Bibit-Style Hero Portfolio Action Pills
The Overview dashboard hero area SHALL feature high-visibility action pills inspired by the Bibit dashboard:
1. `+ Record` (`N` quick capture trigger)
2. `⇄ Transfer` (1-tap account transfer trigger)
3. `🎯 New Goal` (savings target creation trigger)
4. Key portfolio figures displaying both nominal values and percentage changes.

#### Scenario: Triggering transfer from hero action pill
- **WHEN** the user taps the `⇄ Transfer` hero action pill on the Overview screen
- **THEN** the transfer modal opens with primary accounts preselected for immediate execution

### Requirement: Asset Allocation Breakdown Display Card
The system SHALL provide a dedicated summary display card showing the separation between liquid and investment balances:
1. `Kas & Bank (Likuid Operasional)`: Sum of all non-investment cash, bank, and e-wallet accounts.
2. `Portofolio Investasi`: Sum of all investment instruments (stocks, mutual funds, gold, RDN).
3. `Total Saldo Gabungan`: Total net balance across all accounts.

#### Scenario: Inspecting asset allocation card
- **WHEN** the user views the Asset Breakdown card on the Dashboard or Accounts view
- **THEN** it renders distinct totals for Liquid Cash, Investment Portfolio, and Aggregate Balance

