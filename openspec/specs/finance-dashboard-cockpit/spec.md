# finance-dashboard-cockpit Specification

## Purpose
Present financial summaries, cash-flow trends, budget comparisons and account allocation in an analytical dashboard.

## Requirements

### Requirement: Executive KPI Summary Ribbon
The dashboard SHALL display an executive summary ribbon containing.

#### Scenario: Required behavior and constraints
- **WHEN** the dashboard displays its financial summary
- **THEN** the following required behavior and constraints hold:

  The dashboard SHALL display an executive summary ribbon containing:
  1. `Liquid Balance (Kas & Bank)`: total spendable balance across active cash, bank, e-wallet, and wallet accounts, excluding investment instruments and any amount below zero that is pending reconciliation from available-to-spend calculations.
  2. `Total Inflow`: actual non-transfer income recorded within the active timeframe.
  3. `Total Outflow`: actual non-transfer expenses recorded within the active timeframe.
  4. `Surplus Arus Kas`: `Inflow - Outflow` for the active timeframe.
  5. `Ketahanan Dana`: emergency fund coverage against the configured target multiplier.
  6. Total balance and investment balance SHALL be returned and labeled separately from liquid balance.

#### Scenario: Separating liquid and investment assets
- **WHEN** a user has 10,000,000 IDR in bank accounts and 25,000,000 IDR in investment instruments
- **THEN** the liquid KPI reports 10,000,000 IDR and does not report the 35,000,000 IDR total balance as liquid

#### Scenario: Displaying executive KPIs
- **WHEN** the user views the Overview dashboard
- **THEN** liquid balance excludes investments, inflow and outflow exclude internal movements, and Ketahanan Dana uses the configured multiplier

### Requirement: Cumulative Cash Flow Area Trendline
The dashboard SHALL render an interactive Recharts Area/Line chart visualizing cumulative cash flow across the selected timeframe:
1. An emerald green area curve for cumulative inflow.
2. A coral/rose area curve for cumulative outflow.
3. Hovering over any point SHALL display a detailed tooltip with the date, cumulative values, and net variance.

#### Scenario: Inspecting cumulative trendline point
- **WHEN** the user hovers over a date on the cumulative cash flow chart
- **THEN** the tooltip displays the exact cumulative income, cumulative expense, and net spread on that date

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

### Requirement: Observed dashboard planning information
The dashboard SHALL display observed period cash flow, configured budget remaining and separately identified upcoming commitments without representing them as guaranteed funds available to spend.

#### Scenario: Unknown next salary date
- **WHEN** the next salary date is unknown
- **THEN** no daily safe-to-spend figure SHALL be inferred from a guessed cycle end

#### Scenario: Upcoming commitments
- **WHEN** active expense plans are due within the next 30 days or overdue
- **THEN** the dashboard SHALL list them with their dates and a total labelled with the 30-day window and SHALL state that plans are reminders, not payments

#### Scenario: Budget remaining
- **WHEN** a monthly budget is configured and the period is a calendar month or salary cycle
- **THEN** the dashboard SHALL show the budget less observed net spending for that period, and SHALL omit it for rolling 30- or 90-day windows
