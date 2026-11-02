# finance-dashboard-cockpit Specification

## Purpose
Present financial summaries, cash-flow trends, budget comparisons and account allocation in an analytical dashboard.

## Requirements

### Requirement: Executive KPI Summary Ribbon
Beranda SHALL display a KPI strip of four explainable figures for the Beranda period (`beranda-cycle-summary`): "Aman dipakai hari ini", "Keluar", "Masuk" and "Kekayaan bersih". Each SHALL carry its label, an icon tile and at most one comparison chip.

#### Scenario: Required behavior and constraints
- **WHEN** the dashboard displays its financial summary
- **THEN** the following required behavior and constraints hold:

  Beranda SHALL display a KPI strip containing:
  1. `Aman dipakai hari ini`: the safe-to-spend figure, or its unavailable state in words, with the days left to the expected salary date as its caption. It has no comparison chip, because yesterday's figure cannot be rebuilt exactly once plans have been linked since.
  2. `Keluar`: the Beranda keluar (Kakeibo total); its chip compares with the same number of elapsed days of the previous period from the fair period comparison, as a signed amount, and is omitted below 3 compared days or without previous data.
  3. `Masuk`: reportable income of the period, excluding movements, investments and reimbursements; its chip follows the same fair-comparison rule.
  4. `Kekayaan bersih`: total assets less active debts; its explanation lists liquid funds, investments and each active debt, counting parent and child accounts once. It has no comparison chip until a historical valuation exists.
  5. Liquid balance, investment balance and total balance SHALL keep being returned and labelled separately by the overview and net-worth APIs.
  6. Ketahanan Dana SHALL be shown in its own Beranda card with the configured target multiplier.

#### Scenario: Separating liquid and investment assets
- **WHEN** a user has 10,000,000 IDR in bank accounts, 25,000,000 IDR in investment instruments and a 2,400,000 IDR active debt
- **THEN** "Kekayaan bersih" shows 32,600,000 IDR, and its explanation lists 10,000,000 IDR liquid and 25,000,000 IDR investments separately and subtracts the debt

#### Scenario: Displaying executive KPIs
- **WHEN** the user views Beranda
- **THEN** keluar and masuk exclude internal movements between the owner's own non-goal, non-investment accounts, every chip is a signed rupiah amount with its comparison named, and no chip shows a percentage

#### Scenario: Comparison too short
- **WHEN** the current period has run for 2 days
- **THEN** the Keluar and Masuk chips are omitted and their explanations say that the comparison appears after 3 days

### Requirement: Inline Category Budget Editing
The `/insights` Spending Analytics page SHALL allow users to set or update any category's monthly spending limit directly from the Category Variance table:
1. Clicking on a category's budget cell or edit button SHALL open an inline popover or modal.
2. The user can enter a new monthly budget limit (or clear it to "No limit").
3. Saving SHALL call `PATCH /api/categories/{id}` and immediately recalculate variance metrics, budget progress bars, and over-budget counts.

#### Scenario: Setting a category budget limit
- **WHEN** the user clicks "No limit" on the "Food & Dining" category and enters `3000000`
- **THEN** the system sets the category's `monthly_budget` to 3,000,000 IDR and updates the progress bar and remaining variance immediately

### Requirement: Asset Allocation Breakdown Display Card
The system SHALL provide a dedicated summary display card showing the separation between liquid and investment balances:
1. `Kas & Bank (Likuid Operasional)`: Sum of all non-investment cash, bank, and e-wallet accounts.
2. `Portofolio Investasi`: Sum of all investment instruments (stocks, mutual funds, gold, RDN).
3. `Total Saldo Gabungan`: Total net balance across all accounts.

#### Scenario: Inspecting asset allocation card
- **WHEN** the user views the Asset Breakdown card on the Dashboard or Accounts view
- **THEN** it renders distinct totals for Liquid Cash, Investment Portfolio, and Aggregate Balance

### Requirement: Observed dashboard planning information
The dashboard SHALL display observed period cash flow, configured budget remaining and separately identified upcoming commitments without representing them as guaranteed funds. A daily safe-to-spend figure SHALL come only from the expected salary date rule of `beranda-cycle-summary`, never from a guessed cycle end.

#### Scenario: Unknown next salary date
- **WHEN** the next salary date is unknown
- **THEN** no daily safe-to-spend figure SHALL be inferred from a guessed cycle end, and Beranda SHALL show the remaining amount with the reason instead

#### Scenario: Expected salary date from regular receipts
- **WHEN** the owner's confirmed salary receipts give an expected next salary date after today
- **THEN** Beranda SHALL show "Aman dipakai hari ini" and its explanation SHALL name the expected date as an estimate and the receipts it is based on

#### Scenario: Upcoming commitments
- **WHEN** active expense plans are due within the next 30 days or overdue
- **THEN** the dashboard SHALL list them with their dates and a total labelled with the 30-day window and SHALL state that plans are reminders, not payments

#### Scenario: Budget remaining
- **WHEN** a monthly budget is configured and the period is a calendar month or salary cycle
- **THEN** the dashboard SHALL show the budget less observed net spending for that period, and SHALL omit it for rolling 30- or 90-day windows
