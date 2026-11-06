# finance-dashboard-cockpit Specification

## Purpose
Present financial summaries, cash-flow trends, budget comparisons and account allocation in an analytical dashboard.

## Requirements

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
