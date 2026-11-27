## MODIFIED Requirements

### Requirement: Daily Spending by Pillar and Category
Each day of `daily_cadence` SHALL carry `by_pillar` (need, want, culture, unexpected, unclassified) and `by_category` (category id, name and amount), computed from the same full-price rows as its `spending`, each adding up exactly to it. The table SHALL offer "Per pilar" (the pillars and Total) and "Per kategori" (the period's five largest categories, "Lainnya" and Total), and SHALL scroll inside its box with the date fixed when narrow.

#### Scenario: Pillars add up
- **GIVEN** a day with Rp 412.000 of needs, Rp 677.649 of wants and a reimbursement of Rp 50.000 applied to a want
- **WHEN** the period's analytics are requested
- **THEN** that day's `by_pillar` SHALL be need 412.000 and want 677.649, and their sum SHALL equal the day's `spending`

#### Scenario: Per category columns
- **GIVEN** a period with eight spending categories
- **WHEN** the owner switches the table to "Per kategori"
- **THEN** the columns SHALL be the five largest categories of the period, "Lainnya" holding the other three, and Total, and each row SHALL add up to its Total

#### Scenario: Narrow screen
- **WHEN** the table is shown at 390 px in "Per pilar"
- **THEN** the table SHALL scroll sideways inside its box with the date column fixed, and the page SHALL not scroll sideways

## ADDED Requirements

### Requirement: Daily Chart Names the Reimbursed Amount
Analytics SHALL return `spending_totals` with `gross` (the sum of the days' `spending`), `reimbursed` and `spending` (the period's Pengeluaran), where `gross − reimbursed = spending`. When `reimbursed` is above zero, Ringkasan's "Pengeluaran harian" SHALL show beside its scope line "{gross} dibelanjakan · −{reimbursed} diganti" ("spent", "reimbursed" in English), masked when balances are hidden; otherwise it SHALL not.

#### Scenario: Bars explained by the line
- **GIVEN** a period with Rp 1.250.000 of full-price days and Rp 200.000 reimbursed
- **WHEN** Ringkasan is shown
- **THEN** the hero SHALL read Rp 1.050.000 and the daily chart SHALL show "Rp 1.250.000 dibelanjakan · −Rp 200.000 diganti"

#### Scenario: Nothing reimbursed
- **WHEN** nothing was reimbursed in the period
- **THEN** the daily chart SHALL show no such line, and the days SHALL add up to the hero
