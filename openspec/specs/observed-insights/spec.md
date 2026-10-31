# observed-insights Specification

## Purpose
Give the owner observed, actionable insights on Analisis instead of averages and projections: a fair period comparison, net cash flow per period, and detected recurring charges, each with its period and a drill-down to ledger rows.

## Requirements

### Requirement: Fair period comparison
The system SHALL compare the selected salary cycle or calendar month with the previous one over the same elapsed time from each period's start, shortening both windows to the previous period's length when it is shorter. It SHALL report inflow, outflow and net, spending per Kakeibo pillar, and the top three category and merchant increases and decreases with amounts, and SHALL NOT present a percentage change.

#### Scenario: Same elapsed days
- **GIVEN** the current month observed through its 10th day, previous-month spending of Rp 100.000 on the 5th and Rp 900.000 on the 20th
- **WHEN** the comparison is requested
- **THEN** the previous outflow SHALL be Rp 100.000 and both windows SHALL cover 10 local days

#### Scenario: Previous period without data
- **WHEN** the previous window has no recorded transactions
- **THEN** the response SHALL state `previous_has_data: false`, changes SHALL be `null`, and the interface SHALL say that the previous period has no data

#### Scenario: Drill-down from a change
- **WHEN** the owner selects a category or merchant change
- **THEN** the ledger SHALL open filtered to the current comparison window and that category or merchant

### Requirement: Net cash flow history
The system SHALL list observed inflow, outflow net of reimbursements and net cash flow for the last six completed periods and the current period, using the same period rule as the essential cost, and SHALL label the current period as in progress ("berjalan"). Calendar months SHALL be used when the owner selects the calendar-month basis.

#### Scenario: History with a period in progress
- **WHEN** the history is requested for calendar months
- **THEN** it SHALL contain six completed months and the current month marked in progress, each with net equal to inflow minus outflow

### Requirement: Detected recurring charges
The system SHALL detect recurring charges among expenses of the last six months, grouped by normalized merchant, excluding internal movements, movement fees and investment trades. A group SHALL qualify only with at least three occurrences, a dominant weekly (6–8 days) or monthly (25–35 days) interval and an amount coefficient of variation of at most 0.15.

#### Scenario: Monthly and weekly charges
- **WHEN** a merchant was charged the same amount every 30 days five times and another every 7 days six times
- **THEN** both SHALL be listed with merchant, typical amount, cadence, last date and the next expected date labelled as an estimate

#### Scenario: Irregular grocery merchant
- **WHEN** a merchant's charges vary in amount and interval
- **THEN** it SHALL NOT be listed

#### Scenario: Price change
- **WHEN** a monthly charge rises from Rp 50.000 to Rp 60.000 in its latest occurrence
- **THEN** it SHALL still be listed and SHALL show the amount change

#### Scenario: Monthly-equivalent total
- **WHEN** charges are listed
- **THEN** the response SHALL include their monthly-equivalent total, with weekly charges counted as 52/12 occurrences per month

### Requirement: Recurring charges and plans
A detected charge that already matches an active plan SHALL be marked "sudah ada rencana" with that plan. Any other charge SHALL offer "Jadikan rencana", which opens the plan form prefilled with its name, amount, account, category and schedule for owner confirmation and creates only a non-posting plan.

#### Scenario: Creating a plan from a charge
- **WHEN** the owner confirms "Jadikan rencana" for a monthly charge
- **THEN** a non-posting monthly plan SHALL be created, no transaction SHALL be added, and the charge SHALL then be marked as having a plan

### Requirement: Beranda insight uses the fair comparison
The Beranda insight card, on desktop and phone, SHALL be built from the fair period comparison for the selected salary cycle or calendar month. It SHALL name the period and the number of compared days, state the change in outflow and the largest category increases and decreases as amounts, and SHALL NOT show a percentage change or a savings verdict. Below 3 compared days, or without previous data, it SHALL say so instead of comparing.

#### Scenario: Day one of a salary cycle
- **GIVEN** a salary cycle that started today and a previous cycle with Rp 4.000.000 of spending
- **WHEN** the owner opens Beranda
- **THEN** the card SHALL say that only 1 day has passed and that the comparison appears after 3 days, and SHALL show no decrease, percentage or "HEMAT" badge

#### Scenario: Enough elapsed days
- **GIVEN** the 10th day of the current month with Rp 1.200.000 spent and Rp 1.000.000 spent in the first 10 days of the previous month
- **WHEN** the owner opens Beranda with the calendar-month basis
- **THEN** the card SHALL say that outflow over the first 10 days is Rp 200.000 higher than the previous month over the same days, and SHALL list the largest category increases and decreases with their amounts

#### Scenario: Previous period without transactions
- **WHEN** the previous period has no recorded transactions
- **THEN** the card SHALL say that there is no previous data to compare with and SHALL show only the current outflow

#### Scenario: Comparison unavailable
- **WHEN** the comparison request fails
- **THEN** the card SHALL say that the comparison could not be loaded and SHALL show no figures
