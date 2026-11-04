# observed-insights Specification

## Purpose
Give the owner observed, actionable insights on Analisis instead of averages and projections: a fair period comparison, net cash flow per period, and detected recurring charges, each with its period and a drill-down to ledger rows.

## Requirements

### Requirement: Fair period comparison
`GET /api/dashboard/period-comparison` SHALL compare a period with the previous one over the same elapsed time, shortened to the shorter period, reporting inflow, outflow, net, six pillar entries and the top three category and merchant changes as amounts, never percentages. It SHALL stay for API consumers, deprecated, and the web SHALL NOT present it.

#### Scenario: Same elapsed days
- **GIVEN** the current month observed through its 10th day, previous-month spending of Rp 100.000 on the 5th and Rp 900.000 on the 20th
- **WHEN** the comparison is requested
- **THEN** the previous outflow SHALL be Rp 100.000 and both windows SHALL cover 10 local days

#### Scenario: Previous period without data
- **WHEN** the previous window has no recorded transactions
- **THEN** the response SHALL state `previous_has_data: false` and changes SHALL be `null`

#### Scenario: Drill-down from a change
- **WHEN** an API consumer follows a returned category or merchant change
- **THEN** its window and key SHALL select exactly the ledger rows of the current comparison window for that category or merchant

#### Scenario: Spending so far includes days a shortened comparison leaves out
- **GIVEN** 31 October with Rp 1.400.000 spent today and Rp 200.000 on 5 October, after a 30-day September
- **WHEN** the comparison is requested
- **THEN** the compared current outflow SHALL be Rp 200.000 over 1–30 October, `current_to_date` SHALL report Rp 1.600.000 over 1–31 October, and `current_to_date.outflow` SHALL equal the dashboard overview's `total_outflow` for the same period

#### Scenario: Unclassified spending in the comparison
- **WHEN** both windows hold spending without a pillar
- **THEN** the pillar list SHALL include `unclassified` with its current, previous and change amounts, and the six entries SHALL add up to each window's outflow

#### Scenario: Not shown by the web
- **WHEN** the owner opens Beranda, Analisis or Laporan
- **THEN** no request to `/api/dashboard/period-comparison` SHALL be made and no same-day comparison SHALL be shown

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

### Requirement: Period outflow to date
The comparison SHALL also report the selected period's inflow, outflow and net from its start to now as `current_to_date`, never shortened, covering the same window and using the same reportable-flow rules as the dashboard overview for that period.

#### Scenario: Outflow to date matches the overview
- **WHEN** the comparison is requested for the current salary cycle or calendar month
- **THEN** `current_to_date` SHALL run from the period's first local day to today and its outflow SHALL equal the dashboard overview's `total_outflow` for the same period

### Requirement: Kakeibo Comparison Block
The period comparison SHALL also report a `kakeibo` block with the six Kakeibo buckets for the current and the previous compared windows, in which saving includes net fresh saving through linked movements, computed by the same rule as the dashboard's Kakeibo breakdown.

#### Scenario: Kakeibo buckets with saving transfers
- **GIVEN** Rp 500.000 moved from "Bank Utama" into a goal pocket in the current compared window, and none in the previous one
- **WHEN** the comparison is requested
- **THEN** `kakeibo.current.saving` SHALL include Rp 500.000, `kakeibo.previous.saving` SHALL not, the existing `pillars` entries SHALL keep their values, and `kakeibo.current` SHALL equal the dashboard's Kakeibo breakdown of the same window
