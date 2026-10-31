## MODIFIED Requirements

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

#### Scenario: Spending so far includes days a shortened comparison leaves out
- **GIVEN** 31 October with Rp 1.400.000 spent today and Rp 200.000 on 5 October, after a 30-day September
- **WHEN** the comparison is requested
- **THEN** the compared current outflow SHALL be Rp 200.000 over 1–30 October, `current_to_date` SHALL report Rp 1.600.000 over 1–31 October, and `current_to_date.outflow` SHALL equal the dashboard overview's `total_outflow` for the same period

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
- **THEN** the card SHALL say that there is no previous data to compare with and SHALL show only the current outflow to date, which SHALL match the Beranda spending summary for the same period

#### Scenario: Comparison unavailable
- **WHEN** the comparison request fails
- **THEN** the card SHALL say that the comparison could not be loaded and SHALL show no figures

## ADDED Requirements

### Requirement: Period outflow to date
The comparison SHALL also report the selected period's inflow, outflow and net from its start to now as `current_to_date`, never shortened, covering the same window and using the same reportable-flow rules as the dashboard overview for that period.

#### Scenario: Outflow to date matches the overview
- **WHEN** the comparison is requested for the current salary cycle or calendar month
- **THEN** `current_to_date` SHALL run from the period's first local day to today and its outflow SHALL equal the dashboard overview's `total_outflow` for the same period

### Requirement: Beranda spending so far
Any figure the Beranda insight card calls spending so far ("sejauh ini") SHALL be the period's outflow to date, not the shortened comparison figure.

#### Scenario: Spending so far after a shortened comparison
- **GIVEN** 31 October after a 30-day September without transactions, no spending on 1–30 October and Rp 3.600.000 spent today
- **WHEN** the owner opens Beranda with the calendar-month basis
- **THEN** the card SHALL say "Uang keluar sejauh ini Rp 3.600.000", not the compared Rp 0
