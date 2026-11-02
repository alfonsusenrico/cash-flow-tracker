## MODIFIED Requirements

### Requirement: Fair period comparison
The system SHALL compare the selected salary cycle or calendar month with the previous one over the same elapsed time from each period's start, shortening both windows to the previous period's length when it is shorter. It SHALL report inflow, outflow and net, spending per Kakeibo pillar with unclassified spending as a sixth `unclassified` entry, and the top three category and merchant increases and decreases with amounts, and SHALL NOT present a percentage change.

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

#### Scenario: Unclassified spending in the comparison
- **WHEN** both windows hold spending without a pillar
- **THEN** the pillar list SHALL include `unclassified` with its current, previous and change amounts, and the six entries SHALL add up to each window's outflow
