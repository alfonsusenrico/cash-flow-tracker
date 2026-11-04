## MODIFIED Requirements

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
