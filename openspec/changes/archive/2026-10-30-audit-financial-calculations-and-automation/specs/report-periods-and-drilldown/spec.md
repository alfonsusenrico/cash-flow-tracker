## MODIFIED Requirements

### Requirement: Period Controls
Insights SHALL offer a salary-cycle / calendar-month switch and the ledger a period selector (all, salary cycle, calendar month) with a previous/next stepper. Salary cycles are bounded by confirmed salary receipts; the current cycle has no known end, and a cycle without a confirmed receipt SHALL fall back to the labelled calendar month instead of a fixed payday.

#### Scenario: Switching insights to calendar months
- **WHEN** the owner switches insights from salary cycle to calendar month
- **THEN** the figures SHALL be requested with `timeframe=month` and labelled with the month name.

#### Scenario: Stepping to a cycle that is not available
- **WHEN** the owner steps to a cycle before the first confirmed salary receipt
- **THEN** the figures SHALL use the calendar month for that step and the label SHALL say the salary cycle is unavailable
