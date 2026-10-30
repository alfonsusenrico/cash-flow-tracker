# financial-reporting-consistency Specification

## Purpose
Ensure financial figures across clients are reconcilable to the same ledger and period.

## Requirements

### Requirement: Shared ledger figures
The system SHALL calculate observed inflow, gross outflow, reimbursement offsets, net outflow and net cash flow from a common reporting contract with explicit periods and timezone.

#### Scenario: Same period across clients
- **WHEN** web and mobile request the same financial summary and date range
- **THEN** both SHALL show the same server ledger figures and reporting scope

#### Scenario: Transfers and reimbursements
- **WHEN** a period includes an internal movement, fee and qualifying reimbursement
- **THEN** the movement legs SHALL not inflate income or spending, the fee SHALL be counted once, and the reimbursement treatment SHALL be displayed

#### Scenario: Assets and liabilities
- **WHEN** a report presents net worth
- **THEN** it SHALL distinguish total assets from net worth after active liabilities and SHALL not label total account balances alone as net worth

#### Scenario: Net worth is unavailable
- **WHEN** a client cannot obtain liabilities or a valid net-worth calculation
- **THEN** it SHALL show net worth as unavailable rather than substituting total assets

#### Scenario: Percentage change from zero
- **WHEN** the previous period value is zero and the current period value is nonzero
- **THEN** the system SHALL display the observed amounts and an unavailable or zero-baseline state rather than inventing a percentage change

### Requirement: Reporting provenance and availability
The system SHALL show the reporting period, freshness and accessible ledger drill-down for each summary, and SHALL distinguish unknown values from zero.

#### Scenario: Mobile is offline
- **WHEN** mobile cannot refresh a server summary
- **THEN** it SHALL show the last successful refresh and stale or offline state and SHALL NOT present local notification totals as current server ledger totals

#### Scenario: Pending notifications
- **WHEN** notifications are pending or need review
- **THEN** they SHALL be identified separately from committed ledger totals

### Requirement: Preserve filtered ledger semantics
The system SHALL explain filter-sensitive gross and reimbursement treatment when a filtered ledger summary differs from an unfiltered report.

#### Scenario: Filtered report
- **WHEN** filters prevent a reimbursement offset from being applied to a ledger summary
- **THEN** the user SHALL see that the outflow is gross and the offset unavailable
