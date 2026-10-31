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

### Requirement: Top spending categories in the cash-flow summary
`GET /api/dashboard/cash-flow-summary`, for `period` `today` or `month` and with session or Bearer authentication, SHALL return `top_categories`: at most 3 entries with the most net spending, each an owner category or uncategorised spending, as `category_id`, `name`, `kakeibo_type` and integer rupiah `outflow`, sorted by outflow descending, then name. It SHALL use the window and spending rules of the response's `outflow`, omit an entry at 0 or less, and be `[]`, never `null`, without spending.

#### Scenario: Reimbursement netted per category
- **GIVEN** today's expenses of Rp 300.000 in "Makan Bareng" and Rp 200.000 in "Belanja Dapur", and a reimbursement of Rp 200.000 that offsets "Makan Bareng"
- **WHEN** the phone requests the summary for `today`
- **THEN** `outflow` SHALL be Rp 300.000 and `top_categories` SHALL list "Belanja Dapur" Rp 200.000, then "Makan Bareng" Rp 100.000

#### Scenario: Fully reimbursed category
- **WHEN** a reimbursement covers all of a category's spending in the window
- **THEN** that category SHALL NOT be listed

#### Scenario: Movements, investments and fees
- **WHEN** the window holds internal movements, investment trades, expenses in "Investasi" or "Internal Movement", a grocery expense and a transfer fee
- **THEN** only the grocery category and the fee category SHALL be listed, and their amounts SHALL add up to `outflow`

#### Scenario: Listed amounts never exceed the outflow
- **WHEN** any summary is returned
- **THEN** the sum of the listed `outflow` amounts SHALL be at most the response's `outflow`, and SHALL fall short of it only by the entries cut by the limit of 3

#### Scenario: Uncategorised spending ranks with the categories
- **GIVEN** today's expenses of Rp 90.000 in "Belanja Dapur", Rp 75.000 without a category, Rp 40.000 in "Makan Bareng" and Rp 10.000 in "Lain-lain"
- **WHEN** the phone requests the summary for `today`
- **THEN** `top_categories` SHALL list "Belanja Dapur" Rp 90.000, then `{"category_id": null, "name": "Tanpa kategori", "kakeibo_type": null, "outflow": 75000}`, then "Makan Bareng" Rp 40.000
- **AND** `outflow` SHALL be Rp 215.000, Rp 10.000 more than the listed sum

#### Scenario: Listed amounts equal the outflow when none is cut
- **WHEN** the window holds three or fewer entries with spending, uncategorised spending included
- **THEN** every entry SHALL be listed and the listed amounts SHALL add up exactly to `outflow`

#### Scenario: Uncategorised spending is not reimbursed
- **WHEN** a reimbursement offsets a category, or names no category to offset
- **THEN** the "Tanpa kategori" entry SHALL keep its full amount, and the offset category SHALL be listed net of the reimbursement

#### Scenario: Today and month
- **WHEN** the owner spent today in one category and earlier this month in another
- **THEN** `today` SHALL list only today's category and `month` SHALL list both, each from the month's first local day to now

#### Scenario: No spending
- **WHEN** the window holds no reportable spending
- **THEN** `top_categories` SHALL be an empty list

#### Scenario: Limit and ties
- **WHEN** four categories have the same outflow
- **THEN** the first three by name SHALL be listed

#### Scenario: Owner scope and authentication
- **WHEN** another owner spends in a category of the same name or without a category, or the phone calls with its Bearer key
- **THEN** the other owner's spending SHALL NOT be listed, and the Bearer response SHALL equal the session response; an unknown key SHALL get HTTP 401

#### Scenario: Older clients
- **WHEN** a client that does not know `top_categories` reads the summary
- **THEN** every other field SHALL keep its name, type and value, and each `top_categories` entry SHALL keep its four fields, of which only `category_id` and `kakeibo_type` can be `null`
