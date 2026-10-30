# report-periods-and-drilldown Specification

## Purpose
Make report figures trustworthy and traceable: payday-cycle and calendar-month windows, category
breakdowns that add up to total outflow, and links from every figure to the ledger rows behind it.

## Requirements

### Requirement: Calendar Month Window
Dashboard overview and analytics SHALL accept `timeframe=month`, meaning the calendar month of today shifted by `cycle_offset` months, labelled in Indonesian (for example "Oktober 2026"), compared with the previous calendar month.

#### Scenario: Month boundaries
- **WHEN** the month window is computed for 10 February 2026, for 31 January 2026 with offset −1, and for 29 February 2024
- **THEN** it SHALL be 1–28 February 2026 "Februari 2026", 1–31 December 2025 "Desember 2025" and 1–29 February 2024.

#### Scenario: One window for report and ledger
- **GIVEN** expenses of Rp 45.000 inside last month and Rp 99.000 and Rp 11.000 one minute outside it
- **WHEN** analytics are requested with `timeframe=month&cycle_offset=-1` and the ledger with `period=month&period_offset=-1` for that category
- **THEN** both SHALL show only the Rp 45.000 row, with the same label.

### Requirement: Complete Category Breakdown
Dashboard overview `categories` and analytics `category_variance` SHALL list active expense categories, archived categories with spending in the window (`is_archived: true`) and, when uncategorised spending exists, a "Tanpa kategori" bucket (`id: null`, `is_uncategorized: true`). Spending in categories that reports exclude ("Internal Movement", "Investasi", excluded from budget) SHALL count neither in the breakdown nor in total outflow, so that the breakdown sums exactly to `total_outflow`.

#### Scenario: Breakdown adds up
- **GIVEN** a cycle with the shared-dinner figures, Rp 70.000 uncategorised, Rp 30.000 in a category archived afterwards, and spending in "Investasi" and "Internal Movement"
- **WHEN** analytics and overview are requested
- **THEN** the sum of category `spent` SHALL equal `total_outflow` Rp 400.000
- **AND** "Tanpa kategori" SHALL show Rp 70.000 and the archived category Rp 30.000
- **AND** neither "Investasi" nor "Internal Movement" SHALL be listed.

### Requirement: Drill-Down to the Ledger
On the insights page every category figure and every Kakeibo pillar figure SHALL link to the ledger filtered to the same period (`period`, `offset`), `type=expense` and the category (`category=<id>`, or `category=uncategorized` for "Tanpa kategori") or pillar (`kakeibo=<pillar>`).

#### Scenario: Category link
- **GIVEN** insights for last month showing Rp 100.000 for "Hobi"
- **THEN** the figure SHALL link to `/ledger?period=month&offset=-1&category=<Hobi id>&type=expense`.

#### Scenario: Ledger opened from a link
- **WHEN** the ledger opens with `period=month&offset=-1&category=<id>&kakeibo=want&type=expense&review=needs_review`
- **THEN** it SHALL request the list with those filters and without `logical_movements`
- **AND** show the active filters and the period label, and stepping back SHALL set `offset=-2`.

#### Scenario: Invalid link values
- **WHEN** the URL carries an unknown period, pillar, type, review status or a malformed id
- **THEN** that filter SHALL fall back to "all".

### Requirement: Review Status and Fee Links in the Ledger
Ledger rows SHALL show "Perlu ditinjau" for `needs_review` and "Otomatis" for `auto` rows in text. List items SHALL include `movement_fee` on movement legs with a fee and `fee_movement` (movement id, source and target account names, movement amount) on fee rows, and the ledger SHALL show the link in both directions. `/ledger?edit=<id>` SHALL open the edit dialog of that row and, with `return=review`, return to `/review` when the dialog closes.

#### Scenario: Fee row and movement
- **GIVEN** a Rp 102.500 send linked with a Rp 100.000 credit and a Rp 2.500 fee row
- **WHEN** the ledger is listed
- **THEN** the fee row SHALL carry `fee_movement` with both account names and Rp 100.000
- **AND** both legs SHALL carry `movement_fee` with the fee row id and Rp 2.500.

#### Scenario: Edit link from the review page
- **WHEN** `/ledger?edit=<id>&return=review` is opened and the dialog is closed
- **THEN** the dialog SHALL have shown that row and the browser SHALL return to `/review`.

### Requirement: Ledger Period Filter
The ledger list SHALL accept `period=cycle|month` with `period_offset` (−36 to 12) resolved by the same window rule as the reports, and SHALL reject a period combined with `from_date` or `to_date` with 422 `period_with_dates`.

#### Scenario: Period with explicit dates
- **WHEN** the ledger is requested with `period=month&from_date=2026-01-01`
- **THEN** the response SHALL be 422 `period_with_dates`.

#### Scenario: Offset out of range
- **WHEN** the ledger is requested with `period=cycle&period_offset=13`
- **THEN** the response SHALL be 422.

### Requirement: Ledger Period Summary
The ledger list SHALL return a summary `period` block with the window and its figures whenever a period or date range filters the list.

#### Scenario: Date range summary
- **WHEN** the ledger is requested with `from_date` and `to_date`
- **THEN** the summary SHALL contain a `period` block with that window and its figures.

### Requirement: Period Controls
Insights SHALL offer a salary-cycle / calendar-month switch and the ledger a period selector (all, salary cycle, calendar month) with a previous/next stepper. Salary cycles are bounded by confirmed salary receipts; the current cycle has no known end, and a cycle without a confirmed receipt SHALL fall back to the labelled calendar month instead of a fixed payday.

#### Scenario: Switching insights to calendar months
- **WHEN** the owner switches insights from salary cycle to calendar month
- **THEN** the figures SHALL be requested with `timeframe=month` and labelled with the month name.

#### Scenario: Stepping to a cycle that is not available
- **WHEN** the owner steps to a cycle before the first confirmed salary receipt
- **THEN** the figures SHALL use the calendar month for that step and the label SHALL say the salary cycle is unavailable

### Requirement: Ledger Filters in the URL
The ledger SHALL read its period, category, pillar, type, account and review-status filters from the URL, SHALL write changes back to the URL, and SHALL show active filters as visible chips with a way to clear them.

#### Scenario: Clearing a filter chip
- **GIVEN** the ledger opened with `category=<id>&type=expense`
- **WHEN** the owner clears the category chip
- **THEN** the URL SHALL drop `category` and keep `type=expense`.

### Requirement: Uncategorised Ledger Filter
The ledger list API SHALL accept `uncategorized=true` and SHALL reject it together with `category_id` with 422 `uncategorized_with_category`.

#### Scenario: Conflicting category filters
- **WHEN** the list is requested with `uncategorized=true&category_id=<id>`
- **THEN** the response SHALL be 422 `uncategorized_with_category`.

### Requirement: Non-Editable Insight Rows
Archived and "Tanpa kategori" rows on the insights page SHALL not offer budget, edit or archive actions.

#### Scenario: Uncategorised row
- **WHEN** insights list "Tanpa kategori"
- **THEN** that row SHALL link to the ledger but offer no budget, edit or archive action.
