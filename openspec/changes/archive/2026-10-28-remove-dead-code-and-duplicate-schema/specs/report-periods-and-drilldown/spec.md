## MODIFIED Requirements

### Requirement: Calendar Month Window
Dashboard overview and analytics SHALL accept `timeframe=month`, meaning the calendar month of today shifted by `cycle_offset` months, labelled in Indonesian (for example "Oktober 2026"), compared with the previous calendar month.

#### Scenario: Month boundaries
- **WHEN** the month window is computed for 10 February 2026, for 31 January 2026 with offset −1, and for 29 February 2024
- **THEN** it SHALL be 1–28 February 2026 "Februari 2026", 1–31 December 2025 "Desember 2025" and 1–29 February 2024.

#### Scenario: One window for report and ledger
- **GIVEN** expenses of Rp 45.000 inside last month and Rp 99.000 and Rp 11.000 one minute outside it
- **WHEN** analytics are requested with `timeframe=month&cycle_offset=-1` and the ledger with `period=month&period_offset=-1` for that category
- **THEN** both SHALL show only the Rp 45.000 row, with the same label.

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

## ADDED Requirements

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
Insights SHALL offer a payday-cycle / calendar-month switch and the ledger a period selector (all, payday cycle, calendar month) with a previous/next stepper.

#### Scenario: Switching insights to calendar months
- **WHEN** the owner switches insights from payday cycle to calendar month
- **THEN** the figures SHALL be requested with `timeframe=month` and labelled with the month name.

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
