# Spec Delta

## MODIFIED Requirements

### Requirement: Ledger Filters in the URL
The ledger SHALL read its period, date range (`from`, `to` as ISO instants), search text (`q`), category, pillar, type, account, review-status, source and waiting filters from the URL, SHALL write every change back to the URL without a server round trip, and SHALL show active filters as visible chips with a way to clear them. A date range SHALL be ignored when a period is also given. A URL without `period` SHALL mean all time.

#### Scenario: Clearing a filter chip
- **GIVEN** the ledger opened with `category=<id>&type=expense`
- **WHEN** the owner clears the category chip
- **THEN** the URL SHALL drop `category` and keep `type=expense`.

#### Scenario: Date-range drill-down
- **WHEN** the ledger opens with `from=<start>&to=<end>&kakeibo=need&type=expense`
- **THEN** it SHALL request the list with `from_date`, `to_date`, `kakeibo_type=need` and `type=expense`, show the range as an active filter, and clearing filters SHALL drop the range

#### Scenario: Search from a link
- **WHEN** the ledger opens with `q=<merchant>`
- **THEN** its search field SHALL contain that text and the list SHALL be requested with it

#### Scenario: Clearing back to the plain ledger
- **GIVEN** the ledger was opened directly at `/ledger?type=income`
- **WHEN** the owner selects the "Semua" tab
- **THEN** the URL SHALL become `/ledger` and the list SHALL be requested without a type filter

#### Scenario: Back and reload keep the filters
- **WHEN** the owner selects the "Keluar" tab and the "Belum dicek" chip, reloads, and then opens a row and goes back
- **THEN** the URL SHALL hold `type=expense&review=auto` after the reload, and back SHALL return to the list with the same filters

#### Scenario: Navigation opens this cycle
- **WHEN** the owner opens Transaksi from the sidebar or the phone tab bar
- **THEN** the URL SHALL be `/ledger?period=cycle` and the list SHALL show the current payday cycle, while `/ledger` and existing links without `period` SHALL keep showing all time

#### Scenario: Unknown filter values
- **WHEN** the ledger opens with `type=foo&source=bar&waiting=2`
- **THEN** each unknown value SHALL fall back to its default and the page SHALL open without an error
