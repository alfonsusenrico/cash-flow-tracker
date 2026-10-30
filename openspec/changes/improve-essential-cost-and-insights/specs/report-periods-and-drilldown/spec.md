## MODIFIED Requirements

### Requirement: Ledger Filters in the URL
The ledger SHALL read its period, date range (`from`, `to` as ISO instants), search text (`q`), category, pillar, type, account and review-status filters from the URL, SHALL write changes back to the URL, and SHALL show active filters as visible chips with a way to clear them. A date range SHALL be ignored when a period is also given.

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
