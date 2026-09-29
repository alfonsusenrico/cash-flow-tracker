# Spec Delta

## MODIFIED Requirements

### Requirement: Searchable & Filterable Ledger View
The application SHALL provide a dedicated `/ledger` screen displaying all recorded cash movements in a responsive tabular view with real-time filtering:
1. Full-text search by notes, merchant, or category name.
2. Filter chips by transaction type (`All`, `Expense`, `Income`, `Transfer`).
3. Account filter dropdown and category filter dropdown.
4. Cash flow summary display providing both running payday cycle totals (inbound, outbound, net) and cumulative overall totals (inbound, outbound, net) matching active filters, independent of pagination boundaries.

#### Scenario: Searching transactions by note
- **WHEN** the user types "coffee" in the ledger search input
- **THEN** the table immediately filters to show only transactions containing "coffee" in notes or category name

#### Scenario: Filtering transactions by account
- **WHEN** the user selects "Main Bank" from the account dropdown
- **THEN** only transactions originating from or transferring into "Main Bank" are displayed

#### Scenario: Displaying running cycle and cumulative cash flow
- **WHEN** the user views the ledger on any pagination page
- **THEN** the summary cards display the running payday cycle inbound, outbound, and net cash flow alongside the cumulative overall totals, rather than merely summing the current page's rows

#### Scenario: Filtering ledger updates both cycle and cumulative summaries
- **WHEN** the user applies an account, category, or search filter
- **THEN** the ledger summary displays the cycle totals and cumulative totals matching the filtered criteria across all matching transactions
