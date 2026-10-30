## MODIFIED Requirements

### Requirement: Recurring Rule Origin Tracking in Ledger
The ledger and transaction models SHALL retain originating rule identifiers and visible origin indicators for historical recurring and payroll transactions.

#### Scenario: Viewing recurring badge on ledger transaction
- **WHEN** a user inspects a historical transaction created by a recurring rule
- **THEN** the ledger SHALL show its recurring origin without creating another transaction

### Requirement: Searchable & Filterable Ledger View
The application SHALL provide a dedicated `/ledger` screen displaying all recorded cash movements in a responsive tabular view with real-time filtering.

#### Scenario: Required behavior and constraints
- **WHEN** an owner opens or filters the ledger
- **THEN** the following required behavior and constraints hold:

  The application SHALL provide a dedicated `/ledger` screen displaying all recorded cash movements in a responsive tabular view with real-time filtering:
  1. Full-text search by notes, merchant, or category name.
  2. Filter chips by transaction type (`All`, `Expense`, `Income`, `Transfer`).
  3. Account filter dropdown and category filter dropdown.
  4. Cash flow summary display providing both running salary-cycle totals (inbound, outbound, net) from the latest confirmed salary receipt, or the labelled calendar month when no receipt is confirmed, and cumulative overall totals (inbound, outbound, net) matching active filters, independent of pagination boundaries.

#### Scenario: Searching transactions by note
- **WHEN** the user types "coffee" in the ledger search input
- **THEN** the table immediately filters to show only transactions containing "coffee" in notes or category name

#### Scenario: Filtering transactions by account
- **WHEN** the user selects "Main Bank" from the account dropdown
- **THEN** only transactions originating from or transferring into "Main Bank" are displayed

#### Scenario: Displaying running cycle and cumulative cash flow
- **WHEN** the user views the ledger on any pagination page
- **THEN** the summary cards display the running salary-cycle (or labelled calendar-month) inbound, outbound, and net cash flow alongside the cumulative overall totals, rather than merely summing the current page's rows

#### Scenario: Filtering ledger updates both cycle and cumulative summaries
- **WHEN** the user applies an account, category, or search filter
- **THEN** the ledger summary displays the cycle totals and cumulative totals matching the filtered criteria across all matching transactions
