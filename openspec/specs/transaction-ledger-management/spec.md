# transaction-ledger-management Specification

## Purpose
TBD - created by archiving change bibit-executive-sidebar-and-power-tools. Update Purpose after archive.
## Requirements
### Requirement: Searchable & Filterable Ledger View
The application SHALL provide a dedicated `/ledger` screen displaying all recorded cash movements in a responsive tabular view with real-time filtering:
1. Full-text search by notes, merchant, or category name.
2. Filter chips by transaction type (`All`, `Expense`, `Income`, `Transfer`).
3. Account filter dropdown and category filter dropdown.
4. Summary status chips indicating the total transaction count and net volume matching active filters.

#### Scenario: Searching transactions by note
- **WHEN** the user types "coffee" in the ledger search input
- **THEN** the table immediately filters to show only transactions containing "coffee" in notes or category name

#### Scenario: Filtering transactions by account
- **WHEN** the user selects "Main Bank" from the account dropdown
- **THEN** only transactions originating from or transferring into "Main Bank" are displayed

### Requirement: Transaction Detail & Inline Editing
The application SHALL allow users to click any transaction row to open a detail and editing modal supporting:
1. Editing the transaction amount with automatic balance recalculation on affected accounts.
2. Changing the associated account or category.
3. Updating notes and transaction timestamp.
4. Linking or unlinking savings goals (`goal_id`) and debt obligations (`obligation_id`).
5. Deleting the transaction with confirmation, cleanly reversing its balance impact on accounts, goals, or obligations.

#### Scenario: Modifying transaction amount
- **WHEN** the user changes a transaction amount from 50,000 to 75,000 IDR and clicks Save
- **THEN** the backend updates the transaction record, reconciles the corresponding account balance by -25,000 IDR, and reflects the updated numbers across the UI

#### Scenario: Deleting a linked goal transaction
- **WHEN** the user deletes an expense linked to a savings goal
- **THEN** the transaction is removed and the goal's `current_amount` decreases by the transaction amount

### Requirement: Recurring Rule Origin Tracking in Ledger
The ledger and transaction models SHALL support tracking the originating recurring rule ID (`recurring_rule_id`):
1. Transactions created via automated execution or 1-tap confirmation SHALL store `recurring_rule_id`.
2. The ledger UI SHALL render a subtle recurring badge indicator for transactions originating from recurring rules or payroll allocations.

#### Scenario: Viewing recurring badge on ledger transaction
- **WHEN** user inspects a transaction created by the automated recurring engine
- **THEN** ledger displays a recurring icon badge indicating it was generated from a scheduled rule

