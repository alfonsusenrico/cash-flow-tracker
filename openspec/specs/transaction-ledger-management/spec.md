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
4. Linking or unlinking savings goals (`goal_id`) and debt obligations (`obligation_id`), or assigning a debt payment among multiple obligations with explicit amounts (`obligation_allocations`).
5. Showing each debt name and assigned amount for an allocated payment in ledger detail, including debts that were archived by that payment; the transaction SHALL remain one ledger row.
6. Deleting the transaction with confirmation, cleanly reversing its balance impact on accounts, goals, or obligations.

The Quick Capture and ledger edit forms SHALL let a user select multiple active debts for an expense. Selecting a new debt SHALL default its payment amount to that debt's outstanding balance and SHALL update the transaction total to the sum of selected payments. The default view SHALL show each amount without requiring a separate input. An explicit amount-edit action SHALL allow partial payments per debt and SHALL update the total as amounts change. The form SHALL prevent submission when a selected amount is not positive, exceeds the available debt balance, or does not match the transaction total. Reopening a saved payment SHALL show its recorded amounts rather than replacing them with current debt balances. The same behavior and feedback SHALL be available in desktop dialogs and mobile sheets. A single-debt selection MAY keep the existing API field for compatibility.

#### Scenario: Modifying transaction amount
- **WHEN** the user changes a transaction amount from 50,000 to 75,000 IDR and clicks Save
- **THEN** the backend updates the transaction record, reconciles the corresponding account balance by -25,000 IDR, and reflects the updated numbers across the UI

#### Scenario: Deleting a linked goal transaction
- **WHEN** the user deletes an expense linked to a savings goal
- **THEN** the transaction is removed and the goal's `current_amount` decreases by the transaction amount

#### Scenario: Recording one payment for multiple debts
- **WHEN** a user selects two debts for a single expense and allocates the full expense amount between them
- **THEN** Quick Capture shows each assigned amount before submission and the ledger subsequently shows one transaction with the two-debt breakdown

#### Scenario: Selecting debts defaults to full payoff
- **WHEN** a user selects debts with 80,000 IDR and 50,000 IDR outstanding
- **THEN** the form shows those payment amounts without extra amount inputs and sets the transaction total to 130,000 IDR

#### Scenario: Changing to a partial payment
- **WHEN** the user activates the amount-edit action and reduces the first selected debt payment from 80,000 IDR to 30,000 IDR
- **THEN** the transaction total updates to 80,000 IDR while the second selected debt remains at 50,000 IDR

#### Scenario: Correcting an incomplete allocation
- **WHEN** selected debt amounts do not add up to the expense amount
- **THEN** the form shows the remaining or excess amount, focuses an actionable field or summary, and sends no request

#### Scenario: Editing an allocated payment
- **WHEN** a user opens an existing allocated expense in the ledger and changes the debt amounts
- **THEN** the saved breakdown appears unchanged until the user edits it, the transaction total follows the edited amounts, and the updated transaction still appears as one ledger row with the new breakdown

### Requirement: Recurring Rule Origin Tracking in Ledger
The ledger and transaction models SHALL support tracking the originating recurring rule ID (`recurring_rule_id`):
1. Transactions created via automated execution or 1-tap confirmation SHALL store `recurring_rule_id`.
2. The ledger UI SHALL render a subtle recurring badge indicator for transactions originating from recurring rules or payroll allocations.

#### Scenario: Viewing recurring badge on ledger transaction
- **WHEN** user inspects a transaction created by the automated recurring engine
- **THEN** ledger displays a recurring icon badge indicating it was generated from a scheduled rule

### Requirement: Flexible Transaction Creation Contract
The transaction creation API SHALL support both explicit UUID references and human-readable string references:
1. `account_id` OR `account_name` for source liquid cash or investment accounts.
2. `transfer_target_account_id` OR `target_account_name` for transfer destinations.
3. `category_id` OR `category_name` for spending/income classifications.
4. An optional `idempotency_key` (up to 64 characters) ensuring duplicate requests return the original transaction without double-counting ledger balances.

#### Scenario: Creating an expense using category name and account name
- **WHEN** client posts an expense with `account_name = "GoPay"`, `category_name = "Makanan & Minuman"`, and `amount = 25000`
- **THEN** server resolves the names to corresponding IDs, writes the expense to the ledger, and returns HTTP 201

### Requirement: Consolidated Internal Movement Ledger Representation
The application SHALL consolidate paired internal movement transactions into a single bilateral row in the ledger table:
1. In the default "Semua Rekening" ledger view, paired inbound and outbound records representing the same internal transfer SHALL be rendered as a single consolidated row.
2. The consolidated row SHALL display a neutral `↔️ PINDAH SALDO` status badge.
3. The row SHALL display the origin account in the `Rekening` column and destination account in the `Target / Tagihan` column (or format `Sumber → Tujuan`).
4. The transaction amount SHALL be rendered with neutral tabular typography without misleading `+` or `-` prefixes, accurately reflecting zero net cash flow change.
5. In account-filtered views, the movement SHALL display directional context relative to the selected account.
6. Editing or deleting a consolidated movement row SHALL atomically update or delete both underlying paired records to safeguard double-entry balance integrity.

#### Scenario: Viewing internal movements in all-accounts ledger
- **WHEN** the user views the `/ledger` screen with "Semua Rekening" selected
- **THEN** paired transfer records are merged into a single row displaying `↔️ PINDAH SALDO`, the source account, the destination account, and neutral amount formatting

#### Scenario: Deleting a consolidated internal movement
- **WHEN** the user opens the detail sheet for a consolidated transfer and confirms deletion
- **THEN** both the outbound and inbound paired records are removed and balances on both affected accounts are cleanly restored
