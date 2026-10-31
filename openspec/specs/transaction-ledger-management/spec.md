# transaction-ledger-management Specification

## Purpose
Provide searchable ledger records, inspection and editing, recurring origins and consolidated internal movements.

## Requirements

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

### Requirement: Transaction Detail & Inline Editing
The application SHALL allow users to click any transaction row to open a detail and editing modal supporting.

#### Scenario: Required behavior and constraints
- **WHEN** an owner opens transaction details or edits a ledger payment
- **THEN** the following required behavior and constraints hold:

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
The ledger and transaction models SHALL retain originating rule identifiers and visible origin indicators for historical recurring and payroll transactions.

#### Scenario: Viewing recurring badge on ledger transaction
- **WHEN** a user inspects a historical transaction created by a recurring rule
- **THEN** the ledger SHALL show its recurring origin without creating another transaction

### Requirement: Flexible Transaction Creation Contract
The transaction creation API SHALL support both explicit UUID references and human-readable string references.

#### Scenario: Required behavior and constraints
- **WHEN** a client submits transaction references and an optional retry identifier
- **THEN** the following required behavior and constraints hold:

  The transaction creation API SHALL support both explicit UUID references and human-readable string references:
  1. `account_id` OR `account_name` for source liquid cash or investment accounts.
  2. `target_account_id` OR `target_account_name` for the position or funding account of an investment trade. Internal movements between accounts are recorded through `POST /api/movements`.
  3. `category_id` OR `category_name` for spending/income classifications.
  4. An optional `idempotency_key` (up to 64 characters) ensuring duplicate requests return the original transaction without double-counting ledger balances.
  5. A string reference SHALL resolve only by the exact, owner-scoped rules of the idempotent-public-transactions requirement "Name-Based Entity Resolution on Transaction Creation"; an unresolved or ambiguous name SHALL be answered with HTTP 422 and candidates, never by creating an entity.

#### Scenario: Creating an expense using category name and account name
- **WHEN** client posts an expense with `account_name = "GoPay"`, `category_name = "Makanan & Minuman"`, and `amount = 25000`, and the owner has exactly one account and one expense category with those names
- **THEN** server resolves the names to corresponding IDs, writes the expense to the ledger, and returns HTTP 200 with the new transaction id

### Requirement: Consolidated Internal Movement Ledger Representation
The application SHALL persist and present each bilateral internal movement as one logical operation backed by two durably linked ledger records.

#### Scenario: Required behavior and constraints
- **WHEN** an internal movement is persisted, displayed, edited or deleted
- **THEN** the following required behavior and constraints hold:

  The application SHALL persist and present each bilateral internal movement as one logical operation backed by two durably linked ledger records:
  1. Both records SHALL share an immutable movement identifier assigned by the server.
  2. In the default "Semua Rekening" view, durably linked records SHALL render as one consolidated row regardless of their timestamp difference.
  3. The row SHALL display a neutral `↔️ PINDAH SALDO` badge, the source account in the `Rekening` column and the destination account in the `Target / Tagihan` column (or the format `Sumber → Tujuan`), and the amount in neutral tabular typography without misleading `+` or `-` prefixes, reflecting zero net cash flow change.
  4. In account-filtered views, the movement SHALL display direction relative to the selected account.
  5. Creating, editing, or deleting the logical movement SHALL atomically create, update, or delete both records through a server-owned movement operation.
  6. In the all-accounts ledger, two unlinked rows identified as internal movements SHALL render as one inferred row when they have opposite directions, equal positive amounts, different accounts, timestamps less than 30 seconds apart, and exactly one mutually eligible partner in the loaded ledger page. This match SHALL be display-only and SHALL NOT assign a movement identifier or enable atomic pair edit/delete.
  7. A linked movement SHALL never be matched heuristically with an unlinked row, and ambiguous or incomplete legacy rows SHALL remain separate.
  8. The user SHALL be able to select exactly two unlinked transactions, including transactions not categorized as internal movements, by holding an eligible ledger row for one second and then clicking rows. The ledger SHALL display a concise hold instruction and current selection count. The first hold SHALL enter selection mode and select that row; holding a uniquely inferred movement row SHALL select both underlying records together. Subsequent clicks SHALL toggle rows or inferred pairs, selection SHALL stop at two underlying records, and unselecting the final selection SHALL exit selection mode. A short click outside selection mode SHALL continue to open the record for editing. Selection SHALL survive ledger pagination and filters.
  9. The ledger SHALL NOT show a dedicated merge-entry button. The desktop ledger SHALL NOT show a row-level `Gabungkan` action or an `Aksi` column. Once exactly two records are selected, the UI SHALL show a viewport-fixed `Jadikan Pindah Saldo` action. Confirmed linked movements SHALL remain consolidated and visibly marked as `Tergabung`; uniquely inferred legacy pairs SHALL remain consolidated while selecting. Inferred and unlinked rows SHALL NOT display `perkiraan` or `Belum tertaut` labels.
  10. The `Jadikan Pindah Saldo` action SHALL fetch current transaction details and directly submit a pair with equal amounts, without an additional confirmation preview. If one income and one expense have different amounts, the UI SHALL NOT call the conversion endpoint and SHALL show a centered, dimmed warning modal with a single `Ok` button. For eligible equal-value rows, the server SHALL atomically verify same owner, one income and one expense, equal positive amounts, different eligible liquid accounts, no existing movement linkage, and no plan, goal, transfer-fee or other financial association that conversion would corrupt; it SHALL set the shared movement identifier, roles, and internal-movement classifications without creating or deleting transactions or changing balances. Rejected conversion SHALL leave both records unchanged.

#### Scenario: Viewing a durably linked movement
- **WHEN** the user views the all-accounts ledger after moving funds between two accounts
- **THEN** the two records sharing the movement identifier render as one `↔️ PINDAH SALDO` row with correct source and destination

#### Scenario: Atomically editing a movement
- **WHEN** the user changes the amount, date, source, or destination of a consolidated movement
- **THEN** the server updates both linked records in one transaction or updates neither

#### Scenario: Preserving unrelated equal-value transfers
- **WHEN** two movements have the same amount and occur within ten minutes but have different movement identifiers
- **THEN** the ledger renders them as two distinct movements

#### Scenario: Inferring a unique legacy pair
- **WHEN** two unlinked internal-movement rows are each other's only eligible opposite-direction match and their timestamps differ by 29 seconds
- **THEN** the all-accounts ledger shows one inferred movement row without persisting linkage or offering paired edit/delete

#### Scenario: Keeping an ambiguous or late pair separate
- **WHEN** unlinked rows are exactly 30 seconds apart or one row has multiple eligible partners
- **THEN** the ledger keeps the rows separate for explicit selection

#### Scenario: Selecting transactions by holding a ledger row
- **WHEN** the user holds an eligible row for one second
- **THEN** the ledger enters selection mode and highlights that row, subsequent row clicks add or remove selections up to two, and removing the last selection exits selection mode

#### Scenario: Editing with a short click
- **WHEN** the user clicks a row without entering selection mode
- **THEN** the existing edit flow opens for that record

#### Scenario: Keeping inferred pairs consolidated during selection
- **WHEN** the user enters selection mode while the all-accounts ledger contains a uniquely inferred legacy pair
- **THEN** the pair remains one `Pindah Saldo` row without a `perkiraan` or `Belum tertaut` label

#### Scenario: Selecting an inferred pair as a unit
- **WHEN** the user holds a uniquely inferred legacy movement row for one second
- **THEN** both underlying transaction records are selected together and the ledger continues to show one row

#### Scenario: Keeping durable movement status during selection
- **WHEN** the user enters selection mode while the ledger contains a durably linked movement
- **THEN** that movement remains one consolidated row visibly marked `Tergabung`

#### Scenario: Converting two equal-value transactions directly
- **WHEN** exactly one eligible expense and one eligible income from different accounts are selected and their current amounts match
- **THEN** the floating `Jadikan Pindah Saldo` action submits the conversion without a confirmation preview, both existing records receive one shared movement identifier and internal-movement classification atomically, and their balances and original timestamps remain unchanged

#### Scenario: Rejecting unequal transaction amounts before conversion
- **WHEN** the selected expense and income have different amounts
- **THEN** the conversion endpoint is not called and a centered dim-background warning modal displays a single `Ok` button

#### Scenario: Confirming two uncategorized transactions as a movement
- **WHEN** the user selects one eligible expense and one eligible income from different accounts and activates the floating conversion action
- **THEN** both existing records receive one shared movement identifier and internal-movement classification atomically, their balances and original timestamps remain unchanged, and subsequent ledger views show one durable movement

#### Scenario: Rejecting an unsafe manual conversion
- **WHEN** one selected row is already linked, has a debt allocation, or the amounts differ
- **THEN** the server rejects the operation and neither row changes

#### Scenario: Viewing internal movements in all-accounts ledger
- **WHEN** the user views the all-accounts ledger after a canonical internal movement
- **THEN** the linked pair renders as one neutral row with source, destination, and amount

#### Scenario: Deleting a consolidated internal movement
- **WHEN** the user confirms deletion of a linked movement
- **THEN** the server removes both roles atomically and restores both affected balances

### Requirement: Complete Canonical Transaction Capture
The application SHALL provide one canonical transaction-capture form that supports income and expense type, positive amount, active account, type-compatible category, transaction-level Kakeibo classification, timestamp, notes, mutually compatible goal or obligation context, and optional receipt attachment. Selecting a category SHALL default the transaction classification from category metadata unless the user explicitly changes it.

#### Scenario: Inheriting category classification
- **WHEN** a user selects an expense category classified as `want` and does not override it
- **THEN** the submitted transaction uses `want` rather than a hard-coded default

#### Scenario: Attaching a receipt during capture
- **WHEN** a user records a transaction with a valid receipt
- **THEN** the transaction is created and the receipt is attached through the supported receipt contract with visible success or failure feedback

### Requirement: Internal Movement Capture Mode
The canonical Quick Capture form SHALL offer `Pengeluaran`, `Pemasukan`, and `Perpindahan` modes.

#### Scenario: Required behavior and constraints
- **WHEN** an owner selects and submits an internal movement in Quick Capture
- **THEN** the following required behavior and constraints hold:

  The canonical Quick Capture form SHALL offer `Pengeluaran`, `Pemasukan`, and `Perpindahan` modes. Perpindahan SHALL collect a positive amount, distinct eligible liquid source and destination accounts, timestamp with seconds, and optional notes, and SHALL submit through the existing atomic movement creation contract rather than the ordinary transaction endpoint. It SHALL NOT submit transaction-only category, Kakeibo, goal, obligation, or receipt fields. Movement creation SHALL preserve the existing bilateral account-balance and consolidated-ledger behavior.

  The Accounts page SHALL NOT expose dedicated transfer controls in account menus, pocket actions, or page-level desktop/mobile actions. Home movement shortcuts SHALL open Quick Capture in Perpindahan mode. Movement editing and deletion from ledger detail SHALL continue to use the atomic movement edit/delete contract.

#### Scenario: Creating an internal movement from Quick Capture
- **WHEN** a user selects `Perpindahan`, chooses two different eligible liquid accounts, enters a positive amount, and saves
- **THEN** the client calls the existing atomic movement creation contract with source, destination, amount, date, and optional notes, and creates no ordinary transaction request

#### Scenario: Rejecting an invalid movement selection
- **WHEN** a user selects the same account as source and destination, or does not provide two eligible accounts or a positive amount
- **THEN** the form identifies the invalid field, focuses the first invalid control, and sends no movement request

#### Scenario: Opening movement capture from Home
- **WHEN** the user activates a Home movement shortcut
- **THEN** Quick Capture opens with `Perpindahan` selected and exposes the canonical movement fields

#### Scenario: Editing an existing ledger movement
- **WHEN** a user opens a consolidated movement row and edits it
- **THEN** the existing atomic movement edit flow updates both linked records while the ledger continues to show one consolidated row

### Requirement: Complete Ledger Editing
The transaction detail form SHALL expose every safely editable transaction property, including type where compatible, account, category, classification, date, notes, goal or obligation association, and receipt. Movement rows SHALL delegate source, target, amount, notes, and date changes to the canonical atomic movement operation.

#### Scenario: Required behavior and constraints
- **WHEN** an owner inspects or edits a ledger transaction
- **THEN** the following required behavior and constraints hold:

  The transaction detail form SHALL expose every safely editable transaction property, including type where compatible, account, category, classification, date, notes, goal or obligation association, and receipt. Movement rows SHALL delegate source, target, amount, notes, and date changes to the canonical atomic movement operation.

  Transaction capture, movement create/edit, and ordinary ledger edit SHALL show and allow editing seconds in their date-time control. Opening an existing record SHALL retain its stored second value, and submitting an unchanged control SHALL NOT silently round the timestamp to the minute. Compact ledger date/time display SHALL remain unchanged.

#### Scenario: Editing transaction classification
- **WHEN** a user changes a normal expense from `need` to `want`
- **THEN** the updated classification is persisted and affected analytics refresh

#### Scenario: Editing a timestamp with seconds
- **WHEN** a user opens an existing transaction or movement recorded at 12:34:27
- **THEN** the form shows 12:34:27 and submitting preserves that second rather than changing it to 12:34:00

### Requirement: Consistent Post-Mutation Refresh
After a successful financial form mutation, every active view whose displayed data changed SHALL refresh using the canonical cache identity for that resource.

#### Scenario: Updating an investment trade from its modal
- **WHEN** a trade succeeds
- **THEN** accounts, ledger, dashboard, and net-worth views invalidate their active canonical queries
