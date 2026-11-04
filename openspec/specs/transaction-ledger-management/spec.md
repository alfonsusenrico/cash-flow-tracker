# transaction-ledger-management Specification

## Purpose
Provide searchable ledger records, inspection and editing, recurring origins and consolidated internal movements.

## Requirements

### Requirement: Searchable & Filterable Ledger View
The application SHALL provide a dedicated `/ledger` screen showing recorded cash movements grouped by local day, newest first, with one toolbar row of type tabs, search, "Filter" and "⋯", and period figures that match the active filters across all matching rows.

#### Scenario: Required behavior and constraints
- **WHEN** an owner opens or filters the ledger
- **THEN** the following required behavior and constraints hold:

  The application SHALL provide a dedicated `/ledger` screen with real-time filtering:
  1. Full-text search by notes, merchant, or category name.
  2. Type tabs "Semua", "Pengeluaran", "Masuk", "Pindah saldo" and "Investasi", each with the count of rows matching the other filters, in the same row as search, "Filter" and "⋯".
  3. "Filter" opens one popover with the period mode (Siklus gaji, Bulan kalender, Semua waktu), the single-select chips "Menunggu jawaban", "Belum dicek", "Dari HP" and "Lewat Catat", and the account, category and pillar filters; its trigger shows the number of active filters, and every active filter is shown as a removable chip below the row only while set. "⋯" holds "Gabungkan dua transaksi" and "Rutin".
  4. Cash flow figures for the filtered rows: the payday cycle (or labelled calendar month) and cumulative totals, or the selected period's figures, labelled Masuk, Pengeluaran (the summary's `spending`) and Bersih, matching active filters and independent of how many rows are loaded.
  5. Rows grouped by local day with one heading per day, holding that day's Pengeluaran (`spending`), Masuk and the number of rows that are "menunggu jawaban".
  6. On a phone, a purpose-built feed with sticky day headings instead of the table.

#### Scenario: Searching transactions by note
- **WHEN** the user types "coffee" in the ledger search input
- **THEN** the list immediately filters to show only transactions containing "coffee" in notes or category name

#### Scenario: Filtering transactions by account
- **WHEN** the user selects "Main Bank" from the account dropdown
- **THEN** only transactions originating from or transferring into "Main Bank" are displayed

#### Scenario: Displaying running cycle and cumulative cash flow
- **WHEN** the user views the ledger with any number of rows loaded
- **THEN** the figures display the running salary-cycle (or labelled calendar-month) inbound, outbound, and net cash flow alongside the cumulative overall totals, rather than merely summing the loaded rows

#### Scenario: Filtering ledger updates both cycle and cumulative summaries
- **WHEN** the user applies an account, category, or search filter
- **THEN** the ledger figures display the cycle totals and cumulative totals matching the filtered criteria across all matching transactions

#### Scenario: Day heading covers the whole day
- **GIVEN** 60 matching rows, of which 2 November holds rows 45 to 52
- **WHEN** the first 50 rows are loaded
- **THEN** the 2 November heading SHALL show the totals of all 8 rows of that day and SHALL say that 2 more rows of the day are not loaded yet

#### Scenario: Waiting row is counted in its day
- **GIVEN** an expense of Rp 45.000 with an open "Apa ini?" question on 2 November
- **WHEN** the ledger shows 2 November
- **THEN** the day's Pengeluaran SHALL include Rp 45.000 and the heading SHALL say "1 menunggu jawaban", never that it is not counted

#### Scenario: Empty result
- **WHEN** no row matches the filters
- **THEN** the ledger SHALL say "Tidak ditemukan", name the search text if any, and offer "Hapus filter"

### Requirement: Transaction Detail & Inline Editing
The application SHALL let users open any transaction row in a detail drawer (a centered modal dialog above 760 px, a bottom sheet at 760 px and below) that shows the row's provenance and supports editing, a type switch, confirming and deleting. The drawer SHALL NOT open as a side column or a side overlay, and SHALL NOT narrow the page behind it.

#### Scenario: Required behavior and constraints
- **WHEN** an owner opens transaction details or edits a ledger payment
- **THEN** the following required behavior and constraints hold:

  The application SHALL allow users to select any transaction row to open a detail drawer supporting:
  1. Editing the transaction amount with automatic balance recalculation on affected accounts.
  2. Changing the associated account or category.
  3. Updating notes and transaction timestamp.
  4. Linking or unlinking savings goals (`goal_id`) and debt obligations (`obligation_id`), or assigning a debt payment among multiple obligations with explicit amounts (`obligation_allocations`).
  5. Showing each debt name and assigned amount for an allocated payment in ledger detail, including debts that were archived by that payment; the transaction SHALL remain one ledger row.
  6. Deleting the transaction with confirmation, cleanly reversing its balance impact on accounts, goals, or obligations.
  7. Showing where the row came from ("Dari HP", "Lewat Catat", "Dicatat manual" and the other sources), who classified it ("AI", "Kebiasaan", "Pengguna", "Aturan"), and the original notification or typed text when the row has one.
  8. Opening the existing movement or top-up edit dialog for a movement or top-up row.
  9. Titling a movement row with its route in the direction the money moved, with the same "Dari" and "Ke" accounts as the route; an unlinked leg's side is taken from its type and its missing side is shown as "?".

  The Quick Capture and ledger edit forms SHALL let a user select multiple active debts for an expense. Selecting a new debt SHALL default its payment amount to that debt's outstanding balance and SHALL update the transaction total to the sum of selected payments. The default view SHALL show each amount without requiring a separate input. An explicit amount-edit action SHALL allow partial payments per debt and SHALL update the total as amounts change. The form SHALL prevent submission when a selected amount is not positive, exceeds the available debt balance, or does not match the transaction total. Reopening a saved payment SHALL show its recorded amounts rather than replacing them with current debt balances. The same behavior and feedback SHALL be available in desktop drawers and mobile sheets. A single-debt selection MAY keep the existing API field for compatibility.

#### Scenario: Modifying transaction amount
- **WHEN** the user changes a transaction amount from 50,000 to 75,000 IDR and clicks Save
- **THEN** the backend updates the transaction record, reconciles the corresponding account balance by -25,000 IDR, and reflects the updated numbers across the UI

#### Scenario: Deleting a linked goal transaction
- **WHEN** the user deletes an expense linked to a savings goal and confirms
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

#### Scenario: Delete asks first
- **WHEN** the owner presses "Hapus" in the drawer
- **THEN** nothing SHALL be deleted until the owner confirms in a message that names the amount and the balance, goal or debt it changes; "Batal" SHALL keep the row

#### Scenario: Deleting a top-up row
- **WHEN** the owner deletes a top-up converted from a debit and confirms
- **THEN** the drawer SHALL use the top-up delete path, the inbound leg SHALL be gone and the original expense SHALL be restored

#### Scenario: Waiting row points to Tinjau
- **WHEN** the owner opens a row with an open question or an unpaired movement leg
- **THEN** the drawer SHALL show "Jawab di Tinjau" linking to that item and SHALL NOT offer "Tandai sudah dicek"

#### Scenario: Drawer on a phone
- **WHEN** the owner opens a row at 390 px width
- **THEN** the drawer SHALL be a bottom sheet with focus inside it, and the browser's back SHALL close it and return focus to the row

#### Scenario: Detail opens centered on a wide window
- **WHEN** the owner opens a row at 1440 px or 1024 px width
- **THEN** the detail SHALL open as a centered dialog over a dimmed page, with focus inside it and the edit form at its full width, and no side column SHALL appear
- **AND** Esc, the close button or a click on the dimmed page SHALL close it and return focus to the row

#### Scenario: Inbound leg titled in its direction
- **GIVEN** an inbound leg on Jago · Main Pocket linked to an outbound leg on Jago · Dana Darurat
- **WHEN** the owner opens the inbound leg from Rekening detail of Main Pocket
- **THEN** the title SHALL read "Jago · Dana Darurat → Jago · Main Pocket", "Dari" SHALL be Jago · Dana Darurat and "Ke" Jago · Main Pocket

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
  3. The row SHALL display a neutral `↔️ PINDAH SALDO` badge, its route as `Sumber → Tujuan` in the direction the money moved, and the amount in neutral tabular typography without misleading `+` or `-` prefixes, reflecting zero net cash flow change. A pocket SHALL be named with its parent account, as `Jago · Main Pocket`; an account without a parent SHALL be named alone.
  4. In account-filtered views, the movement SHALL display its full route and its direction relative to the selected account, as "Masuk dari" or "Keluar ke" the other account. A leg shown without its partner SHALL take its route from its own movement role and the partner's account, never reversed.
  4a. A movement leg without a partner, such as an unlinked row filed as Internal Movement, SHALL show its own account on the side its type implies (target for an income, source for an expense), "?" for the other side and the label "Pasangan belum ditemukan". The missing side SHALL NOT be guessed from another row.
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

#### Scenario: Route names the parent account
- **WHEN** the all-accounts ledger shows a linked movement from the Jago pocket Main Pocket to the BCA pocket ATM
- **THEN** the row SHALL read "Jago · Main Pocket → BCA · ATM"

#### Scenario: Inbound leg in an account-filtered list
- **GIVEN** a movement of Rp 400.000 from Jago · Dana Darurat to Jago · Main Pocket
- **WHEN** the ledger is filtered to Main Pocket
- **THEN** the row SHALL read "Jago · Dana Darurat → Jago · Main Pocket" with "Masuk dari Jago · Dana Darurat", and SHALL NOT read "Jago · Main Pocket → Jago · Dana Darurat"

#### Scenario: Unlinked movement income
- **GIVEN** an unlinked income of Rp 500.000 on Jago · Main Pocket filed as Internal Movement
- **WHEN** the ledger or Rekening detail lists it
- **THEN** the row SHALL read "? → Jago · Main Pocket" with "Pasangan belum ditemukan"

#### Scenario: Transaction rows carry parent names
- **WHEN** the client requests `GET /api/transactions` or a transaction's detail
- **THEN** each row SHALL include `account_parent_name` and `transfer_target_account_parent_name`, `null` for an account without a parent or a leg without a partner, and every existing field SHALL keep its value

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

### Requirement: Ledger Day Totals and Type Counts
`GET /api/transactions` SHALL accept `include=daily_totals` and `include=type_counts`, and each row SHALL carry `local_date`. `daily_totals` SHALL cover every local day of the returned rows, computed over every row matching the filters on that day with the summary's reportable rules, and SHALL carry `spending` beside the existing fields; `type_counts` SHALL count matching rows per tab, ignoring the type filter. Without `include`, the response SHALL otherwise be unchanged.

#### Scenario: Day totals add up to the summary
- **GIVEN** a filtered window whose rows span three pages
- **WHEN** all three pages are requested with `include=daily_totals`
- **THEN** the sum of each distinct day's `inflow` SHALL equal the summary inflow, the sum of `outflow` SHALL equal the summary's `gross_outflow`, and the sum of `spending` SHALL equal the summary's `spending`

#### Scenario: Split day
- **WHEN** a day's rows are split across two pages
- **THEN** both pages SHALL report the same totals and `count` for that day

#### Scenario: Movement counted once
- **WHEN** a day holds a Rp 500.000 movement from Bank Utama to Dompet Digital with both legs matching
- **THEN** that day's `movements` SHALL be 500.000 and its `inflow` and `outflow` SHALL not include it

#### Scenario: Local day boundary
- **WHEN** an expense is recorded at 2026-11-02 23:30 in Asia/Jakarta (16:30 UTC)
- **THEN** its `local_date` and its day total SHALL be 2026-11-02

#### Scenario: Tab counts
- **WHEN** the ledger is requested with `type=expense&include=type_counts`
- **THEN** `type_counts` SHALL count the expense, income, movement and investment rows of the other filters, and a top-up leg SHALL count only under investment

#### Scenario: Only the owner's rows
- **WHEN** another owner has rows on the same days
- **THEN** none of them SHALL appear in this owner's `daily_totals` or `type_counts`

#### Scenario: Unknown include value
- **WHEN** `include=foo` is sent
- **THEN** the response SHALL be 422

### Requirement: Ledger Type and Waiting Filters
`GET /api/transactions` SHALL accept `type=movement` (internal movements and excluded categories that are not part of a top-up), `type=investment` (top-up legs and Investasi rows) and `waiting=true` (rows in the Tinjau attention set). `type=transfer` SHALL keep its existing meaning.

#### Scenario: Waiting filter matches the badge
- **WHEN** the ledger is requested with `waiting=true` and no other filter
- **THEN** `total` SHALL equal the badge's `attention` count

#### Scenario: Investment tab
- **WHEN** the owner selects "Investasi" after converting an RDN Contoh debit into a top-up of Bibit Contoh · Reksa Dana A
- **THEN** both legs of that top-up SHALL be listed and the "Pindah saldo" tab SHALL not list them

### Requirement: Loading More Ledger Rows
The ledger SHALL load 50 rows at a time and offer "Muat 50 lagi" to append the next rows in a stable order, without repeating a row, while keeping day headings continuous.

#### Scenario: Stable order across pages
- **GIVEN** several rows with the same `date` and `created_at`
- **WHEN** the owner loads every page
- **THEN** each row SHALL appear exactly once

#### Scenario: Load more announces the rows
- **WHEN** the owner presses "Muat 50 lagi"
- **THEN** the rows SHALL be appended, a polite message SHALL say how many were loaded, and focus SHALL stay on the button or move to the first new row

#### Scenario: Load more fails
- **WHEN** loading the next rows fails
- **THEN** the loaded rows SHALL stay and the error SHALL appear beside the button with "Coba lagi"

### Requirement: Confirming a Row With Undo
"Tandai sudah dicek" in the transaction drawer SHALL accept the row through `POST /api/review/bulk` and SHALL offer "Urungkan" with the returned undo token. It SHALL be offered only for automatic rows and needs-review rows without an open question that are not unpaired movement legs.

#### Scenario: Confirm and undo
- **WHEN** the owner marks an automatic "Kedai Awan Rp 30.000" row "sudah dicek" and then presses "Urungkan"
- **THEN** the row SHALL be reviewed after the first step and automatic again after the undo, and the Belum dicek count SHALL drop and return

#### Scenario: Undo after a later edit
- **GIVEN** the owner marked a row "sudah dicek" and then changed its category
- **WHEN** the owner presses Z
- **THEN** the undo SHALL be refused with the message "Tidak dapat dibatalkan: transaksi sudah berubah sejak itu" and the row SHALL keep the new category

### Requirement: Ledger Type Switch
The drawer's type control SHALL switch an ordinary row between Keluar and Masuk through the existing update, SHALL convert an eligible expense into a "Top up Investasi" through the existing conversion that keeps both legs paired, and SHALL start the existing two-row merge for "Pindah saldo".

#### Scenario: Convert to a top-up
- **WHEN** the owner switches an RDN Contoh debit of Rp 112.590 to "Top up Investasi" and picks Bibit Contoh · Reksa Dana A
- **THEN** the debit SHALL become the outbound leg, an inbound leg on the product SHALL exist with the same movement id, the product's value and cost SHALL rise by Rp 112.590, and the toast SHALL offer "Urungkan"

#### Scenario: Not eligible for a top-up
- **WHEN** the row is income, linked to a goal or debt, or on a non-liquid account
- **THEN** "Top up Investasi" SHALL be unavailable with the reason shown

#### Scenario: Pindah saldo needs a pair
- **WHEN** the owner chooses "Pindah saldo" for an ordinary expense
- **THEN** the drawer SHALL close and selection mode SHALL start with that row selected, and no row SHALL change until the owner confirms "Jadikan Pindah Saldo"

### Requirement: Transaksi Keys
Transaksi SHALL register J and K to move between rows and Z to undo the newest confirmation or conversion, as a page group of the shortcut registry scoped to the list region. The keys SHALL be skipped while typing, SHALL be listed in help while the page is open, and SHALL follow the off switch.

#### Scenario: Walk the list
- **WHEN** focus is on a row and the owner presses J twice and then Enter
- **THEN** focus SHALL move two rows down, across a day heading if needed, and Enter SHALL open that row's drawer

#### Scenario: Typing in search
- **WHEN** focus is in the search field and the owner types "j"
- **THEN** the letter SHALL be typed and focus SHALL not move

#### Scenario: Switched off
- **WHEN** single-key shortcuts are switched off
- **THEN** J, K and Z SHALL do nothing on Transaksi

### Requirement: Return to the Tinjau Item
A ledger link with `return=review` SHALL return, when the drawer closes, to `/review?item=<the edited row id>` with the `folder` it carried when that folder is valid. No other return target SHALL be followed.

#### Scenario: Back to the same item
- **WHEN** the owner opens "Ubah" on a row of the Tinjau item "Kedai Awan" in "Perlu jawaban", saves, and the drawer closes
- **THEN** the page SHALL be `/review?item=<that row id>&folder=jawab` with focus on the heading of the item that holds the row

#### Scenario: Item resolved meanwhile
- **WHEN** the edit resolved the item
- **THEN** Tinjau SHALL select the next item by its deep-link rule

#### Scenario: Tampered return
- **WHEN** the ledger opens with `return=https://example.org`, or with `return=review&folder=../x`
- **THEN** closing SHALL stay on the ledger for the unknown target, and SHALL go to `/review?item=<row id>` without a folder for the invalid folder

### Requirement: Ledger Spending Figure
The ledger summary of `GET /api/transactions` SHALL add `spending`, the outflow net of applied reimbursements without expenses whose effective pillar is saving, and `saving`, those saving-pillar expenses. Existing fields SHALL keep their names and values.

#### Scenario: Saving-classified expense
- **GIVEN** a period with Rp 1.000.000 of expenses, of which Rp 200.000 is in a category whose pillar is saving, and no reimbursement
- **WHEN** the ledger summary is requested
- **THEN** `outflow` SHALL be Rp 1.000.000, `spending` Rp 800.000 and `saving` Rp 200.000

#### Scenario: Equal to Beranda
- **WHEN** the ledger is filtered to the open salary cycle without other filters
- **THEN** its `spending` SHALL equal the cycle summary's `spending`
