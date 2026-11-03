# Spec Delta

## MODIFIED Requirements

### Requirement: Searchable & Filterable Ledger View
The application SHALL provide a dedicated `/ledger` screen showing recorded cash movements grouped by local day, newest first, with type tabs, filter chips, search and period figures that match the active filters across all matching rows.

#### Scenario: Required behavior and constraints
- **WHEN** an owner opens or filters the ledger
- **THEN** the following required behavior and constraints hold:

  The application SHALL provide a dedicated `/ledger` screen with real-time filtering:
  1. Full-text search by notes, merchant, or category name.
  2. Type tabs "Semua", "Keluar", "Masuk", "Pindah saldo" and "Investasi", each with the count of rows matching the other filters.
  3. Single-select chips "Menunggu jawaban", "Belum dicek", "Dari HP" and "Lewat Catat"; account, category and pillar filters behind "Filter"; every active filter shown as a removable chip.
  4. Cash flow figures for the filtered rows: the payday cycle (or labelled calendar month) and cumulative totals, or the selected period's figures, labelled Masuk, Belanja and Bersih, matching active filters and independent of how many rows are loaded.
  5. Rows grouped by local day with one heading per day, holding that day's Belanja, Masuk and the number of rows that are "menunggu jawaban".
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
- **THEN** the day's Belanja SHALL include Rp 45.000 and the heading SHALL say "1 menunggu jawaban", never that it is not counted

#### Scenario: Empty result
- **WHEN** no row matches the filters
- **THEN** the ledger SHALL say "Tidak ditemukan", name the search text if any, and offer "Hapus filter"

### Requirement: Transaction Detail & Inline Editing
The application SHALL let users open any transaction row in a detail drawer (a side column on a wide window, an overlay on a narrower one, a bottom sheet on a phone) that shows the row's provenance and supports editing, a type switch, confirming and deleting.

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

## ADDED Requirements

### Requirement: Ledger Day Totals and Type Counts
`GET /api/transactions` SHALL accept `include=daily_totals` and `include=type_counts`, and each row SHALL carry `local_date`. `daily_totals` SHALL cover every local day of the returned rows, computed over every row matching the filters on that day with the summary's reportable rules; `type_counts` SHALL count matching rows per tab, ignoring the type filter. Without `include`, the response SHALL otherwise be unchanged.

#### Scenario: Day totals add up to the summary
- **GIVEN** a filtered window whose rows span three pages
- **WHEN** all three pages are requested with `include=daily_totals`
- **THEN** the sum of each distinct day's `inflow` SHALL equal the summary inflow and the sum of `outflow` SHALL equal the summary's `gross_outflow`

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
