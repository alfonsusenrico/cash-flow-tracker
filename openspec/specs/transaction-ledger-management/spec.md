# transaction-ledger-management Specification

## Purpose
Provide searchable ledger records, inspection and editing, recurring origins and consolidated internal movements.

## Requirements

### Requirement: Searchable & Filterable Ledger View
The application SHALL provide a `/ledger` screen in the interface language that lists recorded cash movements grouped by local day, newest first. From 1200 px a sticky type and period rail sits beside the list; from 768 to 1199 px a type control sits above it. A search field, "Filter" and the active filters as removable chips sit above the rows, and the period figures match the active filters across all matching rows.

#### Scenario: Required behavior and constraints
- **WHEN** an owner opens or filters the ledger
- **THEN** the following required behavior and constraints hold:

  The application SHALL provide a dedicated `/ledger` screen with real-time filtering:
  1. Full-text search by notes, merchant, or category name, in a field labelled "Cari catatan, merchant, kategori".
  2. The types "Semua", "Pengeluaran", "Masuk", "Pindah saldo", "Investasi" and "Tabungan & investasi", each with the count of rows matching the other filters: in the rail's JENIS group from 1200 px, and in one horizontal type control below 1200 px.
  3. "Filter" opens the "Filter transaksi" dialog and shows the number of active filters. Every active filter, including the search text, is shown as a removable chip below the toolbar only while set. The page's "…" holds "Gabungkan dua transaksi" and "Filter transaksi".
  4. Cash flow figures for the filtered rows: the payday cycle (or labelled calendar month) and cumulative totals, or the selected period's figures, shown as the hero "Pengeluaran" (the summary's `spending`) with "Masuk" and "Bersih" beside it, matching active filters and independent of how many rows are loaded.
  5. Rows grouped by local day with one sticky heading per day, holding that day's Masuk, Pengeluaran (`spending`), Pindah saldo and the number of rows that are "menunggu jawaban".
  6. Below 768 px, the phone composition of "Mobile Date-Grouped Transaction Ledger"; from 768 px up, the desktop rows at every width.

#### Scenario: Searching transactions by note
- **WHEN** the user types "coffee" in the ledger search input
- **THEN** the list immediately filters to show only transactions containing "coffee" in notes or category name
- **AND** a chip "“coffee”" SHALL appear, and removing it SHALL clear the search

#### Scenario: Filtering transactions by account
- **WHEN** the user selects "Main Bank" in the Filter dialog's Rekening field and applies it
- **THEN** only transactions originating from or transferring into "Main Bank" are displayed, and a chip "Main Bank" SHALL appear

#### Scenario: Displaying running cycle and cumulative cash flow
- **WHEN** the user views the ledger with any number of rows loaded
- **THEN** the figures display the running salary-cycle (or labelled calendar-month) inbound, spending and net cash flow, or the cumulative overall totals without a period, rather than merely summing the loaded rows

#### Scenario: Filtering ledger updates both cycle and cumulative summaries
- **WHEN** the user applies an account, category, or search filter
- **THEN** the ledger figures display the cycle totals and cumulative totals matching the filtered criteria across all matching transactions

#### Scenario: Day heading covers the whole day
- **GIVEN** 60 matching rows, of which 2 November holds rows 45 to 52
- **WHEN** the first 50 rows are loaded
- **THEN** the 2 November heading SHALL show the totals of all 8 rows of that day and SHALL say "2 lagi di hari ini · Muat lagi" under its loaded rows

#### Scenario: Waiting row is counted in its day
- **GIVEN** an expense of Rp 45.000 with an open "Apa ini?" question on 2 November
- **WHEN** the ledger shows 2 November
- **THEN** the day's Pengeluaran SHALL include Rp 45.000 and the heading SHALL say "1 menunggu jawaban", never that it is not counted

#### Scenario: Empty result
- **WHEN** no row matches the filters or the search
- **THEN** the ledger SHALL say "Tidak ada transaksi yang cocok" with "Ubah pencarian atau filter.", the search text SHALL stay visible as a chip, and "Hapus semua filter" SHALL clear every filter except the period

#### Scenario: Laptop width keeps the desktop rows
- **WHEN** the owner opens Transaksi at 1024 px or 1100 px
- **THEN** the rows SHALL be the one-line desktop rows with time, title, category, pillar and amount, the type control SHALL sit above the search, and the phone layout SHALL NOT appear

#### Scenario: Rail from 1200 px
- **WHEN** the owner opens Transaksi at 1440 px
- **THEN** the JENIS and PERIODE rail SHALL be 200 px wide beside the list with a 32 px gap, stay in view below the top bar while the list scrolls, and no horizontal scroll SHALL appear

### Requirement: Transaction Detail & Inline Editing
The application SHALL open any transaction row in a transaction dialog, a centered 560 px dialog from 768 px and a bottom sheet below. It shows the amount, the row's facts and provenance, and "Hapus" and "Ubah". "Ubah" opens the "Ubah transaksi" dialog in its place, with the type switch and every editable field. The dialog SHALL NOT open as a side column or overlay, and SHALL NOT narrow the page behind it.

#### Scenario: Required behavior and constraints
- **WHEN** an owner opens transaction details or edits a ledger payment
- **THEN** the following required behavior and constraints hold:

  The application SHALL allow users to select any transaction row to open a transaction dialog supporting:
  1. Editing the transaction amount with automatic balance recalculation on affected accounts.
  2. Changing the associated account or category.
  3. Updating notes and transaction timestamp.
  4. Linking or unlinking savings goals (`goal_id`) and debt obligations (`obligation_id`), or assigning a debt payment among multiple obligations with explicit amounts (`obligation_allocations`).
  5. Showing each debt name and assigned amount for an allocated payment in ledger detail, including debts that were archived by that payment; the transaction SHALL remain one ledger row.
  6. Deleting the transaction with confirmation, cleanly reversing its balance impact on accounts, goals, or obligations.
  7. Showing where the row came from ("Dicatat"), who classified it ("Diklasifikasikan": "AI", "Kebiasaan", "Pengguna", "Aturan"), whether it was checked ("Dicek"), whether a receipt is saved ("Bukti"), and the original notification or typed text when the row has one.
  8. Opening the movement edit for a movement row and the existing top-up edit dialog for a top-up row.
  9. Titling a movement row with its route in the direction the money moved, with the same "Dari" and "Ke" accounts as the route; an unlinked leg's side is taken from its type and its missing side is shown as "?".
  10. Showing a movement's linked transfer fee, and a fee row's movement, from the fields the API returns.

  The Quick Capture and ledger edit forms SHALL let a user select multiple active debts for an expense. Selecting a new debt SHALL default its payment amount to that debt's outstanding balance and SHALL update the transaction total to the sum of selected payments. The default view SHALL show each amount without requiring a separate input. An explicit amount-edit action SHALL allow partial payments per debt and SHALL update the total as amounts change. The form SHALL prevent submission when a selected amount is not positive, exceeds the available debt balance, or does not match the transaction total. Reopening a saved payment SHALL show its recorded amounts rather than replacing them with current debt balances. The same behavior and feedback SHALL be available in desktop dialogs and mobile sheets. A single-debt selection MAY keep the existing API field for compatibility.

#### Scenario: Modifying transaction amount
- **WHEN** the user changes a transaction amount from 50,000 to 75,000 IDR in "Ubah transaksi" and clicks Simpan
- **THEN** the backend updates the transaction record, reconciles the corresponding account balance by -25,000 IDR, the transaction dialog returns with "Perubahan disimpan.", and the updated numbers appear across the UI

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
- **WHEN** the owner presses "Hapus" in the transaction dialog
- **THEN** nothing SHALL be deleted until the owner confirms "Hapus transaksi {amount}?" with the list of balance, goal or debt changes and "Penghapusan tidak dapat dibatalkan."; "Batal" SHALL keep the row and take focus

#### Scenario: Deleting a top-up row
- **WHEN** the owner deletes a top-up converted from a debit and confirms
- **THEN** the dialog SHALL use the top-up delete path, the inbound leg SHALL be gone and the original expense SHALL be restored

#### Scenario: Waiting row points to Tinjau
- **WHEN** the owner opens a row with an open question or an unpaired movement leg
- **THEN** the dialog SHALL show "Transaksi ini menunggu jawaban di Tinjau." with "Jawab di Tinjau" linking to that item and SHALL NOT offer "Tandai sudah dicek"

#### Scenario: Drawer on a phone
- **WHEN** the owner opens a row at 390 px width
- **THEN** the dialog SHALL be a bottom sheet with focus inside it, and the browser's back SHALL close it and return focus to the row

#### Scenario: Detail opens centered on a wide window
- **WHEN** the owner opens a row at 1440 px or 1024 px width
- **THEN** the detail SHALL open as a centered 560 px dialog over a dimmed page with focus inside it, and no side column SHALL appear
- **AND** Esc, the close button or a click on the dimmed page SHALL close it and return focus to the row

#### Scenario: Edit returns to the detail
- **WHEN** the owner presses "Ubah", then "Batal" or Esc in "Ubah transaksi"
- **THEN** the transaction dialog SHALL be shown again with focus on "Ubah", and no request SHALL be sent

#### Scenario: Expense amount in the dialog
- **WHEN** the owner opens an expense of Rp 120.000 in either theme
- **THEN** the amount SHALL read "−Rp 120.000" in the expense red at 32 px, measuring at least 4.5:1 on the dialog, and the dialog SHALL name its kind in words under "Jenis"

#### Scenario: Movement with a fee
- **GIVEN** a transfer of Rp 200.000 from Jago · Kantong Utama to GoPay with a linked fee row of Rp 1.000
- **WHEN** the owner opens the transfer
- **THEN** the dialog SHALL list "Dari", "Ke", "Biaya Rp 1.000 · Lihat transaksi biaya" and "Status pasangan Lengkap", and "Lihat transaksi biaya" SHALL open the fee row in the same dialog

#### Scenario: Inbound leg titled in its direction
- **GIVEN** an inbound leg on Jago · Main Pocket linked to an outbound leg on Jago · Dana Darurat
- **WHEN** the owner opens the inbound leg from Rekening detail of Main Pocket
- **THEN** the title SHALL read "Jago · Dana Darurat → Jago · Main Pocket", "Dari" SHALL be Jago · Dana Darurat and "Ke" Jago · Main Pocket

#### Scenario: Movement edit error in the interface language
- **GIVEN** an Indonesian interface and a movement edit the server rejects with the code for different source and target accounts
- **WHEN** the owner saves "Ubah pindah saldo"
- **THEN** the dialog SHALL show the Indonesian message of that code, never "Source and target accounts must be different", and both legs SHALL stay unchanged

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
The application SHALL persist and present each bilateral internal movement as one logical operation backed by two durably linked ledger records, listed as one row that names its route and is shown as a transfer by its words and colour.

#### Scenario: Required behavior and constraints
- **WHEN** an internal movement is persisted, displayed, edited or deleted
- **THEN** the following required behavior and constraints hold:

  The application SHALL persist and present each bilateral internal movement as one logical operation backed by two durably linked ledger records:
  1. Both records SHALL share an immutable movement identifier assigned by the server.
  2. In the default "Semua Rekening" view, durably linked records SHALL render as one consolidated row regardless of their timestamp difference.
  3. The row SHALL be titled by the movement's note, as written by the owner or stored by the server, with its route `Sumber → Tujuan` in the direction the money moved as the meta; without a note, or with the server's placeholder note, the route SHALL be the title. The category column SHALL read "Pindah saldo", the pillar column SHALL name its saving direction ("Ditabung", "Diambil dari tabungan", "Investasi", "Bayar utang") or "—", never "Tidak dihitung", and the amount SHALL be in the transfer colour without a `+` or `−` sign, reflecting zero net cash flow change. A pocket SHALL be named with its parent account, as `Jago · Main Pocket`; an account without a parent SHALL be named alone.
  4. In account-filtered views, the movement SHALL display its full route and its direction relative to the selected account, as "Masuk dari" or "Keluar ke" the other account. A leg shown without its partner SHALL take its route from its own movement role and the partner's account, never reversed.
  4a. A movement leg without a partner, such as an unlinked row filed as Internal Movement, SHALL show its own account on the side its type implies (target for an income, source for an expense), "?" for the other side and the label "Pasangan belum ditemukan". The missing side SHALL NOT be guessed from another row.
  5. Creating, editing, or deleting the logical movement SHALL atomically create, update, or delete both records through a server-owned movement operation.
  6. In the all-accounts ledger, two unlinked rows identified as internal movements SHALL render as one inferred row when they have opposite directions, equal positive amounts, different accounts, timestamps less than 30 seconds apart, and exactly one mutually eligible partner in the loaded ledger page. This match SHALL be display-only and SHALL NOT assign a movement identifier or enable atomic pair edit/delete.
  7. A linked movement SHALL never be matched heuristically with an unlinked row, and ambiguous or incomplete legacy rows SHALL remain separate.
  8. The user SHALL be able to select exactly two unlinked transactions, including transactions not categorized as internal movements, by choosing "Gabungkan dua transaksi" in the page's "…" menu, or on a phone by holding an eligible row, and then toggling rows by their checkboxes. Holding SHALL enter selection mode and select that row; holding a uniquely inferred movement row SHALL select both underlying records together. Selection SHALL stop at two underlying records. "Batal gabungkan" SHALL exit selection mode. A short click outside selection mode SHALL continue to open the record. Selection SHALL survive ledger pagination and filters.
  9. The ledger SHALL NOT show a row-level `Gabungkan` action or an `Aksi` column. In selection mode, a selection bar SHALL stay in view under the top bar with "{n} dari 2 dipilih", "Batal gabungkan" and "Gabungkan jadi pindah saldo", which is enabled once exactly two records are selected. Confirmed linked movements SHALL remain consolidated; uniquely inferred legacy pairs SHALL remain consolidated while selecting. Inferred and unlinked rows SHALL NOT display `perkiraan` or `Belum tertaut` labels.
  10. "Gabungkan jadi pindah saldo" SHALL fetch current transaction details and directly submit a pair with equal amounts, without an additional confirmation preview. If one income and one expense have different amounts, the UI SHALL NOT call the conversion endpoint and SHALL show the centered dialog "Nominal tidak sama" with a single "Tutup" button. For eligible equal-value rows, the server SHALL atomically verify same owner, one income and one expense, equal positive amounts, different eligible liquid accounts, no existing movement linkage, and no plan, goal, transfer-fee or other financial association that conversion would corrupt; it SHALL set the shared movement identifier, roles, and internal-movement classifications without creating or deleting transactions or changing balances. Rejected conversion SHALL leave both records unchanged.

#### Scenario: Viewing a durably linked movement
- **WHEN** the user views the all-accounts ledger after moving funds between two accounts without a note
- **THEN** the two records sharing the movement identifier render as one row titled with the correct source and destination, "Pindah saldo" in its category column and its amount in the transfer colour

#### Scenario: Transfer with a note
- **GIVEN** a movement of Rp 434.000 from Jago · Tabungan to Jago · Bayar Kredit with the note "Cicilan SPayLater"
- **WHEN** the ledger lists it
- **THEN** the row SHALL be titled "Cicilan SPayLater" with the meta "Jago · Tabungan → Jago · Bayar Kredit"

#### Scenario: Placeholder note
- **WHEN** a movement's stored note is the server's default placeholder for no note
- **THEN** the row SHALL be titled by its route, and the placeholder SHALL NOT be shown

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
- **WHEN** the user holds an eligible row for one second on a phone
- **THEN** the ledger enters selection mode with that row checked, subsequent row taps add or remove selections up to two, and "Batal gabungkan" exits selection mode

#### Scenario: Selecting transactions from the menu
- **WHEN** the owner chooses "…" › "Gabungkan dua transaksi" at 1440 px and presses Space on two rows' checkboxes
- **THEN** both rows SHALL be checked, the selection bar SHALL read "2 dari 2 dipilih", and "Gabungkan jadi pindah saldo" SHALL be enabled

#### Scenario: Editing with a short click
- **WHEN** the user clicks a row without entering selection mode
- **THEN** the transaction dialog opens for that record

#### Scenario: Keeping inferred pairs consolidated during selection
- **WHEN** the user enters selection mode while the all-accounts ledger contains a uniquely inferred legacy pair
- **THEN** the pair remains one `Pindah saldo` row without a `perkiraan` or `Belum tertaut` label

#### Scenario: Selecting an inferred pair as a unit
- **WHEN** the user holds a uniquely inferred legacy movement row for one second
- **THEN** both underlying transaction records are selected together and the ledger continues to show one row

#### Scenario: Keeping durable movement status during selection
- **WHEN** the user enters selection mode while the ledger contains a durably linked movement
- **THEN** that movement remains one consolidated row whose checkbox is disabled

#### Scenario: Converting two equal-value transactions directly
- **WHEN** exactly one eligible expense and one eligible income from different accounts are selected and their current amounts match
- **THEN** "Gabungkan jadi pindah saldo" submits the conversion without a confirmation preview, both existing records receive one shared movement identifier and internal-movement classification atomically, and their balances and original timestamps remain unchanged

#### Scenario: Rejecting unequal transaction amounts before conversion
- **WHEN** the selected expense and income have different amounts
- **THEN** the conversion endpoint is not called and the centered dialog "Nominal tidak sama" names both amounts with a single "Tutup" button

#### Scenario: Confirming two uncategorized transactions as a movement
- **WHEN** the user selects one eligible expense and one eligible income from different accounts and activates "Gabungkan jadi pindah saldo"
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
- **THEN** the row SHALL read "Jago · Dana Darurat → Jago · Main Pocket" (as its title without a note) with "Masuk dari Jago · Dana Darurat", and SHALL NOT read "Jago · Main Pocket → Jago · Dana Darurat"

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
`GET /api/transactions` SHALL accept `include=daily_totals` and `include=type_counts`, and each row SHALL carry `local_date`. `daily_totals` SHALL cover every local day of the returned rows over every row matching the filters on that day, with the summary's reportable rules and `spending`. `type_counts` SHALL count matching rows per tab, including `saving` and `credit`, ignoring the type filter. Without `include`, the response SHALL otherwise be unchanged.

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
- **THEN** `type_counts` SHALL count the expense, income, movement, investment, saving and credit rows of the other filters, a top-up leg SHALL count under investment and, when it is counted as saving, under saving, a credit purchase SHALL count under expense and credit, and no other key SHALL change

#### Scenario: Only the owner's rows
- **WHEN** another owner has rows on the same days
- **THEN** none of them SHALL appear in this owner's `daily_totals` or `type_counts`

#### Scenario: Unknown include value
- **WHEN** `include=foo` is sent
- **THEN** the response SHALL be 422

### Requirement: Ledger Type and Waiting Filters
`GET /api/transactions` SHALL accept `type=movement` (internal movements and excluded categories outside top-ups), `type=investment` (top-up legs and Investasi rows), `type=saving` (rows whose `saving_flow` changes "Ditabung & investasi"), `type=credit` (rows on a credit account and cash legs of movements with one) and `waiting=true` (Tinjau's attention set). With `type=saving` and no other row filter, the summary SHALL carry `saving_net`. `type=transfer` SHALL keep its meaning.

#### Scenario: Waiting filter matches the badge
- **WHEN** the ledger is requested with `waiting=true` and no other filter
- **THEN** `total` SHALL equal the badge's `attention` count

#### Scenario: Investment tab
- **WHEN** the owner selects "Investasi" after converting an RDN Contoh debit into a top-up of Bibit Contoh · Reksa Dana A
- **THEN** both legs of that top-up SHALL be listed and the "Pindah saldo" tab SHALL not list them

#### Scenario: Savings filter equals Beranda
- **GIVEN** a cycle with Rp 1.000.000 moved from harian into a tabungan pocket, Rp 300.000 taken back out, Rp 50.000 spent from the tabungan pocket and Rp 200.000 of saving-pillar expenses
- **WHEN** the ledger is requested with `type=saving&period=cycle`
- **THEN** it SHALL list those four rows, and `saving_net` SHALL be Rp 850.000, equal to the cycle summary's `tabungan.total`

#### Scenario: Savings net only without other filters
- **WHEN** the ledger is requested with `type=saving&account_id=<id>`
- **THEN** the rows SHALL be filtered, `saving_net` SHALL be `null`, and every existing summary field SHALL keep its value

#### Scenario: Credit type filter
- **GIVEN** a SPayLater purchase of Rp 120.000, a SPayLater fee, a refund on SPayLater, a payment of Rp 470.000 from Jago into SPayLater and an ordinary BCA expense
- **WHEN** the ledger is requested with `type=credit&logical_movements=true`
- **THEN** it SHALL list the purchase, the fee, the refund and one row for the payment, SHALL NOT list the BCA expense, and every existing summary field SHALL be computed over those rows as for any type

#### Scenario: Unknown type
- **WHEN** `type=savings` or another unknown value is sent
- **THEN** the response SHALL be 422, as for any value outside the pattern

### Requirement: Loading More Ledger Rows
The ledger SHALL load 50 rows at a time and offer "Muat 50 lagi" after the last day to append the next rows in a stable order, without repeating a row, while keeping day headings continuous. A day with unloaded rows SHALL say "{n} lagi di hari ini · Muat lagi" under its rows, and a fully loaded list SHALL end with "{total} transaksi · akhir daftar".

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

#### Scenario: Load more of one day on a phone
- **WHEN** a day has 8 unloaded rows at 390 px
- **THEN** "8 lagi di hari ini · Muat lagi" SHALL be one centered line across the list's width, without wrapping inside a narrow column

### Requirement: Confirming a Row With Undo
"Tandai sudah dicek" in the transaction dialog's "Dicek" row SHALL accept the row through `POST /api/review/bulk` and SHALL offer "Urungkan" with the returned undo token. It SHALL be offered only for automatic rows and needs-review rows without an open question that are not unpaired movement legs.

#### Scenario: Confirm and undo
- **WHEN** the owner marks an automatic "Kedai Awan Rp 30.000" row "sudah dicek" and then presses "Urungkan"
- **THEN** the row SHALL be reviewed after the first step and automatic again after the undo, and the Belum dicek count SHALL drop and return

#### Scenario: Undo after a later edit
- **GIVEN** the owner marked a row "sudah dicek" and then changed its category
- **WHEN** the owner presses Z
- **THEN** the undo SHALL be refused with the message "Tidak dapat dibatalkan: transaksi sudah berubah sejak itu" and the row SHALL keep the new category

### Requirement: Ledger Type Switch
The "Jenis" control of "Ubah transaksi" SHALL switch an ordinary row between Pengeluaran and Masuk through the existing update, SHALL convert an eligible expense into a "Top up Investasi" through the existing conversion that keeps both legs paired, and SHALL start the existing two-row merge for "Pindah saldo".

#### Scenario: Convert to a top-up
- **WHEN** the owner switches an RDN Contoh debit of Rp 112.590 to "Top up Investasi" and picks Bibit Contoh · Reksa Dana A
- **THEN** the debit SHALL become the outbound leg, an inbound leg on the product SHALL exist with the same movement id, the product's value and cost SHALL rise by Rp 112.590, and the toast SHALL offer "Urungkan"

#### Scenario: Not eligible for a top-up
- **WHEN** the row is income, linked to a goal or debt, or on a non-liquid account
- **THEN** "Top up Investasi" SHALL be unavailable with the reason shown

#### Scenario: Pindah saldo needs a pair
- **WHEN** the owner chooses "Pindah saldo" for an ordinary expense
- **THEN** the dialog SHALL close and selection mode SHALL start with that row selected, and no row SHALL change until the owner confirms "Gabungkan jadi pindah saldo"

### Requirement: Transaksi Keys
Transaksi SHALL register J and K to move between rows and Z to undo the newest confirmation, conversion or merge, as a page group of the shortcut registry scoped to the list region. Enter SHALL open the focused row's dialog. The keys SHALL be skipped while typing, SHALL be listed in help while the page is open, and SHALL follow the off switch.

#### Scenario: Walk the list
- **WHEN** focus is on a row and the owner presses J twice and then Enter
- **THEN** focus SHALL move two rows down, across a day heading if needed, and Enter SHALL open that row's transaction dialog

#### Scenario: Typing in search
- **WHEN** focus is in the search field and the owner types "j"
- **THEN** the letter SHALL be typed and focus SHALL not move

#### Scenario: Switched off
- **WHEN** single-key shortcuts are switched off
- **THEN** J, K and Z SHALL do nothing on Transaksi

#### Scenario: Walk while selecting
- **WHEN** selection mode is on and the owner presses J
- **THEN** focus SHALL move to the next row's checkbox, and Space SHALL toggle it

### Requirement: Return to the Tinjau Item
A ledger link with `return=review` SHALL return, when the transaction dialog closes, to `/review?item=<the edited row id>` with the `folder` it carried when that folder is valid. No other return target SHALL be followed.

#### Scenario: Back to the same item
- **WHEN** the owner opens "Ubah" on a row of the Tinjau item "Kedai Awan" in "Perlu jawaban", saves, and closes the transaction dialog
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

### Requirement: Ledger Table Columns
Each desktop ledger row SHALL be one line with the columns time, title with its meta, category, pillar (136 px) and amount, in that order, without a header row, a type column or a type pill. The category SHALL show the row's category name as the owner wrote it, and the pillar its dot and name, or a transfer's saving direction. The row SHALL NOT show provenance; the transaction dialog SHALL keep showing it.

#### Scenario: Expense shows its category
- **GIVEN** an expense "Indomaret" in the category "Belanja Harian", whose pillar is Kebutuhan
- **WHEN** the owner opens Transaksi on a desktop
- **THEN** its row SHALL show "Belanja Harian" in the category column and the Kebutuhan dot with "Kebutuhan" in the pillar column, and no "Pengguna · Diketik di ponsel" text SHALL appear in the row

#### Scenario: Movement has no category
- **WHEN** the list shows a movement "Jago · Makan-Jalan → Gopay"
- **THEN** the category column SHALL name the kind as "Pindah saldo" and the pillar column SHALL show its saving direction or "—", never "Internal Movement" or "Tidak dihitung"

#### Scenario: Provenance stays in the detail
- **WHEN** the owner opens that expense's dialog
- **THEN** the dialog SHALL still show "Dicatat" and "Diklasifikasikan"

#### Scenario: Row without a category
- **WHEN** the list shows an expense or income without a category, a waiting row without one, or an investment top-up
- **THEN** the category column SHALL read "Tanpa kategori" for the expense or income, "Belum ada kategori" for the waiting row with "Menunggu jawaban" in the warning colour as its pillar, and "Top up investasi" for the top-up

#### Scenario: Amount carries the kind
- **WHEN** the list shows an expense of Rp 30.000, an income of Rp 18.400, a transfer of Rp 200.000 and a top-up of Rp 112.590 in either theme
- **THEN** they SHALL read "−Rp 30.000" in the expense red, "+Rp 18.400" in green, "Rp 200.000" in sky and "Rp 112.590" in lavender, each in 14 px semibold tabular figures measuring at least 4.5:1, with no icon tile or type pill in the row

#### Scenario: Saving direction fits
- **WHEN** a transfer reads "Diambil dari tabungan", or "Taken from savings" in English, at 1024 px
- **THEN** the label SHALL fit the pillar column on one line, and the row SHALL not scroll horizontally

### Requirement: Ledger Saving Flow
Each row of `GET /api/transactions` and `GET /api/transactions/{id}` SHALL carry a read-only `saving_flow`: `saved`, `invested`, `taken`, `paid_from_savings`, `saving_pillar`, `debt_payment` or `null`, by the rule and account roles that compute Beranda's "Ditabung & investasi". A saving label SHALL appear only when the row changes that figure; "Bayar utang" is informational. Both legs SHALL carry the same value, and no existing field or figure SHALL change.

#### Scenario: Saved and taken
- **WHEN** the owner moves Rp 1.000.000 from Jago · Harian to Jago · Tabungan and later Rp 300.000 back
- **THEN** the first movement SHALL carry `saved` and read "Ditabung", and the second SHALL carry `taken` and read "Diambil dari tabungan"

#### Scenario: Goal-linked pocket counts as savings
- **WHEN** the owner moves money from a harian account into a harian pocket linked to a goal
- **THEN** the movement SHALL carry `saved`, as Beranda counts it

#### Scenario: Investment cash is not counted
- **WHEN** the owner moves Rp 2.000.000 from Jago · Harian into RDN Contoh (role `dana_investasi`)
- **THEN** the movement SHALL carry `null` and its pillar column SHALL read "—", it SHALL NOT be listed under "Tabungan & investasi", and Beranda's figure SHALL not change

#### Scenario: Move inside savings and investments
- **WHEN** the owner tops up Bibit Contoh · Reksa Dana A from Jago · Tabungan
- **THEN** the top-up SHALL carry `null` and read "—", it SHALL NOT be listed under "Tabungan & investasi", and Beranda's figure SHALL not change

#### Scenario: Top-up into a product
- **WHEN** an RDN Contoh debit is converted into a top-up of Bibit Contoh · Reksa Dana A
- **THEN** the top-up SHALL carry `invested` and read "Investasi"

#### Scenario: Paying a credit account
- **WHEN** the owner moves money from a harian account into SPayLater, and from a tabungan pocket into SPayLater
- **THEN** the first SHALL carry `debt_payment` and read "Bayar utang" without being listed under "Tabungan & investasi", and the second SHALL carry `taken`, read "Diambil dari tabungan" and be listed

#### Scenario: Ordinary move
- **WHEN** the owner moves money between two harian pockets
- **THEN** the movement SHALL carry `null` and its pillar column SHALL read "—"

#### Scenario: Aggregate unchanged
- **WHEN** the backend suite runs on the parity seed
- **THEN** the sum of `saved` and `invested` movement amounts SHALL equal `saving_into`, the sum of `taken` amounts SHALL equal `saving_out`, and every existing Kakeibo and cycle-summary figure SHALL be unchanged

### Requirement: Transaksi Hero Figures
Transaksi SHALL show one hero "Pengeluaran" with the summary's `spending` in 44 px, and beside it "Masuk" (`inflow`, signed "+", green) and "Bersih · masuk − pengeluaran" (`spending_net`, signed). Without a period the label SHALL add "semua waktu". One muted line SHALL name a reimbursement offset or saved part. For "Tabungan & investasi" the hero SHALL be "Ditabung & investasi" with `saving_net`. Values SHALL be the server's.

#### Scenario: Cycle figures
- **GIVEN** a cycle with `spending` Rp 3.502.990, `inflow` Rp 9.518.400 and `spending_net` Rp 6.015.410
- **WHEN** the owner opens `/ledger?period=cycle`
- **THEN** the hero SHALL read "Rp 3.502.990", "Masuk" "+Rp 9.518.400" and "Bersih · masuk − pengeluaran" "+Rp 6.015.410", and no other figure on the page SHALL be 33 px or larger except the 32 px amount inside an open dialog

#### Scenario: Saved part named
- **GIVEN** a period whose saving-pillar expenses are Rp 400.000 and with no reimbursement offset
- **WHEN** the hero is shown
- **THEN** its line SHALL read "tanpa Rp 400.000 yang ditabung"

#### Scenario: Older API without spending
- **WHEN** the summary carries no `spending`
- **THEN** the hero SHALL read "Belum tersedia" in words, and no number SHALL be invented

#### Scenario: Phone hero
- **WHEN** the owner opens Transaksi at 390 px
- **THEN** the hero SHALL come first, with "Masuk" and "Bersih" side by side under it

#### Scenario: Savings hero
- **WHEN** the owner chooses "Tabungan & investasi" for the open cycle without other filters
- **THEN** the hero SHALL read "Ditabung & investasi" with the summary's `saving_net`, byte-identical to Beranda's "Ditabung & investasi", and "Masuk" and "Bersih" SHALL be hidden

#### Scenario: Savings hero with another filter
- **WHEN** the owner adds an account filter to "Tabungan & investasi"
- **THEN** the hero SHALL read "Tidak tersedia dengan filter lain" in words, and no figure SHALL be computed in the browser

### Requirement: Transaksi Type and Period Rail
From 1200 px Transaksi SHALL show a rail labelled "Jenis dan periode" with JENIS (Semua, Pengeluaran, Masuk, Pindah saldo, Investasi, Tabungan & investasi, Kredit, with counts) and PERIODE (Siklus gaji, Bulan kalender, Semua waktu). The chosen items SHALL be marked by `aria-pressed`, a fill and the straight selection bar. Below 1200 px the types SHALL be a horizontal control and the period mode SHALL be in Filter.

#### Scenario: Choosing a type
- **WHEN** the owner presses "Pengeluaran" in the rail
- **THEN** the URL SHALL gain `type=expense` without a navigation, the list SHALL show only spending rows, and focus SHALL stay on the pressed item

#### Scenario: Choosing a period mode
- **WHEN** the owner presses "Bulan kalender" in the rail
- **THEN** the URL SHALL read `period=month` with the offset reset, the stepper SHALL name the month, and Analisis opened next SHALL show the same period

#### Scenario: Counts while loading
- **WHEN** the first page is still loading
- **THEN** the rail and the type control SHALL show no counts and no invented numbers

### Requirement: Ledger Filter Dialog
"Filter" SHALL open the centered 640 px dialog "Filter transaksi", a bottom sheet below 768 px, with Rekening, Kategori, Pilar, Status, Sumber, "Hanya yang menunggu jawaban di Tinjau", "Kemungkinan duplikat" while set, and Periode. "Terapkan" SHALL write every field to the URL at once; "Batal" and Esc SHALL discard; "Atur ulang" SHALL clear all but the period. Focus SHALL return to the opener.

#### Scenario: Apply several filters at once
- **WHEN** the owner chooses Rekening "BCA", Status "Belum dicek" and Sumber "Dari HP" and presses "Terapkan"
- **THEN** the URL SHALL be written once with `account`, `review=auto` and `source=notification`, three chips SHALL appear, "Filter · 3" SHALL show the count, and focus SHALL return to "Filter"

#### Scenario: Cancel keeps the filters
- **WHEN** the owner changes the Kategori field and presses Esc
- **THEN** the URL and the list SHALL be unchanged

#### Scenario: Duplicate filter from Laporan
- **WHEN** the owner opens `/ledger?dup=1` and then the Filter dialog
- **THEN** "Kemungkinan duplikat" SHALL be checked, and clearing it and applying SHALL remove `dup` from the URL

#### Scenario: Period below 1200 px
- **WHEN** the owner opens the Filter dialog at 1024 px and chooses "Semua waktu"
- **THEN** applying SHALL remove `period` and `offset` from the URL and hide the stepper

### Requirement: Ledger Day Headings
Each day group SHALL start with a sticky 40 px heading under the top bar, labelled with the uppercase weekday and date and "hari ini" or "kemarin" when they apply. At its right it SHALL show the day's Masuk, Pengeluaran and Pindah saldo from the server's day totals and the number of rows waiting for an answer, in muted text with only the waiting count in the warning colour.

#### Scenario: Day with every kind
- **WHEN** a day heading shows Masuk Rp 100.000, Pengeluaran Rp 35.000, Pindah saldo Rp 21.701 and 1 waiting row
- **THEN** it SHALL read "Masuk Rp 100.000 · Pengeluaran Rp 35.000 · Pindah saldo Rp 21.701 · 1 menunggu jawaban", the amounts SHALL be muted, and "1 menunggu jawaban" SHALL use the warning colour

#### Scenario: Day without totals from an older API
- **WHEN** the server sends no `daily_totals`
- **THEN** the heading SHALL show only the date

### Requirement: Ledger Loading, Failure and Empty States
While the first page loads, Transaksi SHALL show skeleton rows and skeleton figures with `aria-busy` and no numbers. A failed first page SHALL show an alert "Transaksi belum bisa dimuat" with "Coba lagi" and no figures. With no row at all, it SHALL say "Belum ada transaksi" and offer "Catat transaksi"; with filters that match nothing, "Tidak ada transaksi yang cocok".

#### Scenario: Loading
- **WHEN** the first page has not arrived
- **THEN** six skeleton rows SHALL be shown in a busy status region named "Memuat transaksi", and no amount, count or date figure SHALL be shown

#### Scenario: Failure and retry
- **WHEN** the first page fails and the owner presses "Coba lagi"
- **THEN** the request SHALL be sent again, and the alert SHALL be replaced by the rows when it succeeds

#### Scenario: Nothing recorded yet
- **WHEN** the owner has no transaction and opens `/ledger` without filters, search or period
- **THEN** the page SHALL say "Belum ada transaksi" with "Transaksi dari HP dan yang dicatat manual muncul di sini." and "Catat transaksi", which opens Catat

### Requirement: Transaksi in Both Languages
Transaksi and its dialogs SHALL render in the interface language: "Transactions", "Spending", "Income", "Net" and "Transfer" in English, and today's Indonesian with "Pengeluaran" for spending. User data SHALL be shown as stored without a `lang` attribute. The untranslated-page notice SHALL NOT appear on Transaksi. Every figure, count and date SHALL be identical in both languages.

#### Scenario: English Transaksi
- **WHEN** an English account opens `/ledger?period=cycle`
- **THEN** the title SHALL read "Transactions", the hero "Spending", the rail "Type" and "Period", no "This page is in Indonesian for now." line and no `lang="id"` region SHALL be present, and no label SHALL use "Expense", "Spent", "Outflow" or "Ledger"

#### Scenario: Same figures in both languages
- **WHEN** the same data is shown to an English and an Indonesian account in region Indonesia
- **THEN** every amount, count and date number on the page and in the dialogs SHALL be byte-identical

#### Scenario: Hidden balances
- **WHEN** "Sembunyikan saldo" is on
- **THEN** every amount on Transaksi and in its dialogs SHALL read "Rp ••••••" and be announced as hidden in the interface language

### Requirement: Credit Purchase Rows
A credit purchase row SHALL carry the meta "dibayar nanti · {provider} · jatuh tempo {date}" ("pay later · {provider} · due {date}"). With instalments it SHALL add "cicilan {k}/{n}" ("installment {k} of {n}"), and once settled it SHALL read "{provider} · lunas {date}" ("paid {date}"). Its amount SHALL be in the credit colour until it is fully settled and in the expense red afterwards, and it SHALL count as Pengeluaran on its day.

#### Scenario: Credit purchase meta
- **GIVEN** a Rp 120.000 SPayLater dinner on 5 Nov whose next due date is 11 Dec
- **WHEN** Transaksi lists it
- **THEN** its meta SHALL read "dibayar nanti · SPayLater · jatuh tempo 11 Des" and its amount "−Rp 120.000" in the credit colour (`--credit-fg`)

#### Scenario: Instalments and settlement
- **WHEN** a purchase in 3 instalments has 1 settled, and another purchase was settled on 4 Dec
- **THEN** the first SHALL read "dibayar nanti · SPayLater · cicilan 2/3 · jatuh tempo …" in the credit colour, and the second "SPayLater · lunas 4 Des" in the expense red

### Requirement: Credit Payment Rows
A movement into a credit account SHALL keep its title and route and SHALL add "bayar tagihan" ("bill payment") to its meta.

#### Scenario: Credit payment meta
- **WHEN** Jago pays Rp 470.000 into SPayLater without a note
- **THEN** the row SHALL be titled "Jago → SPayLater" with the meta "bayar tagihan", and with the note "Bayar SPayLater" it SHALL be titled by the note with the meta "Jago → SPayLater · bayar tagihan"

### Requirement: Credit Provider View
The type "Kredit" SHALL list the credit accounts as providers to choose from. With one credit account chosen as the account filter, the page SHALL show that account's Utang, next bill, "Pengeluaran pakai kredit" and "Tagihan kredit dibayar" from `GET /api/credit/accounts/{id}` over the same window as the listed rows, and the Rekening filter SHALL list credit accounts in a "Kredit" group.

#### Scenario: Provider view
- **WHEN** the owner chooses "Kredit" and then "SPayLater"
- **THEN** the URL SHALL carry `type=credit&account=<id>`, the list SHALL hold SPayLater's purchases, fees, refunds and payments only, and the header SHALL show the four figures of the endpoint for the page's period, hidden with "Sembunyikan saldo"

#### Scenario: A purchase timed later today
- **GIVEN** the cycle view of SPayLater at 12.39 with Rp 459.000 spent, and a Rp 77.000 SPayLater purchase timed 23.00 today
- **WHEN** the owner opens the provider view
- **THEN** the header's "Pengeluaran pakai kredit" and the hero's Pengeluaran SHALL both read Rp 459.000, because the header requests `period=cycle&period_offset=0` as the list does

#### Scenario: A custom range
- **WHEN** the owner filters SPayLater's rows from 1 to 15 October
- **THEN** the header SHALL request the list's `from_date` and `to_date` instants, and its "Pengeluaran pakai kredit" SHALL equal the hero's Pengeluaran

### Requirement: Credit Terms in the Transaction Dialog
The dialog of a credit purchase SHALL show how it is paid, its next due date, its instalments with due dates and state, and its settlement date and days. "Ubah transaksi" SHALL change the instalment count and the first due date through `PUT /api/transactions/{id}/credit-terms`, and return it to paid in full through `DELETE`. Paying in full SHALL stay the default. A stored first due date that the owner did not change SHALL be sent back as it is, whoever set it.

#### Scenario: Change the instalments
- **WHEN** the owner changes a paid-in-full SPayLater purchase to 3 instalments and saves
- **THEN** the web SHALL send today's `PATCH` and then `PUT /api/transactions/{id}/credit-terms` with `{installment_count: 3, first_due_date: null}`, and the dialog SHALL list three instalments

#### Scenario: Back to paid in full
- **WHEN** the owner chooses "Sekali bayar" without a first due date for a purchase with terms and no kept due date
- **THEN** the web SHALL send `DELETE /api/transactions/{id}/credit-terms`

#### Scenario: Count changed on a purchase with a kept due date
- **GIVEN** a purchase whose stored first due date 11 November was kept by the system after a payment-day change
- **WHEN** the owner changes only the instalments to 3, or back to "Sekali bayar", and saves
- **THEN** the web SHALL send `PUT /api/transactions/{id}/credit-terms` with that count and `first_due_date` 11 November, and no `DELETE`

### Requirement: Perpindahan Default Accounts
When Catat opens Perpindahan without a chosen target, "Ke" SHALL default to the first liquid account that is not "Dari", is not an account with pockets, and is not the pocket that "Dari" resolves to (its default pocket, or any of its pockets when it has none). When no account qualifies, "Ke" SHALL stay empty. "Dari" and any target given by the caller SHALL keep their current defaults.

#### Scenario: Parent account first in the list
- **GIVEN** liquid accounts listed as "BCA" (default pocket "Utama"), its pockets "Utama" and "Harian", then "Jago"
- **WHEN** the owner opens Perpindahan
- **THEN** "Dari" SHALL be "BCA", "Ke" SHALL be "Harian", and saving with an amount SHALL send `POST /api/movements` that the server accepts, never a `same_account` refusal

#### Scenario: Parent without a default pocket
- **GIVEN** "BCA" has the pockets "Utama" and "Harian" and no default pocket, followed by "Jago"
- **WHEN** the owner opens Perpindahan with "Dari" "BCA"
- **THEN** "Ke" SHALL be "Jago"

#### Scenario: Only one account family
- **GIVEN** the only liquid accounts are "BCA" (default pocket "Utama") and its pockets "Utama" and "Harian"
- **WHEN** the owner opens Perpindahan
- **THEN** "Ke" SHALL be "Harian"

#### Scenario: A caller's target is kept
- **WHEN** "Bayar tagihan" opens Perpindahan for "Kartu Kredit Contoh"
- **THEN** "Ke" SHALL be "Kartu Kredit Contoh" as before

### Requirement: Transaction Titles
A transaction that is not a movement or top-up SHALL be titled by its note, else its merchant, else its category, else "Tanpa keterangan". A generic note a notification writes SHALL count as no note when the row has a merchant. When the note is the title and the row has a merchant that the note does not contain (ignoring case), the merchant SHALL lead the row's meta line. The edit dialog's subtitle SHALL follow the same order.

#### Scenario: Note and merchant
- **WHEN** a row has the note "Top up saldo untuk development" and the merchant "fal.ai"
- **THEN** its title SHALL be "Top up saldo untuk development" and its meta SHALL start with "fal.ai"

#### Scenario: Note naming the merchant
- **WHEN** the note is "Top up saldo fal.ai untuk development" and the merchant "fal.ai"
- **THEN** the title SHALL be the note and the meta SHALL NOT repeat "fal.ai"

#### Scenario: Merchant only
- **WHEN** a row has the merchant "Kedai Awan" and no note
- **THEN** its title SHALL be "Kedai Awan"

#### Scenario: Generic notification note
- **WHEN** a notification row has the note "Pengeluaran" and the merchant "Toko Contoh"
- **THEN** its title SHALL be "Toko Contoh"
