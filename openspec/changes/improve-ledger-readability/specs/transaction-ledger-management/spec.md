## MODIFIED Requirements

### Requirement: Searchable & Filterable Ledger View
The application SHALL provide a `/ledger` screen in the interface language that lists recorded cash movements grouped by local day, newest first. From 1200 px a sticky type and period rail sits beside the list; from 768 to 1199 px a type control sits above it. A search field, "Filter" and the active filters as removable chips sit above the rows, and the period figures match the active filters across all matching rows.

#### Scenario: Required behavior and constraints
- **WHEN** an owner opens or filters the ledger
- **THEN** the following required behavior and constraints hold:

  The application SHALL provide a dedicated `/ledger` screen with real-time filtering:
  1. Full-text search by notes, merchant, or category name, in a field labelled "Cari catatan, merchant, kategori".
  2. The types "Semua", "Pengeluaran", "Masuk", "Pindah saldo", "Tabungan & investasi" and "Kredit", each with the count of rows matching the other filters: in the rail's JENIS group from 1200 px, and in one horizontal type control below 1200 px.
  3. "Filter" opens the "Filter transaksi" dialog and shows the number of active filters. Every active filter, including the search text, is shown as a removable chip below the toolbar only while set. The page's "…" holds "Gabungkan dua transaksi" and "Filter transaksi".
  4. Cash flow figures for the filtered rows: the payday cycle (or labelled calendar month) and cumulative totals, or the selected period's figures, shown as the hero the chosen type calls for ("Transaksi Hero Figures"), matching active filters and independent of how many rows are loaded.
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

### Requirement: Consolidated Internal Movement Ledger Representation
The application SHALL persist and present each bilateral internal movement as one logical operation backed by two durably linked ledger records, listed as one row that names its route and is shown as a transfer by its words and colour.

#### Scenario: Required behavior and constraints
- **WHEN** an internal movement is persisted, displayed, edited or deleted
- **THEN** the following required behavior and constraints hold:

  The application SHALL persist and present each bilateral internal movement as one logical operation backed by two durably linked ledger records:
  1. Both records SHALL share an immutable movement identifier assigned by the server.
  2. In the default "Semua Rekening" view, durably linked records SHALL render as one consolidated row regardless of their timestamp difference.
  3. The row SHALL be titled by the movement's note, as written by the owner or stored by the server, with its route `Sumber → Tujuan` in the direction the money moved as the meta; without a note, or with the server's placeholder note, the route SHALL be the title. The category column SHALL read "Pindah saldo", the pillar column SHALL show the transfer dot with its saving direction ("Ditabung", "Diambil dari tabungan", "Investasi", "Bayar utang") or "Pindah saldo", never "—" or "Tidak dihitung", and the amount SHALL be in the transfer colour without a `+` or `−` sign, reflecting zero net cash flow change. A pocket SHALL be named with its parent account, as `Jago · Main Pocket`; an account without a parent SHALL be named alone.
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
- **THEN** `type_counts` SHALL count the expense, income, movement, investment, saving and credit rows of the other filters, a top-up leg SHALL count under investment and under saving, a credit purchase SHALL count under expense and credit, and no other key SHALL change

#### Scenario: Only the owner's rows
- **WHEN** another owner has rows on the same days
- **THEN** none of them SHALL appear in this owner's `daily_totals` or `type_counts`

#### Scenario: Unknown include value
- **WHEN** `include=foo` is sent
- **THEN** the response SHALL be 422

### Requirement: Ledger Type and Waiting Filters
`GET /api/transactions` SHALL accept `type=movement` (internal movements and excluded categories outside top-ups), `type=investment` (top-up legs and Investasi rows), `type=saving` (rows whose `saving_flow` changes "Ditabung & investasi", and top-up legs and Investasi rows), `type=credit` (rows on a credit account and cash legs of movements with one) and `waiting=true` (Tinjau's attention set). With `type=saving` and no other row filter, the summary SHALL carry `saving_net`. `type=transfer` SHALL keep its meaning.

#### Scenario: Waiting filter matches the badge
- **WHEN** the ledger is requested with `waiting=true` and no other filter
- **THEN** `total` SHALL equal the badge's `attention` count

#### Scenario: Investment tab
- **WHEN** the API is asked for `type=investment` after an RDN Contoh debit was converted into a top-up of Bibit Contoh · Reksa Dana A
- **THEN** both legs of that top-up SHALL be listed and `type=movement` SHALL not list them

#### Scenario: Top-ups under Tabungan & investasi
- **WHEN** the owner selects "Tabungan & investasi" after that conversion, and after a top-up funded from Jago · Tabungan
- **THEN** both top-ups SHALL be listed, the second reading "Pindah saldo", and `saving_net` SHALL be the same as without them

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

### Requirement: Ledger Table Columns
Each desktop ledger row SHALL have the columns time, title with its second line ("Ledger Row Title Lines"), category, pillar (136 px) and amount, in that order, without a header row, a type column or a type pill. The category SHALL show the row's category name as the owner wrote it, and the pillar a dot and a word on every row: an expense's pillar, "Uang masuk", a move's saving direction or "Pindah saldo", "Investasi", or "Menunggu jawaban". The row SHALL NOT show provenance; the transaction dialog SHALL keep showing it.

#### Scenario: Expense shows its category
- **GIVEN** an expense "Indomaret" in the category "Belanja Harian", whose pillar is Kebutuhan
- **WHEN** the owner opens Transaksi on a desktop
- **THEN** its row SHALL show "Belanja Harian" in the category column and the Kebutuhan dot with "Kebutuhan" in the pillar column, and no "Pengguna · Diketik di ponsel" text SHALL appear in the row

#### Scenario: Movement has no category
- **WHEN** the list shows a movement "Jago · Makan-Jalan → Gopay"
- **THEN** the category column SHALL name the kind as "Pindah saldo" and the pillar column SHALL show the transfer dot with its saving direction or "Pindah saldo", never "—", "Internal Movement" or "Tidak dihitung"

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

#### Scenario: Every row has a dot
- **WHEN** the list shows an expense in Budaya, an income, an ordinary move, a top-up and a waiting row
- **THEN** each pillar cell SHALL start with a dot: the Budaya colour, the income colour with "Uang masuk", the transfer colour with "Pindah saldo", the investment colour with "Investasi", and the warning colour with "Menunggu jawaban", and the dot SHALL be hidden from assistive technology

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
- **THEN** the movement SHALL carry `null` and its pillar column SHALL read "Pindah saldo", it SHALL NOT be listed under "Tabungan & investasi", and Beranda's figure SHALL not change

#### Scenario: Move inside savings and investments
- **WHEN** the owner tops up Bibit Contoh · Reksa Dana A from Jago · Tabungan
- **THEN** the top-up SHALL carry `null` and read "Pindah saldo", it SHALL be listed under "Tabungan & investasi" as an investment row, and Beranda's figure SHALL not change

#### Scenario: Top-up into a product
- **WHEN** an RDN Contoh debit is converted into a top-up of Bibit Contoh · Reksa Dana A
- **THEN** the top-up SHALL carry `invested` and read "Investasi"

#### Scenario: Paying a credit account
- **WHEN** the owner moves money from a harian account into SPayLater, and from a tabungan pocket into SPayLater
- **THEN** the first SHALL carry `debt_payment` and read "Bayar utang" without being listed under "Tabungan & investasi", and the second SHALL carry `taken`, read "Diambil dari tabungan" and be listed

#### Scenario: Ordinary move
- **WHEN** the owner moves money between two harian pockets
- **THEN** the movement SHALL carry `null` and its pillar column SHALL read "Pindah saldo"

#### Scenario: Aggregate unchanged
- **WHEN** the backend suite runs on the parity seed
- **THEN** the sum of `saved` and `invested` movement amounts SHALL equal `saving_into`, the sum of `taken` amounts SHALL equal `saving_out`, and every existing Kakeibo and cycle-summary figure SHALL be unchanged

### Requirement: Transaksi Hero Figures
Transaksi SHALL show one hero "Pengeluaran" with the summary's `spending` in 44 px, and beside it "Masuk" (`inflow`, signed "+", green) and "Bersih · masuk − pengeluaran" (`spending_net`, signed). Without a period the label SHALL add "semua waktu". One muted line SHALL name a reimbursement offset or saved part. For "Tabungan & investasi" the hero SHALL be "Ditabung & investasi" with `saving_net`; "Masuk" and "Pindah saldo" follow "Hero for Masuk and Pindah Saldo". Values SHALL be the server's.

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
From 1200 px Transaksi SHALL show a rail labelled "Jenis dan periode" with JENIS (Semua, Pengeluaran, Masuk, Pindah saldo, Tabungan & investasi, Kredit, with counts) and PERIODE (Siklus gaji, Bulan kalender, Semua waktu). The chosen items SHALL be marked by `aria-pressed`, a fill and the straight selection bar. Below 1200 px the types SHALL be a horizontal control and the period mode SHALL be in Filter.

#### Scenario: Choosing a type
- **WHEN** the owner presses "Pengeluaran" in the rail
- **THEN** the URL SHALL gain `type=expense` without a navigation, the list SHALL show only spending rows, and focus SHALL stay on the pressed item

#### Scenario: Choosing a period mode
- **WHEN** the owner presses "Bulan kalender" in the rail
- **THEN** the URL SHALL read `period=month` with the offset reset, the stepper SHALL name the month, and Analisis opened next SHALL show the same period

#### Scenario: Counts while loading
- **WHEN** the first page is still loading
- **THEN** the rail and the type control SHALL show no counts and no invented numbers

#### Scenario: An old Investasi link
- **WHEN** the owner opens `/ledger?type=investment`
- **THEN** "Tabungan & investasi" SHALL be the chosen type, the URL SHALL read `type=saving`, and no "Investasi" type SHALL be offered

## ADDED Requirements

### Requirement: Ledger Row Title Lines
A transaction row SHALL give its title the whole first line of the title column. When the row has a meta, a note or tags, they SHALL be shown on a muted second line under the title, left-aligned with it, in that order, separated by " · ", the note and the tags each marked by their icon. A row without them SHALL stay one line. On the phone the existing two-line composition SHALL remain.

#### Scenario: A long name keeps its line
- **GIVEN** an income "Ganti uang transport dari Rani untuk fellowship" on BCA with the note "fellowship coach" and the tag "fellowship coach"
- **WHEN** the owner opens Transaksi at 1280 px
- **THEN** the first line SHALL show the whole name, and the second line SHALL read "BCA · fellowship coach · fellowship coach" with the note icon before the note and the tag icon before the tag

#### Scenario: Extras line up
- **WHEN** two rows with different name lengths both have tags
- **THEN** both tag lists SHALL start at the same horizontal position, the left edge of the title

#### Scenario: A row without extras
- **WHEN** a row has no meta, note or tags
- **THEN** it SHALL be one line, as tall as before

#### Scenario: Narrow desktop
- **WHEN** Transaksi is shown at 1024 px with a long name, a long note and three tags
- **THEN** each line SHALL end with an ellipsis where it does not fit, the tags SHALL stay readable before the note does, and the page SHALL not scroll horizontally

### Requirement: Hero for Masuk and Pindah Saldo
For "Masuk" Transaksi SHALL show one hero "Masuk" with the summary's `inflow`, without "Pengeluaran" or "Bersih". For "Pindah saldo" it SHALL show one hero "Pindah saldo" with the summary's `moved`. The ledger summary SHALL carry `moved`: the sum of the matching movements in the window, each counted once, by the rule of the day totals' `movements`.

#### Scenario: Money in
- **GIVEN** a cycle with `inflow` Rp 7.837.737
- **WHEN** the owner chooses "Masuk"
- **THEN** the hero SHALL read "Masuk" "+Rp 7.837.737", and no "Pengeluaran Rp 0" SHALL be shown

#### Scenario: Moves
- **GIVEN** a cycle whose movements are Rp 247.856 and Rp 648.026
- **WHEN** the owner chooses "Pindah saldo"
- **THEN** the hero SHALL read "Pindah saldo" "Rp 895.882", equal to the sum of the day totals' `movements` over every page

#### Scenario: Older API
- **WHEN** the summary carries no `moved`
- **THEN** the "Pindah saldo" hero SHALL read "Belum tersedia" in words
