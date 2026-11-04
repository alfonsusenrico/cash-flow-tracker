## ADDED Requirements

### Requirement: Ledger Spending Figure
The ledger summary of `GET /api/transactions` SHALL add `spending`, the outflow net of applied reimbursements without expenses whose effective pillar is saving, and `saving`, those saving-pillar expenses. Existing fields SHALL keep their names and values.

#### Scenario: Saving-classified expense
- **GIVEN** a period with Rp 1.000.000 of expenses, of which Rp 200.000 is in a category whose pillar is saving, and no reimbursement
- **WHEN** the ledger summary is requested
- **THEN** `outflow` SHALL be Rp 1.000.000, `spending` Rp 800.000 and `saving` Rp 200.000

#### Scenario: Equal to Beranda
- **WHEN** the ledger is filtered to the open salary cycle without other filters
- **THEN** its `spending` SHALL equal the cycle summary's `spending`

## MODIFIED Requirements

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
