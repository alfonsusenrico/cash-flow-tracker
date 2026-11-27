## ADDED Requirements

### Requirement: Laporan Default Basis Matches Readiness
When `/laporan` is opened without a `basis` query parameter, the page SHALL open `cycle` when the owner has confirmed salary cycles (`cycle_available: true`, matching `GET /api/reports/monthly/latest`) and `month` otherwise. An explicit `basis=month` or `basis=cycle` query parameter SHALL always be respected.

#### Scenario: User with salary cycles opens Laporan from sidebar
- **GIVEN** an owner with confirmed salary cycles
- **WHEN** the owner opens `/laporan` with no query parameters
- **THEN** the page SHALL load the salary-cycle report and highlight the salary-cycle basis segment

#### Scenario: Explicit calendar month URL stays on month
- **GIVEN** an owner with confirmed salary cycles
- **WHEN** the owner opens `/laporan?basis=month`
- **THEN** the page SHALL load the calendar-month report

#### Scenario: User without salary cycles defaults to month
- **GIVEN** an owner with no confirmed salary cycles
- **WHEN** the owner opens `/laporan` with no query parameters
- **THEN** the page SHALL load the calendar-month report

### Requirement: Top Spending Places Cover Named Expenses Net of Linked Reimbursements
`merchant_spending` and `spending_merchants` SHALL identify each reportable expense by its cleaned `merchant`, falling back to its cleaned `name` when `merchant` is empty (skipping empty or generic placeholder labels), and SHALL subtract allocations from `reimbursement_links` for that expense row floored at zero. `search_text` SHALL strip invisible Unicode format characters and formatted numeric tokens without leaving orphaned punctuation.

#### Scenario: Expense with only name ranks in top places
- **GIVEN** a Rp 1.500.000 expense with `merchant: null` and `name: "Perbaikan sepeda"` and a Rp 400.000 expense with `merchant: "Toko Contoh"`
- **WHEN** the report is requested
- **THEN** `"Perbaikan sepeda"` SHALL rank first in `top_spending_merchants` with Rp 1.500.000

#### Scenario: Fully linked-reimbursed expense drops out of top places
- **GIVEN** a Rp 900.000 expense at `"Travel Contoh"` with a Rp 900.000 linked reimbursement in `reimbursement_links`
- **WHEN** the report is requested
- **THEN** `"Travel Contoh"` SHALL NOT appear in `top_spending_merchants` or as the top-merchant fact

#### Scenario: Merchant name with formatted digits produces working search text
- **GIVEN** an expense with `merchant: "Warung Contoh 125.000"`
- **WHEN** the report is requested
- **THEN** its `search_text` SHALL be `"Warung Contoh"` (with no trailing dot) and querying the ledger with that `q` SHALL return that transaction

## MODIFIED Requirements

### Requirement: Data quality box
The report SHALL show "Kualitas data" for its window with rows waiting in Tinjau, unclassified spending, possible duplicates (non-movement expense or income rows on the same account, amount and type within 3 minutes whose non-null categories and non-empty labels do not contradict each other), and complete local days without records (excluding a closed salary cycle's partial closing day), each linking to its fix.

#### Scenario: Possible duplicate
- **GIVEN** two Rp 25.000 expenses on the same account 2 minutes apart with the same label (or without labels) and a Rp 25.000 movement leg 1 minute later
- **WHEN** the report is requested
- **THEN** possible duplicates SHALL be 2, and `GET /api/transactions?possible_duplicates=true` for the window SHALL list exactly those two rows

#### Scenario: Distinct counterparties or items within 3 minutes are not duplicates
- **GIVEN** two Rp 75.000 incomes on the same account 1 minute apart named `"Transfer dari Andi"` and `"Transfer dari Budi"`, and two Rp 36.000 expenses 2 minutes apart in different categories
- **WHEN** the report is requested
- **THEN** possible duplicates SHALL be 0

#### Scenario: Days without records
- **WHEN** a closed month has transactions on every day except the 3rd and the 17th
- **THEN** days without records SHALL be 2 with those dates

#### Scenario: Closed salary cycle ignores partial closing morning
- **GIVEN** a salary cycle running from 25 September 04:00 to 24 October 03:00 WIB with transactions on every day from 25 September through 23 October and none between 00:00 and 03:00 on 24 October
- **WHEN** the cycle report is requested
- **THEN** days without records SHALL be 0 and 24 October SHALL NOT be listed

### Requirement: Report page layout
`/laporan` SHALL show, without verdict words, the basis segment and stepper in its title block with `basis` and `start` in the URL, then one hero "Pengeluaran {period}", the four questions in a reading column of at most 880 px and a context panel with "Kualitas data", "Selisih" and "Laporan sebelumnya", beside it from 1200 px and after it below. It SHALL have no KPI cards or prose lead, and states SHALL be truthful.

#### Scenario: Phone width
- **WHEN** the page is shown at 390 and at 320 CSS pixels
- **THEN** no content SHALL overflow horizontally, the panel SHALL follow the questions, and every action SHALL be keyboard operable with a visible focus

#### Scenario: New user
- **WHEN** a user without any transaction opens `/laporan`
- **THEN** the page SHALL say that the first report appears after the first recorded month, without zero figures presented as results

#### Scenario: Stepping to the previous report
- **GIVEN** the page was opened at `/laporan?basis=month&start=2026-10-01`
- **WHEN** the owner presses the stepper's previous button
- **THEN** the September report SHALL be shown and the URL SHALL become `/laporan?basis=month&start=2026-09-01`

#### Scenario: Past reports
- **WHEN** the report page is shown on a desktop with a running November, a full October, and a September where tracking started on 15 September
- **THEN** "Laporan sebelumnya" SHALL label November "Berjalan", October "Lengkap", and September "Belum lengkap" ("Partial" in English), each a whole-row link to its report with `aria-current="page"` on the shown one

#### Scenario: Hero against both comparisons
- **GIVEN** October with Pengeluaran Rp 5.993.830, a whole September of Rp 5.240.000 and an average of Rp 4.943.333
- **WHEN** the October report is shown
- **THEN** the hero SHALL read "Pengeluaran Oktober 2026" with "▲ Rp 753.830 vs September · ▲ Rp 1.050.497 vs rata-rata 3 bulan", and the page SHALL have exactly one 44 px figure

#### Scenario: Whole-row targets and expense colour
- **WHEN** the owner activates a merchant row, a pillar row of the table or an income source
- **THEN** the ledger SHALL open on that report's window with the row's filter, and merchant amounts SHALL read "−Rp …" in the expense text colour

#### Scenario: Hidden balances
- **WHEN** "Sembunyikan saldo" is on
- **THEN** every amount, change, bar label and chart description SHALL read as hidden, and only percentages and counts SHALL stay
