# monthly-report Specification

## Purpose
Give every user one monthly report (Laporan Bulanan) per calendar month or salary cycle that answers the four Kakeibo month-end questions from the shared ledger figures, compares fairly, shows data quality, keeps a reflection note, and tells the phone when a report is ready.

## Requirements

### Requirement: Monthly report periods
The system SHALL offer one monthly report ("Laporan Bulanan") per period at `GET /api/reports/monthly`, identified by its basis and local `start_date`. The basis SHALL be the calendar month (`basis=month`, the default) or, for a user with confirmed salary cycles, the closed salary cycle (`basis=cycle`). Without `start_date` the report SHALL be the latest closed period, else the period in progress.

#### Scenario: Default month report
- **GIVEN** transactions recorded since 15 September 2026, viewed on 2 November 2026
- **WHEN** the report is requested without a period
- **THEN** it SHALL be "Oktober 2026" from 1 to 31 October in the application timezone

#### Scenario: Salary cycle basis
- **GIVEN** confirmed salary receipts on 25 August, 25 September and 24 October
- **WHEN** the report is requested with `basis=cycle`
- **THEN** it SHALL cover the closed cycle from the 25 September receipt to the instant before the 24 October receipt, with local dates 25 September to 24 October, and the list SHALL contain only salary cycles

#### Scenario: No salary cycle
- **WHEN** a user without confirmed salary receipts requests `basis=cycle`
- **THEN** the response SHALL be 409 `cycle_unavailable` and the periods list SHALL say `cycle_available: false`

#### Scenario: Unknown period
- **WHEN** the report is requested with a `start_date` that is not a listed period start
- **THEN** the response SHALL be 404 `period_not_found`

### Requirement: Past report list
`GET /api/reports/monthly/periods` SHALL list, newest first, the period in progress marked `complete: false` and up to 24 closed periods of the basis, back to the period in which the first transaction was recorded. That earliest period SHALL carry `tracking_started_inside: true` and the first recorded date when tracking began after its first day.

#### Scenario: List with partial first month
- **GIVEN** transactions recorded since 15 September 2026, viewed on 2 November 2026
- **WHEN** the month list is requested
- **THEN** it SHALL contain November 2026 (in progress), October 2026 and September 2026 with `tracking_started_inside: true` and first recorded date 15 September

### Requirement: Four Kakeibo questions in a fixed order
The page SHALL answer four numbered questions in this order: 1 "Berapa uang masuk?" (Masuk and its sources by category), 2 "Berapa yang ditabung?" (Ditabung & investasi: saving-classified expenses and net savings movements, separate from the surplus), 3 "Berapa pengeluarannya?" (Pengeluaran and its breakdowns), 4 "Apa yang mau diperbaiki bulan depan?" (facts and the note). Every figure SHALL come from the existing reporting services.

#### Scenario: Savings separate from surplus
- **GIVEN** a month with Rp 5.000.000 inflow, Rp 3.000.000 outflow, a Rp 500.000 movement into a goal-linked pocket and a Rp 100.000 saving-classified expense
- **WHEN** the report is requested
- **THEN** fresh savings SHALL be Rp 600.000 with both components listed under question 2, the API surplus SHALL be Rp 2.000.000, and the page's Selisih SHALL be Masuk minus Pengeluaran, Rp 2.100.000, without calling it money set aside

#### Scenario: No reflection note
- **WHEN** neither the period nor the previous period has a reflection note
- **THEN** question 4 SHALL show its facts and a "Tulis catatan untuk {period}" button, and no empty form and no line about last period's note

#### Scenario: Writing the first note
- **GIVEN** the October report without a note
- **WHEN** the owner selects "Tulis catatan untuk Oktober 2026", types "Kurangi jajan kopi" and saves
- **THEN** the note SHALL be stored with `PUT /api/reports/monthly/reflection`, the form SHALL close, the note SHALL be shown read-only with "Ubah", and focus SHALL move to "Ubah"

#### Scenario: Numbered questions
- **WHEN** the October report is shown at 1440 and at 390 px
- **THEN** the four questions SHALL be numbered 1 to 4 in that order, each heading SHALL be a level-2 heading, and each question SHALL be 48 px below the previous one with a rule between them (32 px on the phone)

### Requirement: Spending answer
The spending answer SHALL give net outflow, spending per Kakeibo pillar and "Belum diklasifikasi", the top five categories and merchants, the "Tak Terduga" items and the recurring charges detected in the period. The page SHALL present Pengeluaran without the saving pillar, whose expenses belong to Ditabung & investasi.

#### Scenario: Unexpected items
- **WHEN** the month has three expenses with the effective pillar `unexpected`
- **THEN** the report SHALL list them with date, amount and description, and their total SHALL equal the `unexpected` pillar amount

#### Scenario: Recurring charges in the period
- **WHEN** a monthly charge was last seen inside the period
- **THEN** it SHALL be listed with its merchant, cadence and typical amount, and charges last seen before the period SHALL NOT be listed

### Requirement: Report equals the ledger
The report's inflow, gross outflow, reimbursement offset, outflow and net, each pillar amount, each category amount and each income source SHALL equal the summary of `GET /api/transactions` for the same `from_date`/`to_date` window with the matching filters. No report figure SHALL use a calculation rule the ledger summary does not use.

#### Scenario: Reconciliation
- **GIVEN** a month with income, expenses in several pillars and categories, an unclassified expense, an uncategorised expense, an internal movement with a fee, an investment trade and a reimbursement
- **WHEN** the report and the ledger summaries are requested for the same window
- **THEN** every listed amount SHALL be equal, the pillar amounts SHALL add up to the outflow and the category amounts SHALL add up to the outflow

### Requirement: Comparison with the previous period
A completed period SHALL be compared with the whole previous period of the same basis, never over the same elapsed days. The comparison SHALL mark a previous period in which tracking began after its first day as partial and give no verdict for it. No percentage change SHALL be shown.

#### Scenario: Partial previous month
- **WHEN** the October report is compared with a September in which tracking began on the 15th
- **THEN** the previous period SHALL carry `partial: true`, the interface SHALL say that September was recorded only from 15 September, and "belum lengkap" SHALL replace the ▲ or ▼ of every change against September

#### Scenario: Whole months, not the same days
- **GIVEN** September spending of Rp 100.000 on the 5th and Rp 900.000 on the 20th, and October spending of Rp 1.200.000
- **WHEN** the October report is requested on 2 November
- **THEN** the previous Pengeluaran SHALL be Rp 1.000.000 for the whole of September and the change SHALL be Rp 200.000 higher

### Requirement: Data quality box
The report SHALL show "Kualitas data" for its window with the number of rows waiting in Tinjau, the count and amount of unclassified spending, the number of possible duplicates (expense or income rows with another row of the same account, amount and type within 3 minutes, excluding movement legs and movement fees) and the days without any recorded transaction, each linking to its fix: Tinjau, the ledger filtered to the exact rows or "Lengkapi klasifikasi", and Catat.

#### Scenario: Possible duplicate
- **GIVEN** two Rp 25.000 expenses on the same account 2 minutes apart and a Rp 25.000 movement leg on that account 1 minute later
- **WHEN** the report is requested
- **THEN** possible duplicates SHALL be 2, and `GET /api/transactions?possible_duplicates=true` for the window SHALL list exactly those two rows

#### Scenario: Days without records
- **WHEN** a closed month has transactions on every day except the 3rd and the 17th
- **THEN** days without records SHALL be 2 with those dates

### Requirement: Drill-down from every report figure
Every amount in the report that is a sum of ledger rows SHALL link to the ledger filtered to its exact window (`from`, `to`) and rows: `type`, `category` (or `uncategorized`), `kakeibo` (including `unclassified`), `q` for a merchant or recurring charge, `review` or `dup=1`. A figure that is not a sum of rows in one window, the essential cost and the net savings movements, SHALL say where it is explained instead of linking to rows that do not add up to it.

#### Scenario: Pillar link
- **WHEN** the owner selects the Rp 300.000 "Belum diklasifikasi" amount of the October report
- **THEN** the ledger SHALL open with `from` and `to` of October, `type=expense` and `kakeibo=unclassified` and its summary outflow SHALL be Rp 300.000

### Requirement: Reflection note per period
The owner SHALL be able to save one reflection note of 1 to 1000 characters per basis and period ("Apa yang mau diperbaiki bulan depan?") with `PUT /api/reports/monthly/reflection` and remove it with `DELETE`. Notes SHALL be stored in `monthly_reflections` (Flyway V41), scoped to their owner, and returned only to that owner. The report SHALL return the period's note and the previous period's note, which the interface SHALL show again as last period's intention.

#### Scenario: Shown again next month
- **GIVEN** the owner saved "Kurangi jajan kopi" on the October report
- **WHEN** the owner opens the November report
- **THEN** it SHALL show "Kurangi jajan kopi" as the note from October

#### Scenario: Owner scope
- **WHEN** another owner saves, reads or deletes a note for the same basis and period
- **THEN** each owner SHALL see only their own note and a delete SHALL never remove another owner's note

#### Scenario: Invalid note
- **WHEN** the note is blank or longer than 1000 characters, or the period is not listed
- **THEN** the response SHALL be 422 for the note and 404 `period_not_found` for the period, and nothing SHALL be stored

### Requirement: Monthly readiness API
`GET /api/reports/monthly/latest`, with session or Bearer authentication, SHALL return for the owner's basis (`cycle` when available, else `month`, or the requested `basis`) the latest closed period with its label, start, end and local dates, `ready: true`, the pending Tinjau count (`pending_review`, as the Tinjau badge), the `auto` count (`unreviewed`) and the web path of that report. Without a closed period it SHALL return `ready: false` and `period: null`.

#### Scenario: Ready after the month closes
- **GIVEN** activity in October and three rows needing review
- **WHEN** the phone calls the API on 2 November with its Bearer key
- **THEN** it SHALL return `ready: true`, the "Oktober 2026" window, `pending_review: 3` and `url: "/laporan?basis=month&start=2026-10-01"`, equal to the session response

#### Scenario: First month
- **WHEN** a new user's only activity is in the current month
- **THEN** the API SHALL return `ready: false` with `period: null`

#### Scenario: Unknown key
- **WHEN** the API is called with an unknown Bearer key
- **THEN** it SHALL answer 401

### Requirement: Optional budget in the report
The report SHALL return `budget: null` and show no budget figure when the owner has no category budget and no overall monthly spending budget, and SHALL then offer "Atur anggaran", a per-category monthly budget editor that saves `monthly_budget`. With a budget it SHALL show each budgeted category's budget, net spending in the period and remaining amount, and the overall budget against the outflow.

#### Scenario: No budget
- **WHEN** no category has a budget
- **THEN** the report SHALL contain no budget figure and SHALL offer "Atur anggaran"

#### Scenario: Budget set
- **GIVEN** a Rp 500.000 budget on "Jajan" and Rp 620.000 net spent in it in October
- **WHEN** the October report is requested
- **THEN** it SHALL show budget Rp 500.000, spent Rp 620.000 and Rp 120.000 over, in text

### Requirement: Report page layout
`/laporan` SHALL show, without verdict words, the basis segment and stepper in its title block with `basis` and `start` in the URL, then one hero "Pengeluaran {period}", the four questions in a reading column of at most 880 px and a context panel with "Kualitas data" in one line, "Selisih" and "Laporan sebelumnya", beside it from 1200 px and after it below. It SHALL have no KPI cards or prose lead, and states SHALL be truthful.

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
- **WHEN** the report page is shown on a desktop
- **THEN** the panel's "Laporan sebelumnya" SHALL list the periods of the basis with "Berjalan" or "Lengkap" and " · ada catatan", each a whole-row link to its report, the shown one marked with the straight selection bar and `aria-current="page"`

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

### Requirement: Three-Period Average
A completed period SHALL also be compared with the average of the three most recent completed, non-partial periods before it on the same basis: Oktober with Juli to September. The period SHALL never be part of its own average. Averages SHALL be whole rupiah rounded half up, with two usable periods labelled as a two-period average, and with fewer none.

#### Scenario: Oktober against Juli to September
- **GIVEN** Pengeluaran of Rp 3.810.000 in Juli, Rp 3.710.000 in Agustus, Rp 3.218.500 in September and Rp 4.802.000 in Oktober
- **WHEN** the October comparison is requested
- **THEN** the baseline SHALL be Rp 3.579.500 over Juli to September, and Oktober SHALL be Rp 1.222.500 above it

#### Scenario: Two usable periods
- **WHEN** only two completed non-partial periods precede the report period
- **THEN** the baseline SHALL be their average with `period_count: 2`, labelled "rata-rata 2 bulan"

#### Scenario: Not enough history
- **WHEN** fewer than two completed non-partial periods precede the report period
- **THEN** there SHALL be no baseline, the page SHALL say "Rata-rata muncul setelah 2 bulan lengkap", and no Rp 0 baseline SHALL be shown

### Requirement: Running Period Progress
For the period in progress the report SHALL show Pengeluaran so far and its share of the previous whole period's Pengeluaran, in total and per pillar, as a bar and "{n}% dari total {period}", and in total its share of the 3-period average when one exists. It SHALL show no change, no ▲ or ▼, and no words judging the period until it is complete.

#### Scenario: November in progress
- **GIVEN** Rp 1.450.000 of Pengeluaran in November so far, Rp 4.802.000 in October and a 3-month average of Rp 3.910.167
- **WHEN** the November report is shown on 4 November
- **THEN** it SHALL read "30% dari total Oktober 2026 · 37% dari rata-rata 3 bulan" with a bar and "Periode berjalan · dibandingkan setelah selesai", and no figure SHALL carry "naik", "turun" or an arrow

#### Scenario: No previous whole period
- **WHEN** the running period has no completed previous period
- **THEN** only Pengeluaran so far SHALL be shown, without a share

### Requirement: Spending by Pillar and Category
The report SHALL list Pengeluaran grouped by Kakeibo pillar and "Belum diklasifikasi": each pillar with its total, share of Pengeluaran and change against the previous whole period, then its categories sorted by amount with their share and change. Every amount SHALL link to the ledger for its window and filter.

#### Scenario: Sorted categories
- **WHEN** the October report shows Kebutuhan with Belanja Harian Rp 1.452.500, Transportasi Rp 779.000 and Tagihan & Utilitas Rp 580.000
- **THEN** they SHALL be listed in that order under the Kebutuhan row, each with its share of Pengeluaran and its change against September

#### Scenario: Category new this month
- **WHEN** a category had no spending in September
- **THEN** its previous amount SHALL be Rp 0 and its change SHALL equal its October amount

### Requirement: Improvement Facts
The report SHALL show "Yang bisa diperbaiki": at most seven deterministic facts, each one line with a figure and a ledger link. These are categories at least Rp 50.000 and 10% above their own average, the Keinginan share against the previous period, the top merchant, recurring bills and unexpected spending. A running period SHALL get only the last three, titled "Sejauh ini". No fact SHALL be written by a model.

#### Scenario: Category above its average
- **GIVEN** Belanja Harian Rp 1.452.500 in Oktober and a three-month average of Rp 1.133.333
- **WHEN** the October report is shown
- **THEN** a fact SHALL read "Belanja Harian di atas rata-rata 3 bulan" with "+Rp 319.167" and link to that category's October transactions

#### Scenario: Threshold edge
- **WHEN** a category is Rp 49.999 above its average
- **THEN** it SHALL NOT be listed as a fact

#### Scenario: Running month facts
- **WHEN** the November report is shown while November is in progress
- **THEN** "Sejauh ini" SHALL list only the top merchant, the recurring bills seen and the unexpected spending, without any comparison

### Requirement: Monthly Spending History
The report SHALL show the Pengeluaran of the last four completed periods and the running one as one series of bars proportional to their amounts, each with its amount as text, the viewed period highlighted, the running one hatched and marked "berjalan", a legend naming both, and the same values as a table, and SHALL say that a running period is not compared until it ends.

#### Scenario: Five bars
- **WHEN** the October report is shown in November with data since Juli
- **THEN** the chart SHALL show Juli, Agustus, September, Oktober (highlighted) and November marked "berjalan", each with its amount as text

#### Scenario: Proportional heights
- **WHEN** the bars are Rp 4.980.000, Rp 5.240.000, Rp 5.993.830 and Rp 2.463.990
- **THEN** their heights SHALL be 93, 98, 112 and 46 px within 1 px

#### Scenario: Table alternative
- **WHEN** the owner selects "Lihat sebagai tabel"
- **THEN** a table SHALL list each period with its Pengeluaran, the viewed one marked "dilihat" and the running one "berjalan"

### Requirement: Whole-Period Comparison API
`GET /api/reports/monthly/comparison` SHALL return, for the owner and the requested basis and period, the totals, the previous period, the baseline, running progress, pillars with categories, facts and history. Running progress SHALL carry `baseline_share`, Pengeluaran so far as a whole percent of the baseline's, rounded half up, or null. `GET /api/reports/monthly` SHALL return the same object as `comparison`. Every amount SHALL equal the ledger summary of its window and filters.

#### Scenario: Equal to the ledger
- **WHEN** the comparison for Oktober is requested
- **THEN** each history period's Pengeluaran and each pillar and category amount SHALL equal `GET /api/transactions` summaries for the same window and filters

#### Scenario: Another owner
- **WHEN** another owner has spending in the same months
- **THEN** none of it SHALL appear in any figure, fact or history bar

#### Scenario: Unauthenticated or unknown period
- **WHEN** the request has no valid session or Bearer key, or names a period that is not listed
- **THEN** the response SHALL be 401, or 404 `period_not_found`, with no figures

#### Scenario: Running share of the average
- **GIVEN** Rp 1.450.000 of Pengeluaran in November so far and a baseline of Rp 3.910.167 over Agustus to Oktober
- **WHEN** the November comparison is requested on 4 November
- **THEN** `progress.baseline_share` SHALL be 37, `progress.share` and every other field SHALL keep their values, and no change against the baseline SHALL be given

#### Scenario: No baseline share
- **WHEN** the running period has fewer than two whole, non-partial periods before it, or a completed period is requested
- **THEN** `progress.baseline_share` SHALL be null, or `progress` SHALL be null as before

### Requirement: Laporan in Both Languages
Laporan SHALL read in English and Indonesian from one catalogue per language, with period labels and facts from the server's codes and dates and amounts formatted by the account region, and SHALL show no untranslated-page notice. Every figure SHALL be the same string in both languages for the same data, and "Pengeluaran" / "Spending" SHALL be the only label of the spending figure.

#### Scenario: English Laporan
- **WHEN** an English account in region Indonesia opens the October report
- **THEN** it SHALL read "Report", "Spending for October 2026", "How much came in?" and "Data quality", amounts SHALL read "Rp 5.993.830", and no region SHALL carry `lang="id"`

#### Scenario: Figures in both languages
- **GIVEN** an English and an Indonesian account with the same data
- **WHEN** both open the same report
- **THEN** every amount, count, percent and date number SHALL be the same string in both

### Requirement: One Pengeluaran on Laporan
Every "Pengeluaran" figure on Laporan SHALL equal the comparison's `totals.spending` for its period, as on Beranda, Transaksi and Analisis, so expenses with the Tabungan pillar never count as spending there. The report SHALL return read-only Pengeluaran-basis fields beside its Belanja-basis fields, which keep their values, and Laporan SHALL show them for its top places, budgets, reimbursement caption and top-merchant fact.

#### Scenario: Pengeluaran-basis fields
- **WHEN** a report is requested
- **THEN** it SHALL contain `spending.top_spending_merchants`, `budget.categories[].spending` and `spending_remaining`, `budget.total_spending`, `budget.overall.spending` and `spending_remaining`, `flow.spending_reimbursement_offset` and `comparison.spending_top_merchant`, each from the Pengeluaran rule of `services/spending_basis.py`

#### Scenario: Same Pengeluaran as Analisis
- **GIVEN** a closed month with Rp 635.000 of Pengeluaran after a Rp 60.000 reimbursement and Rp 1.240.000 of expenses with the Tabungan pillar
- **WHEN** Laporan and Analisis show that month
- **THEN** both SHALL read Pengeluaran Rp 635.000, and the sums of Analisis's daily, category, calendar and flow figures SHALL equal it

#### Scenario: Top places without saving
- **GIVEN** the largest merchant of the month holds only expenses with the Tabungan pillar
- **WHEN** the report is shown
- **THEN** "Tempat belanja terbesar" and the top-merchant fact SHALL name the largest merchant of Pengeluaran instead, and `spending.top_merchants` SHALL keep its value

#### Scenario: Budget on Pengeluaran
- **GIVEN** a Rp 1.000.000 budget on Belanja Dapur with Rp 450.000 of Pengeluaran and a Rp 900.000 row with the Tabungan pillar in it
- **WHEN** the report is shown
- **THEN** the budget row SHALL read "Rp 450.000 dari Rp 1.000.000 · sisa Rp 550.000", while `spent` stays Rp 1.350.000
