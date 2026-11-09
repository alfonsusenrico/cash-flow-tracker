## MODIFIED Requirements

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

### Requirement: Running Period Progress
For the period in progress the report SHALL show Pengeluaran so far and its share of the previous whole period's Pengeluaran, in total and per pillar, as a bar and "{n}% dari total {period}", and in total its share of the 3-period average when one exists. It SHALL show no change, no ▲ or ▼, and no words judging the period until it is complete.

#### Scenario: November in progress
- **GIVEN** Rp 1.450.000 of Pengeluaran in November so far, Rp 4.802.000 in October and a 3-month average of Rp 3.910.167
- **WHEN** the November report is shown on 4 November
- **THEN** it SHALL read "30% dari total Oktober 2026 · 37% dari rata-rata 3 bulan" with a bar and "Periode berjalan · dibandingkan setelah selesai", and no figure SHALL carry "naik", "turun" or an arrow

#### Scenario: No previous whole period
- **WHEN** the running period has no completed previous period
- **THEN** only Pengeluaran so far SHALL be shown, without a share

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

## ADDED Requirements

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
