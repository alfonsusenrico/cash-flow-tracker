## ADDED Requirements

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
For the period in progress the report SHALL show Pengeluaran so far and its share of the previous whole period's Pengeluaran, in total and per pillar, as a bar and "{n}% dari total {period}". It SHALL show no change, no ▲ or ▼, and no words judging the period until it is complete.

#### Scenario: November in progress
- **GIVEN** Rp 1.450.000 of Pengeluaran in November so far and Rp 4.802.000 in October
- **WHEN** the November report is shown on 4 November
- **THEN** it SHALL read "30% dari total Oktober 2026" with a bar, and no figure SHALL carry "naik", "turun" or an arrow

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
The report SHALL show the Pengeluaran of the last four completed periods and the running one as stacked pillar bars with their amounts as text, the viewed period highlighted and the running one marked "berjalan", and SHALL say that a running period is not compared until it ends.

#### Scenario: Five bars
- **WHEN** the October report is shown in November with data since Juli
- **THEN** the chart SHALL show Juli, Agustus, September, Oktober (highlighted) and November marked "berjalan", each with its amount as text

### Requirement: Whole-Period Comparison API
`GET /api/reports/monthly/comparison` SHALL return, for the owner and the requested basis and period, the totals, the previous period, the baseline, running progress, pillars with categories, facts and history. `GET /api/reports/monthly` SHALL return the same object as `comparison`. Every amount SHALL equal the ledger summary of its window and filters.

#### Scenario: Equal to the ledger
- **WHEN** the comparison for Oktober is requested
- **THEN** each history period's Pengeluaran and each pillar and category amount SHALL equal `GET /api/transactions` summaries for the same window and filters

#### Scenario: Another owner
- **WHEN** another owner has spending in the same months
- **THEN** none of it SHALL appear in any figure, fact or history bar

#### Scenario: Unauthenticated or unknown period
- **WHEN** the request has no valid session or Bearer key, or names a period that is not listed
- **THEN** the response SHALL be 401, or 404 `period_not_found`, with no figures

## MODIFIED Requirements

### Requirement: Four Kakeibo questions in a fixed order
The report SHALL answer the Kakeibo month-end questions with its figures: Masuk with its sources by category, Ditabung & investasi (fresh savings: saving-classified expenses and net savings movements, separate from the cash-flow surplus) and Pengeluaran. The page SHALL show the reflection note only when one exists. Every figure SHALL come from the existing reporting services and their rules.

#### Scenario: Savings separate from surplus
- **GIVEN** a month with Rp 5.000.000 inflow, Rp 3.000.000 outflow, a Rp 500.000 movement into a goal-linked pocket and a Rp 100.000 saving-classified expense
- **WHEN** the report is requested
- **THEN** fresh savings SHALL be Rp 600.000 with both components listed, the API surplus SHALL be Rp 2.000.000, and the page's Selisih SHALL be Masuk minus Pengeluaran, Rp 2.100.000, without calling it money set aside

#### Scenario: No reflection note
- **WHEN** neither the period nor the previous period has a reflection note
- **THEN** the page SHALL show no "Apa yang mau diperbaiki bulan depan?" block and no empty form

### Requirement: Spending answer
The spending answer SHALL give net outflow, spending per Kakeibo pillar and "Belum diklasifikasi", the top five categories and merchants, the "Tak Terduga" items and the recurring charges detected in the period. The page SHALL present Pengeluaran without the saving pillar, whose expenses belong to Ditabung & investasi.

#### Scenario: Unexpected items
- **WHEN** the month has three expenses with the effective pillar `unexpected`
- **THEN** the report SHALL list them with date, amount and description, and their total SHALL equal the `unexpected` pillar amount

#### Scenario: Recurring charges in the period
- **WHEN** a monthly charge was last seen inside the period
- **THEN** it SHALL be listed with its merchant, cadence and typical amount, and charges last seen before the period SHALL NOT be listed

### Requirement: Comparison with the previous period
A completed period SHALL be compared with the whole previous period of the same basis, never over the same elapsed days. The comparison SHALL mark a previous period in which tracking began after its first day as partial and give no verdict for it. No percentage change SHALL be shown.

#### Scenario: Partial previous month
- **WHEN** the October report is compared with a September in which tracking began on the 15th
- **THEN** the previous period SHALL carry `partial: true`, the interface SHALL say that September was recorded only from 15 September, and "belum lengkap" SHALL replace the ▲ or ▼ of every change against September

#### Scenario: Whole months, not the same days
- **GIVEN** September spending of Rp 100.000 on the 5th and Rp 900.000 on the 20th, and October spending of Rp 1.200.000
- **WHEN** the October report is requested on 2 November
- **THEN** the previous Pengeluaran SHALL be Rp 1.000.000 for the whole of September and the change SHALL be Rp 200.000 higher

### Requirement: Report page layout
`/laporan` SHALL present the report in Indonesian without verdict words, on phone and desktop in both themes, with the basis switch and stepper in the top bar and `basis` and `start` in the URL. "Kualitas data" SHALL be one line, then KPI cards, the pillar and category table, "Yang bisa diperbaiki", the monthly chart and top merchants, with no prose lead and no unused column. States SHALL be truthful.

#### Scenario: Phone width
- **WHEN** the page is shown at 390 CSS pixels
- **THEN** no content SHALL overflow horizontally and every action SHALL be keyboard operable with a visible focus

#### Scenario: New user
- **WHEN** a user without any transaction opens `/laporan`
- **THEN** the page SHALL say that the first report appears after the first recorded month, without zero figures presented as results

#### Scenario: Stepping to the previous report
- **GIVEN** the page was opened at `/laporan?basis=month&start=2026-10-01`
- **WHEN** the owner presses the stepper's previous button
- **THEN** the September report SHALL be shown and the URL SHALL become `/laporan?basis=month&start=2026-09-01`

#### Scenario: Past reports
- **WHEN** the report page is shown on a desktop
- **THEN** a "Laporan sebelumnya" card SHALL list the earlier periods of the basis with their readiness, each linking to its report

## REMOVED Requirements

### Requirement: Three-period median
**Reason**: The owner rejected comparisons over the same first days and asked for the average of the three months before (2026-11-04).
**Migration**: "Three-Period Average" compares whole periods; `comparison.baseline` replaces `comparison.median`.

### Requirement: Report Written Lead
**Reason**: The owner found prose summaries hard to read and asked for KPI figures instead (2026-11-04).
**Migration**: The KPI cards and the pillar table carry the same figures.

### Requirement: Report Lead Register
**Reason**: The written lead it governed is removed.
**Migration**: Every label on the page stays governed by `ui-copy-register`.
