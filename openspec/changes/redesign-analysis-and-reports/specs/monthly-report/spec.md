## MODIFIED Requirements

### Requirement: Report page layout
The web page `/laporan`, reachable from the navigation as "Laporan", SHALL present the report in Indonesian without verdicts, phone first and also for the desktop, in the light and the dark theme. The basis switch and a report stepper over the listed periods SHALL be in the top bar, and the page SHALL keep `basis` and `start` in the URL. The page SHALL show the written lead first, then the four questions in order. The comparison, "Kualitas data", the budget and the list of past reports SHALL be beside them on wide screens and between them on the phone. It SHALL handle loading, empty and error states truthfully.

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

## ADDED Requirements

### Requirement: Report Written Lead
The report SHALL open with a lead of two or three sentences built only from the report's own figures. The lead SHALL give the period and whether it is complete, Masuk and Belanja, and the Kakeibo pillar with the largest change against the previous period when a comparison exists. It SHALL also give the number of rows still unclassified, with a link to classify them.

#### Scenario: Complete month with a comparison
- **GIVEN** October with Masuk Rp 8.200.000, Belanja Rp 4.910.000, Keinginan Rp 310.000 lower than September and 2 unclassified rows
- **WHEN** the October report is shown
- **THEN** the lead SHALL read like "Oktober: masuk Rp 8.200.000, belanja Rp 4.910.000. Pengeluaran Keinginan turun Rp 310.000 dibanding September. 2 transaksi belum diklasifikasi." with the last sentence linking to classification

#### Scenario: No previous period
- **WHEN** the report has no comparison
- **THEN** the lead SHALL leave out the change sentence

### Requirement: Report Lead Register
The lead SHALL be in neutral standard Indonesian, SHALL contain no verdict, and SHALL not address the reader with "kamu" or "Anda". A sentence whose figure is unavailable SHALL be left out, never shown as Rp 0, and the lead SHALL respect hidden balances.

#### Scenario: Hidden balances
- **WHEN** "Sembunyikan saldo" is on
- **THEN** the amounts in the lead SHALL be hidden the same way as every other amount

#### Scenario: Unavailable figure
- **WHEN** the report has no Masuk figure for the period
- **THEN** the lead SHALL leave out that part of the sentence and SHALL show no Rp 0
