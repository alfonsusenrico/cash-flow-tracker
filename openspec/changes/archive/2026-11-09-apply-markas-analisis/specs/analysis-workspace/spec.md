## MODIFIED Requirements

### Requirement: Analisis Tabs in the URL
`/insights` SHALL show the tabs Ringkasan, Kalender and Peta Aliran as an ARIA tab list with arrow-key, Home and End navigation, drawn as underline tabs with a 2 px bar under the selected one. The selected tab SHALL be the query parameter `tab` (`ringkasan`, `kalender`, `aliran`); a missing or unknown value SHALL select Ringkasan. Changing the tab, the day (`day`) or the flow view (`view`) SHALL update the URL without a round trip and keep every other parameter.

#### Scenario: Old link
- **WHEN** the owner opens `/insights`
- **THEN** Ringkasan SHALL be selected and the page SHALL render as before the tab parameter existed

#### Scenario: Tab from a link
- **WHEN** the owner opens `/insights?tab=kalender&day=2026-11-02`
- **THEN** Kalender SHALL be selected with 2 November as the selected day

#### Scenario: Unknown tab
- **WHEN** the owner opens `/insights?tab=grafik`
- **THEN** Ringkasan SHALL be selected and no error SHALL be shown

#### Scenario: Switching from a URL with a query
- **GIVEN** the page was opened at `/insights?tab=aliran`
- **WHEN** the owner selects Ringkasan
- **THEN** the URL SHALL become `/insights?tab=ringkasan` and the Ringkasan panel SHALL render

#### Scenario: Underline tabs and the report link
- **WHEN** Analisis is shown at 1440 px
- **THEN** the selected tab SHALL carry a 2 px accent bar under it, the others none, and "Baca Laporan Bulanan" SHALL sit at the right of the tabs and open `/laporan`; at 390 px the link SHALL be hidden and the tabs SHALL be at least 44 px high

### Requirement: Period Basis for All Analisis Tabs
Analisis SHALL offer the Siklus gaji / Bulan kalender segment and the period stepper in its title block, and both SHALL apply to all three tabs. The stepper SHALL name the period from its dates in the interface language. Analisis SHALL use only the cycle and month bases. When the shared period is a rolling window, Analisis SHALL show the current cycle, SHALL say so, and SHALL NOT change the shared value.

#### Scenario: Toggle applies to every tab
- **WHEN** the owner switches to Bulan kalender on Ringkasan and then opens Kalender
- **THEN** the calendar SHALL show the calendar month

#### Scenario: Stepping back
- **WHEN** the owner steps to the previous cycle on Peta Aliran
- **THEN** the flow SHALL show the whole of that cycle, and the stepper SHALL name it

#### Scenario: Period name in English
- **GIVEN** an English account on the cycle of 23 October to 24 November
- **WHEN** Analisis is shown, and then the owner switches to Bulan kalender and steps back once
- **THEN** the stepper SHALL read "Cycle 23 Oct – 24 Nov" and then "Oct 2026", and no Indonesian month name SHALL appear in it

### Requirement: Ringkasan Figures and Names
Ringkasan SHALL show Pengeluaran as its one hero figure, with one line naming Masuk and Ditabung & investasi. Pengeluaran SHALL equal the ledger summary's `spending` that Transaksi and Beranda show, and Ditabung & investasi the Kakeibo saving bucket. One "Rincian" SHALL list only server lines whose total equals the figure. No figure SHALL be a dotted button, and the client SHALL compute no money value.

#### Scenario: Keluar equals Beranda
- **GIVEN** the current cycle with Rp 1.834.000 of spending and Rp 500.000 of net fresh saving
- **WHEN** Ringkasan and Beranda are shown
- **THEN** Ringkasan's hero and Beranda SHALL show the same Pengeluaran Rp 1.834.000, its line and Beranda the same Ditabung & investasi Rp 500.000, and the hero SHALL equal Transaksi's Pengeluaran for the cycle

#### Scenario: Explanation of Belanja
- **WHEN** the owner opens "Rincian" beside the hero in a period with a Rp 40.000 reimbursement
- **THEN** a centered dialog SHALL list gross spending, the applied reimbursement and any saving-pillar expense moved to Ditabung & investasi, and end with the Pengeluaran figure; Esc SHALL return focus to "Rincian"

#### Scenario: Older API without explanations
- **WHEN** the analytics response has no `explanations`
- **THEN** the hero SHALL show its value without "Rincian", and no figure SHALL be shown as Rp 0 in place of a missing value

#### Scenario: No KPI row
- **WHEN** Ringkasan is shown
- **THEN** Masuk, Pengeluaran and Ditabung & investasi SHALL each appear once, the page SHALL have exactly one 44 px figure, and no element SHALL carry a dotted underline

### Requirement: Ringkasan Kakeibo Comparison
Ringkasan SHALL compare whole periods only. While running, it SHALL show Pengeluaran so far as a share of the whole previous period and of the average of up to three whole periods (`monthly-report`), with bars and no verdict, and per pillar the amount, share and previous amount. A completed period SHALL show its change against both as amounts with ▲ or ▼, and per pillar the change against the average. It SHALL NOT compare the same elapsed days.

#### Scenario: Running period
- **GIVEN** day 14 of a cycle with Rp 3.502.990 so far, a whole previous cycle of Rp 5.452.830 and an average of Rp 5.404.610 over August to October
- **WHEN** Ringkasan is shown
- **THEN** it SHALL read "64% dari total siklus 25 Sep – 22 Okt" and "65% dari rata-rata 3 bulan (Agu–Okt)" with both amounts and bars, the shares SHALL come from the server, and no ▲, ▼, "naik" or "turun" SHALL appear

#### Scenario: Completed period
- **GIVEN** October with Rp 5.993.830, a whole September of Rp 5.240.000 and an average of Rp 4.943.333
- **WHEN** the owner steps back to October
- **THEN** the rows SHALL read "▲ Rp 753.830" against September and "▲ Rp 1.050.497" against the average, and each pillar SHALL show its change against the average

#### Scenario: Kakeibo against the same day
- **WHEN** the running period is shown
- **THEN** the pillar amounts SHALL add up to Pengeluaran so far, and no marker at the same day of the previous period SHALL be drawn

#### Scenario: Unclassified spending
- **WHEN** the period has spending without a pillar
- **THEN** the "Belum diklasifikasi" row SHALL link its amount to the ledger and offer "Perlu dipilih", which opens the classification for that window

#### Scenario: No average yet
- **WHEN** fewer than two whole periods precede the viewed one
- **THEN** the second row SHALL read "Rata-rata muncul setelah 2 bulan lengkap" and no Rp 0 average SHALL be shown

### Requirement: Ringkasan Daily Spending
Ringkasan SHALL show "Pengeluaran harian" as one bar per local day of the period up to today, with gridlines, the highest day in the expense mark colour and today hatched, a legend naming both with date and amount, and the same values as a table. Pointing at a bar, or tapping it on a touch screen, SHALL show that day's date and amount above the bar until the pointer leaves the chart, the bar is tapped again or Esc is pressed.

#### Scenario: Daily bars
- **WHEN** the current cycle is shown
- **THEN** the highest day SHALL be drawn in the expense mark colour and named in the legend with its date and amount, today SHALL be hatched and named as still running, later days SHALL be absent, and "Lihat sebagai tabel" SHALL show the same values as a table

#### Scenario: Hovering a bar
- **GIVEN** 1 November with Rp 40.000 of spending
- **WHEN** the owner points at its bar
- **THEN** "1 Nov" and "Rp 40.000" SHALL appear above the bar, inside the chart, and SHALL disappear when the pointer leaves the chart or Esc is pressed

#### Scenario: Tapping a bar
- **WHEN** the owner taps today's bar on a phone
- **THEN** its date with "hari ini" and its amount SHALL appear, and a second tap SHALL hide them

#### Scenario: Labels at every width
- **WHEN** the chart is shown at 1440, 1024, 390 and 320 px
- **THEN** every axis and date label SHALL render at 12 px, no two labels SHALL overlap, and the chart SHALL not overflow its column

### Requirement: Analisis Layout and Accessibility
From 1200 CSS pixels Ringkasan and Kalender SHALL show a 352 px context panel beside the reading column, below 1200 the panel after it, and below 768 the phone composition, by viewport width. Each tab SHALL show loading, empty and error states inside its panel. The page SHALL meet WCAG 2.2 AA in both themes at 1440, 1280, 1024, 768, 390 and 320 CSS pixels, at 200% and 400% zoom and with reduced motion, in the register of `ui-copy-register`.

#### Scenario: Phone width
- **WHEN** Analisis is shown at 390 and at 320 CSS pixels in either theme
- **THEN** no content SHALL overflow horizontally, every target SHALL be at least 24 by 24 CSS pixels, segments and small buttons SHALL be at least 40 px high, and focus SHALL stay visible and unobscured by the tab bar

#### Scenario: One tab fails
- **WHEN** the calendar request fails
- **THEN** Kalender SHALL show the error with "Coba lagi" while the title block, the stepper and the other tabs keep working

#### Scenario: Labels in the neutral register
- **WHEN** Analisis is shown in Indonesian
- **THEN** it SHALL read "Siklus gaji", "Pengeluaran harian" and "Menunggu jawaban", no figure SHALL be labelled "Belanja" or "Keluar", and no label SHALL contain "kamu", "Anda" or a listed slang token

#### Scenario: Empty period
- **WHEN** the viewed period has no spending
- **THEN** Ringkasan SHALL say "Belum ada pengeluaran di periode ini." and show no Rp 0 figure as a result

## ADDED Requirements

### Requirement: Ringkasan Context Panel
Ringkasan's context panel SHALL show "Tagihan & langganan" with the detected charges' count and monthly-equivalent total, the four largest charges as rows that open their ledger rows, "Lihat semua" opening a centered dialog with every charge and its plan state, and "Jadikan rencana". It SHALL then link to the latest closed monthly report while one is ready.

#### Scenario: Bills panel
- **GIVEN** eight detected charges with a monthly equivalent of Rp 2.428.990
- **WHEN** Ringkasan is shown at 1440 px
- **THEN** the panel SHALL read "8 · Rp 2.428.990/bln", list four charges, and "Lihat semua" SHALL open a dialog listing all eight with "Sudah ada rencana" or "Jadikan rencana"

#### Scenario: Report link
- **GIVEN** the October 2026 report is ready
- **WHEN** Ringkasan is shown
- **THEN** the panel SHALL link "Laporan Oktober 2026 siap dibaca" to that report, and without a ready report the block SHALL be absent

#### Scenario: One Rincian per page
- **WHEN** Ringkasan is shown
- **THEN** the only control named "Rincian" SHALL be the hero's

### Requirement: Spending by Category on Ringkasan
Ringkasan SHALL list the period's spending per category in groups per Kakeibo pillar, each under a 2 px rail in the pillar colour with its count, sorted by amount, with a budget bar and "{n}% dari {budget}" or "tanpa anggaran". Each row SHALL open the ledger for that category and period. "Tanpa kategori" and archived categories with spending SHALL stay listed. "Kelola kategori" and "Atur anggaran" SHALL open centered dialogs.

#### Scenario: Grouped and sorted
- **WHEN** Kebutuhan holds Cicilan Rp 850.000 without a budget and Belanja Dapur Rp 590.000 of a Rp 1.000.000 budget
- **THEN** both SHALL be under "KEBUTUHAN · 7 kategori" in that order, Belanja Dapur with a bar and "59% dari Rp 1.000.000", Cicilan with "tanpa anggaran"

#### Scenario: Over budget
- **WHEN** Belanja is Rp 527.000 against a Rp 500.000 budget
- **THEN** its bar SHALL be full in the expense mark colour and "105%" SHALL be in the expense text colour

#### Scenario: Whole-row link
- **WHEN** the owner activates the Belanja Dapur row
- **THEN** the ledger SHALL open with the same period, `type=expense` and that category

#### Scenario: Categories without spending
- **WHEN** two categories have no spending
- **THEN** they SHALL be hidden behind "Tampilkan 2 kategori tanpa pengeluaran"

### Requirement: One Spending Basis on Analisis
Every figure Analisis names "Pengeluaran" SHALL be the Pengeluaran basis of Beranda and Transaksi: reportable expenses outside the Tabungan pillar, net of reimbursements. The analytics, the calendar and the flow SHALL return it per day, per category and in total as read-only additions that add up to `comparison.totals.spending`; every existing field SHALL keep its value (owner decision, 2026-11-08).

#### Scenario: Saving-pillar expense
- **GIVEN** a period with Rp 4.355.990 of Pengeluaran and a Rp 300.000 expense in the Tabungan pillar
- **WHEN** Ringkasan, Kalender and Peta Aliran are shown for that period
- **THEN** the hero, the Kalender hero, the sum of the daily bars, the sum of the category amounts and Peta Aliran's Pengeluaran SHALL each read Rp 4.355.990, and the Belanja-basis fields SHALL still return Rp 4.655.990

#### Scenario: Fields of the Pengeluaran basis
- **WHEN** the analytics, the calendar and the flow are requested
- **THEN** they SHALL carry `daily_cadence[].spending`, `category_variance[].spending` and `spending_percentage_used`, `totals.spending`, `days[].spending`, `days[].spending_heat` and `spending_heat_thresholds`, and `summary.spending`

#### Scenario: Reimbursement on the Pengeluaran basis
- **WHEN** a Rp 40.000 reimbursement offsets a Kebutuhan category in the period
- **THEN** the daily and category Pengeluaran SHALL be reduced by it on that category's days, never below zero, and still add up to `comparison.totals.spending`

### Requirement: Analisis in Both Languages
Analisis SHALL read in English and Indonesian from one catalogue per language, with dates, amounts and compact amounts formatted by the account region, and SHALL show no untranslated-page notice. Every figure SHALL be the same string in both languages for the same data, and "Pengeluaran" / "Spending" SHALL be the only label of the spending figure.

#### Scenario: English Analisis
- **WHEN** an English account in region Indonesia opens Analisis
- **THEN** the page SHALL read "Analysis", "Overview", "Calendar", "Money flow", "Spending" and "Daily spending", amounts SHALL read "Rp 3.502.990", compact amounts "79K", and no region SHALL carry `lang="id"`

#### Scenario: Figures in both languages
- **GIVEN** an English and an Indonesian account with the same data
- **WHEN** both open each tab of Analisis
- **THEN** every amount, count, percent and date number SHALL be the same string in both

## REMOVED Requirements

### Requirement: Ringkasan Supporting Cards
**Reason**: The cards are replaced by the context panel, the category list and two dialogs. "Seberapa sering ditanya" moves to Pengaturan › Kebiasaan and "Kebiasaan dihafal" is dropped, because that section already lists every habit.
**Migration**: "Ringkasan Context Panel", "Spending by Category on Ringkasan", `habit-learning` "Seberapa Sering Ditanya Panel" and `finance-dashboard-cockpit` "Inline Category Budget Editing" carry the behaviour. The analytics response still includes `day_of_week_heatmap`, and no weekday heatmap or same-day comparison is shown.
