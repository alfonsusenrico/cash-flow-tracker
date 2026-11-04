# analysis-workspace Specification

## Purpose
Give one place to study a period's money through three views, Ringkasan, Kalender and Peta Aliran, that share one period choice and always agree with Beranda and Transaksi.

## Requirements

### Requirement: Analisis Tabs in the URL
`/insights` SHALL show the tabs Ringkasan, Kalender and Peta Aliran as an ARIA tab list with arrow-key, Home and End navigation. The selected tab SHALL be the query parameter `tab` with the values `ringkasan`, `kalender` and `aliran`. A missing or unknown value SHALL select Ringkasan. Changing the tab, the calendar day (`day`) or the flow view (`view`) SHALL update the URL without a server round trip and SHALL keep every other parameter.

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

### Requirement: Period Basis for All Analisis Tabs
Analisis SHALL offer a Siklus gaji / Bulan kalender toggle and the top-bar period stepper, and both SHALL apply to all three tabs. Analisis SHALL use only the cycle and month bases. When the shared period is a rolling window, Analisis SHALL show the current cycle, SHALL say so, and SHALL NOT change the shared value.

#### Scenario: Toggle applies to every tab
- **WHEN** the owner switches to Bulan kalender on Ringkasan and then opens Kalender
- **THEN** the calendar SHALL show the calendar month

#### Scenario: Stepping back
- **WHEN** the owner steps to the previous cycle on Peta Aliran
- **THEN** the flow SHALL show that cycle and replay SHALL cover all of its days

### Requirement: Ringkasan Figures and Names
Ringkasan SHALL show Masuk, Pengeluaran and Ditabung & investasi as explainable figures. Pengeluaran SHALL equal the ledger summary's `spending` that Transaksi and Beranda show, and Ditabung & investasi the Kakeibo saving bucket. The explanations SHALL list only server lines whose total equals the figure, and the client SHALL compute no money value.

#### Scenario: Keluar equals Beranda
- **GIVEN** the current cycle with Rp 1.834.000 of spending and Rp 500.000 of net fresh saving
- **WHEN** Ringkasan and Beranda are shown
- **THEN** Ringkasan and Beranda SHALL show the same Pengeluaran Rp 1.834.000 and the same Ditabung & investasi Rp 500.000, and Ringkasan's Pengeluaran SHALL equal Transaksi's for the cycle

#### Scenario: Explanation of Belanja
- **WHEN** the owner opens the explanation of Pengeluaran in a period with a Rp 40.000 reimbursement
- **THEN** it SHALL list gross spending, the applied reimbursement and any saving-pillar expense moved to Ditabung & investasi, and end with the Pengeluaran figure

#### Scenario: Older API without explanations
- **WHEN** the analytics response has no `explanations`
- **THEN** the figures SHALL show their values without an explanation trigger, and no figure SHALL be shown as Rp 0 in place of a missing value

### Requirement: Ringkasan Kakeibo Comparison
Ringkasan SHALL show the running period's Pengeluaran so far against the previous whole period's total, in total and per pillar, as a share without a verdict, then the last completed period against its three-period average (`monthly-report`) with each pillar's change as an amount, never a percentage. It SHALL NOT compare the same elapsed days.

#### Scenario: Kakeibo against the same day
- **GIVEN** day 9 of the cycle and a completed previous cycle
- **WHEN** Ringkasan shows the progress card
- **THEN** each pillar SHALL show its amount so far and its share of the previous cycle's pillar total without "Naik" or "Turun", no marker at the same day SHALL be drawn, and the amounts SHALL add up to Pengeluaran so far

#### Scenario: Unclassified spending
- **WHEN** the period has spending without a pillar
- **THEN** the unclassified bucket SHALL read "Perlu dipilih" with a link to classify it

### Requirement: Ringkasan Daily Spending
Ringkasan SHALL show one spending bar per local day of the period up to today, with the highest day labelled with its date and amount and today marked as still running, and SHALL offer the same values as a table.

#### Scenario: Daily bars
- **WHEN** the current cycle is shown
- **THEN** the highest day SHALL be labelled with its date and amount, today SHALL be marked as still running, later days SHALL be absent, and the same values SHALL be available as a table

### Requirement: Ringkasan Supporting Cards
Ringkasan SHALL show compact cards for the detected bills and subscriptions, "Seberapa sering ditanya" and "Kebiasaan dihafal", each with its headline figure first and details behind "Rincian", without method paragraphs. Its last card SHALL be "Kategori & anggaran" with unchanged behaviour. The weekday heatmap and the same-day increases and decreases SHALL NOT be shown.

#### Scenario: Category budget editing stays
- **WHEN** the owner edits a category's budget in "Kategori & anggaran"
- **THEN** the change SHALL be saved through the existing category endpoint and the Kakeibo and budget figures SHALL refresh

#### Scenario: No weekday heatmap
- **WHEN** Ringkasan is shown
- **THEN** no weekday heatmap card SHALL be rendered, and the analytics response SHALL still include `day_of_week_heatmap`

#### Scenario: Headline first
- **WHEN** Ringkasan shows "Tagihan & langganan terdeteksi" with three detected charges
- **THEN** the card SHALL show "3" and their monthly total first, and the charge list SHALL appear only after "Rincian" is opened

### Requirement: Analisis Layout and Accessibility
At a main column of 900 CSS pixels or less, the Ringkasan cards SHALL form one column whose DOM order follows the visual order. At 760 CSS pixels or less, the page SHALL use the phone composition. Each tab SHALL show loading, empty and error states inside its panel. The page SHALL meet WCAG 2.2 AA in the light and the dark theme at 1440, 1280, 1024, 768 and 390 CSS pixels, at 200% and 400% zoom, and with reduced motion. Every label SHALL follow the neutral register of `ui-copy-register`.

#### Scenario: Phone width
- **WHEN** Analisis is shown at 390 CSS pixels in either theme
- **THEN** no content SHALL overflow horizontally, every target SHALL be at least 24 by 24 CSS pixels, and focus SHALL stay visible and unobscured by the tab bar

#### Scenario: One tab fails
- **WHEN** the calendar request fails
- **THEN** Kalender SHALL show the error with "Coba lagi" while the toolbar, the stepper and the other tabs keep working

#### Scenario: Labels in the neutral register
- **WHEN** Analisis is shown
- **THEN** it SHALL read "Siklus gaji", "Seberapa sering ditanya" and "Menunggu jawaban", and no label SHALL contain "kamu", "Anda" or a listed slang token
