## ADDED Requirements

### Requirement: Beranda Hero
Beranda SHALL open with one hero, "Aman dipakai sampai gajian", as the page's only 44 px figure, followed by one line, a split bar of Pengeluaran, Ditabung & investasi, Terjadwal and Aman dipakai with the percentages in its accessible name, and Masuk, Pengeluaran, Ditabung & investasi and Terjadwal as supporting figures with one caption each. No other Beranda figure SHALL be 33 px or larger, and no hero or supporting figure SHALL be a button.

#### Scenario: Hero on a desktop
- **WHEN** the owner opens Beranda at 1440 × 900
- **THEN** the hero SHALL start at `y = 192`, the four supporting figures SHALL follow in one row in that order, and each of the five figures SHALL appear once on the page

#### Scenario: Figures add up to Masuk
- **WHEN** the hero shows masuk Rp 9.800.000
- **THEN** Pengeluaran, Ditabung & investasi, Terjadwal and the hero figure SHALL add up to Rp 9.800.000, or the hero line SHALL say by how much the period is over

#### Scenario: Screen reader
- **WHEN** a screen reader reaches the split bar
- **THEN** it SHALL read one image named with each segment and its percentage of masuk, and every supporting figure SHALL be read as text with its label and caption

#### Scenario: Savings decrease
- **WHEN** the period's net fresh saving is −Rp 500.000
- **THEN** Ditabung & investasi SHALL show Rp 500.000 with the caption "Tabungan berkurang", and its segment SHALL take no width in the bar

#### Scenario: No figure leaves its block
- **WHEN** Beranda is shown at any width from 320 px, including 1024 to 1199 px with the full sidebar
- **THEN** no hero, supporting or panel figure SHALL extend past its block or the page frame, and the page SHALL NOT scroll horizontally

### Requirement: Rincian of the Safe Amount
The hero SHALL carry the page's only "Rincian" control. It SHALL open the calculation of the safe amount: the rule, the lines Masuk, Pengeluaran, Ditabung & investasi and Terjadwal with signed amounts, the total equal to the hero figure, and a link to the period's transactions. Each line that has server lines SHALL open them in place.

#### Scenario: Lines and total
- **GIVEN** masuk Rp 9.518.400, Pengeluaran Rp 3.502.990, Ditabung & investasi Rp 2.750.000 and Terjadwal Rp 989.000 until 24 November
- **WHEN** the owner activates "Rincian"
- **THEN** a dialog titled "Aman dipakai sampai gajian" SHALL list Rp 9.518.400, −Rp 3.502.990, −Rp 2.750.000 and "Terjadwal sampai 24 Nov" −Rp 989.000, and its total "Aman dipakai" SHALL be Rp 2.276.410, equal to the hero

#### Scenario: A line opens its parts
- **WHEN** the owner opens the Pengeluaran, Ditabung & investasi or Terjadwal line in the dialog
- **THEN** Pengeluaran SHALL show its five buckets, each linked to the ledger with its `kakeibo` filter for the period
- **AND** Ditabung & investasi SHALL show what went into savings, what was taken out and what was spent from savings
- **AND** Terjadwal SHALL show each bill with its due date and each goal set-aside with its monthly amount and what is already funded

#### Scenario: One Rincian per page
- **WHEN** Beranda is shown
- **THEN** exactly one control named "Rincian" SHALL be on the page, and the supporting figures, Kekayaan bersih, Ketahanan Dana and Kakeibo SHALL have none

### Requirement: Beranda Context Panel
Beside the reading column from 1200 px, and after it below, Beranda SHALL show one panel with three blocks. Kekayaan bersih SHALL show the figure and "aset … · utang …" and link to Rekening & Kantong. Ketahanan Dana SHALL be one line of months of essential cost, the emergency balance and the target in months, and link to Target & Tagihan. Kakeibo for the period SHALL list the five spending buckets with amounts and ledger links, with no total and no Tabungan row.

#### Scenario: Net worth once
- **WHEN** a pocket and its parent bank both hold money and a credit account owes Rp 893.000
- **THEN** Kekayaan bersih SHALL count each balance once, equal the figure on Rekening & Kantong, and include Rp 893.000 in its utang

#### Scenario: Unknown essential cost
- **WHEN** the essential monthly cost is unknown
- **THEN** Ketahanan Dana SHALL say that it cannot be computed yet and why, and SHALL NOT show a number of months or a sentinel

#### Scenario: Unclassified spending
- **WHEN** "Belum diklasifikasi" holds Rp 40.000
- **THEN** the Kakeibo block SHALL list it with its amount, its label SHALL link to the ledger with `kakeibo=unclassified` for the period, and "Perlu dipilih di Tinjau" SHALL link to Tinjau

#### Scenario: Panel position
- **WHEN** Beranda is shown at 1440 px and at 1100 px
- **THEN** at 1440 px the panel SHALL be 352 px wide at the right of the reading column, and at 1100 px it SHALL follow the reading column at its full width with every figure inside its block

## MODIFIED Requirements

### Requirement: Beranda Period
Beranda SHALL describe the current open salary cycle when the owner has one, and otherwise the current calendar month, and SHALL say which one it uses in the title block's line: the cycle's start, its end when the expected salary date is known, and the day of the cycle. Beranda SHALL NOT offer a period selector; other periods are read on Analisis, Transaksi and Laporan.

#### Scenario: Open salary cycle
- **GIVEN** a confirmed salary receipt on 25 October and no later receipt, today is 2 November, and the next salary is expected on 25 November
- **WHEN** the owner opens Beranda
- **THEN** the line SHALL read "Siklus gaji 25 Okt – 24 Nov · hari ke-9", and every Beranda figure SHALL cover 25 October 00.00 to now in Asia/Jakarta

#### Scenario: Expected salary date unknown
- **GIVEN** the same cycle without an expected salary date
- **WHEN** the owner opens Beranda
- **THEN** the line SHALL read "Siklus gaji sejak 25 Okt · hari ke-9"

#### Scenario: No confirmed salary cycle
- **GIVEN** an owner without a confirmed salary receipt on 10 November
- **WHEN** the owner opens Beranda
- **THEN** the line SHALL name November and say that the salary cycle is not available yet, every figure SHALL cover 1 November to now, and Beranda SHALL link to the salary settings

### Requirement: Waiting-for-Answer Figure
Beranda SHALL state, in the Rincian of its hero, the count and total of the rows that count towards the Tinjau attention badge in the period, and how much of it is already inside the period's figures. It SHALL list them there by Tinjau section and say, per line, whether and under which bucket the amount is already counted. Waiting rows SHALL stay recorded and counted exactly as the ledger counts them.

#### Scenario: Question row is already counted
- **GIVEN** a merchant-less Rp 45.000 payment with an open "Apa ini?" question and a one-sided Rp 112.000 transfer leg awaiting its counterpart
- **WHEN** the owner opens "Menunggu jawaban" in the hero's Rincian
- **THEN** it SHALL show Rp 157.000 in two lines, SHALL say that Rp 45.000 is already inside "Pengeluaran" as "Belum diklasifikasi" and that Rp 112.000 is not counted yet, and SHALL link to Tinjau

#### Scenario: Answering changes the bucket, not the total
- **WHEN** the owner answers the Rp 45.000 question as "Makan & minum" (need)
- **THEN** "Pengeluaran" SHALL stay the same, "Belum diklasifikasi" SHALL fall by Rp 45.000, need SHALL rise by Rp 45.000 and the waiting figure SHALL fall by Rp 45.000

#### Scenario: Not beside Perlu tindakan
- **WHEN** rows wait for an answer
- **THEN** the "Perlu tindakan" header SHALL show "Buka Tinjau" with the attention count and no waiting amount

### Requirement: Safe-to-Spend Today
Beranda's hero figure SHALL be "Aman dipakai sampai gajian": the remaining amount for the whole period (masuk less keluar less scheduled), never a per-day or per-week amount. Its line SHALL give the days left and the expected salary date in an open cycle with that date after today, and the reason otherwise. The API's `per_day` SHALL stay as defined for other consumers, unavailable with a reason otherwise.

#### Scenario: Available
- **GIVEN** masuk Rp 9.800.000, keluar Rp 3.164.500, scheduled Rp 1.713.500, today 2 November and an expected salary date of 25 November
- **WHEN** the owner opens Beranda
- **THEN** the hero SHALL read Rp 4.922.000 with the line "23 hari lagi sampai gajian 25 Nov", its Rincian SHALL list Masuk, Pengeluaran, Ditabung & investasi and Terjadwal, and no per-day amount SHALL be shown

#### Scenario: Next salary date unknown
- **GIVEN** an open cycle whose confirmed receipts do not form a regular pattern
- **WHEN** the owner opens Beranda
- **THEN** the hero SHALL still show the remaining amount with its Rincian lines and its line SHALL say "Tanggal gaji berikutnya belum diketahui" instead of a day count

#### Scenario: Salary is late
- **GIVEN** the expected salary date was yesterday and no new receipt is confirmed
- **WHEN** the owner opens Beranda
- **THEN** the hero SHALL show the remaining amount and its line SHALL say that the salary has not been recorded yet, and no negative or zero day count SHALL be used

#### Scenario: Spent more than the remaining amount
- **WHEN** keluar plus scheduled exceeds masuk by Rp 300.000 with 10 days left
- **THEN** the hero SHALL show Rp 0 and its line SHALL say that the period is Rp 300.000 over, instead of showing a negative safe amount

### Requirement: Kerjakan Dulu
Under "Perlu tindakan" Beranda SHALL list at most three of what needs the owner first: open questions, awaiting or ambiguous transfers, groups that need an answer, automatically linked plans to check, and plans due or due within two days. Each row SHALL name the merchant or plan, its kind and time, and its amount where it has one, and the whole row SHALL be its one action. A due plan SHALL keep its link, skip and record actions.

#### Scenario: Items and order
- **GIVEN** one open question, two awaiting transfer legs, one group that needs an answer and a plan due tomorrow
- **WHEN** the owner opens Beranda
- **THEN** the header SHALL show "Buka Tinjau · 4", and the rows SHALL be the question, then the awaiting transfers, then the plan
- **AND** each review row SHALL open Tinjau at the matching item, and the group SHALL be reached through "Buka Tinjau"

#### Scenario: One-column Beranda
- **WHEN** Beranda is shown at any width, including one column below 1200 px
- **THEN** "Perlu tindakan" SHALL follow the hero directly and come before "Aktivitas terbaru" in the visual, reading and focus order

#### Scenario: Recording a due plan
- **WHEN** the owner selects a plan due today in "Perlu tindakan"
- **THEN** "Rencana jatuh tempo" SHALL open with the same link, skip and record actions as before the redesign, same-amount captures SHALL be offered for linking first, and nothing SHALL be posted without the owner's explicit confirmation

#### Scenario: Nothing to do
- **WHEN** nothing waits and no plan is due
- **THEN** the section SHALL say "Tidak ada yang perlu dijawab." in one line, and its header link SHALL read "Buka Tinjau" without a count

### Requirement: Recent Activity on Beranda
Under "Aktivitas terbaru" Beranda SHALL list up to eight transactions from today and yesterday that do not wait for an answer, newest first, under one heading per day. Each row SHALL show the time, the merchant or movement route, the account, the pillar or status, and the amount with its sign and type colour, plus a link to all transactions. Selecting a row SHALL open the shared transaction dialog of Transaksi, centered from 768 px.

#### Scenario: Waiting row in the list
- **WHEN** a row from today has an open question
- **THEN** it SHALL NOT be listed under "Aktivitas terbaru", and it SHALL be reachable from "Perlu tindakan" or Tinjau

#### Scenario: Day headings
- **WHEN** today and yesterday both have rows
- **THEN** they SHALL be grouped under "KAMIS, 5 NOVEMBER · HARI INI" and "RABU, 4 NOVEMBER · KEMARIN", and each row SHALL show its time, such as "18.23", and not its date

#### Scenario: Movement row
- **WHEN** a "Pindah Saldo" between two of the owner's accounts is listed
- **THEN** it SHALL appear once as "source → target" with a sky amount, and SHALL say "Tidak dihitung" instead of a pillar
- **AND** a pocket SHALL be named with its parent, for example "Jago · Main Pocket → BCA · ATM"

#### Scenario: Expense and income rows
- **WHEN** an expense of Rp 30.000 and an income of Rp 9.518.400 are listed
- **THEN** the expense SHALL read "−Rp 30.000" in the ink with its pillar, and the income SHALL read "+Rp 9.518.400" in green with "Uang masuk"

#### Scenario: Row opens the transaction drawer
- **WHEN** the owner selects "Kedai Awan Rp 30.000" in "Aktivitas terbaru"
- **THEN** the transaction dialog SHALL open for that row with its provenance, "Ubah", "Tandai sudah dicek" where allowed and "Hapus"
- **AND** closing it SHALL return focus to the row, and a delete confirmed there SHALL refresh Beranda's figures

#### Scenario: Row deleted elsewhere
- **GIVEN** the row was deleted in another tab
- **WHEN** the owner selects it on Beranda
- **THEN** the dialog SHALL say "Transaksi ini sudah tidak ada" and Beranda's list SHALL refresh

#### Scenario: Phone row
- **WHEN** Beranda is shown at 390 px
- **THEN** each row SHALL show two lines, the title and "18.23 · Kebutuhan", with the amount at the right and no horizontal scrolling

### Requirement: Beranda Loading and Failure
Beranda SHALL show static placeholders of the final layout while loading. When the summary request fails, it SHALL say in the hero's place that the figures could not be loaded and offer a retry, without showing any amount, while the blocks with their own data still render.

#### Scenario: Loading
- **WHEN** the summary is still loading
- **THEN** Beranda SHALL be marked busy and show placeholders for the hero, the rows and the panel, without a pulse and without any amount

#### Scenario: Summary request fails
- **WHEN** `GET /api/dashboard/cycle-summary` fails
- **THEN** the hero's place SHALL show the failure message and "Coba lagi" and no Rp 0
- **AND** "Perlu tindakan", "Aktivitas terbaru" and Ketahanan Dana SHALL still render, and Kekayaan bersih and Kakeibo SHALL be left out

## REMOVED Requirements

### Requirement: Uang Siklus Ini Card
**Reason**: On 2026-11-05 the owner accepted option 6 "Markas+". The audit found that the card repeats four figures already shown above it. The prototype merges its bar and amounts into the hero.
**Migration**: The bar becomes the hero's split bar, and the four amounts become the supporting figures ("Beranda Hero"). "Masuk" and the formula move into the Rincian ("Rincian of the Safe Amount"). The "Tagihan" link is replaced by the Terjadwal line of the Rincian and the bell.

### Requirement: Kakeibo and Ketahanan Dana Cards
**Reason**: The prototype shows Kakeibo without its repeated Pengeluaran total and Tabungan row, and Ketahanan Dana as one line in a context panel. The full Ketahanan Dana card belongs to Target & Tagihan.
**Migration**: Both move into "Beranda Context Panel". The saving bucket is "Ditabung & investasi" in the hero. The coverage bar and the rupiah target stay on Target & Tagihan.

### Requirement: Beranda KPI Cards
**Reason**: Five competing KPI cards put seven figures within 75 % of the hero's size (audit finding 1). The prototype has one hero with quiet supporting figures and moves Kekayaan bersih to the panel.
**Migration**: "Aman dipakai sampai gajian" is the hero ("Beranda Hero", "Safe-to-Spend Today"). Masuk, Pengeluaran and Ditabung & investasi are supporting figures. Kekayaan bersih is in "Beranda Context Panel". The rule of no same-date comparison stays in `observed-insights`.
