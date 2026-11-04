# beranda-cycle-summary Specification

## Purpose
Beranda tells the owner, in a few checkable numbers, where the current period stands: what went out, what is safe to spend today, what still waits for an answer, and what to do first, on desktop and on phones.

## Requirements

### Requirement: Beranda Period
Beranda SHALL describe the current open salary cycle when the owner has one, and otherwise the current calendar month, and SHALL say which one it uses. Beranda SHALL NOT offer a period selector; other periods are read on Analisis, Transaksi and Laporan.

#### Scenario: Open salary cycle
- **GIVEN** a confirmed salary receipt on 25 October and no later receipt, and today is 2 November
- **WHEN** the owner opens Beranda
- **THEN** the header SHALL read "Siklus gaji hari ke-9" with the cycle start date, and every Beranda figure SHALL cover 25 October 00.00 to now in Asia/Jakarta

#### Scenario: No confirmed salary cycle
- **GIVEN** an owner without a confirmed salary receipt on 10 November
- **WHEN** the owner opens Beranda
- **THEN** the header SHALL name November and say that the salary cycle is not available yet, every figure SHALL cover 1 November to now, and Beranda SHALL link to the salary settings

### Requirement: Keluar Figure
The cycle summary's `keluar` SHALL stay the period's Kakeibo total, net of applied reimbursements. Beranda SHALL show it split: "Pengeluaran" (`spending`, need, want, culture, unexpected and unclassified) and "Ditabung & investasi" (`fresh_saving`, the saving bucket with net fresh saving). Each explanation SHALL list its buckets with amounts and ledger links, and "Keluar" SHALL NOT be a figure label.

#### Scenario: Saving counts in Keluar
- **GIVEN** in the cycle Rp 1.420.000 need, Rp 610.000 want, Rp 245.000 culture, Rp 89.500 unexpected, Rp 800.000 net fresh saving and Rp 0 unclassified
- **WHEN** the owner opens the explanations of "Pengeluaran" and "Ditabung & investasi"
- **THEN** Pengeluaran SHALL be Rp 2.364.500 in five lines, Ditabung & investasi Rp 800.000 saying that it is moved to pockets or invested, and the two SHALL add up to the Kakeibo total Rp 3.164.500 that Analisis shows for the same period

#### Scenario: Internal movement is not Keluar
- **WHEN** the owner moves Rp 500.000 from a bank to an e-wallet that is neither a goal account nor an investment
- **THEN** neither "Pengeluaran" nor "Ditabung & investasi" SHALL change, and the Pengeluaran explanation SHALL say that moves between the owner's own accounts are not counted

### Requirement: Waiting-for-Answer Figure
"Perlu tindakan" SHALL show, beside its count, the total amount of rows that count towards the Tinjau attention badge in the period. Its explanation SHALL list them by Tinjau section and SHALL state, per line, whether the amount is already inside "Pengeluaran" or "Ditabung & investasi" and under which bucket. Waiting rows SHALL stay recorded and counted exactly as the ledger counts them.

#### Scenario: Question row is already counted
- **GIVEN** a merchant-less Rp 45.000 payment with an open "Apa ini?" question and a one-sided Rp 112.000 transfer leg awaiting its counterpart
- **WHEN** the owner opens the explanation of the waiting figure
- **THEN** it SHALL show Rp 157.000 in two lines, SHALL say that Rp 45.000 is already inside "Pengeluaran" as "Belum diklasifikasi" and that Rp 112.000 is not counted yet, and SHALL link to Tinjau

#### Scenario: Answering changes the bucket, not the total
- **WHEN** the owner answers the Rp 45.000 question as "Makan & minum" (need)
- **THEN** "Pengeluaran" SHALL stay the same, "Belum diklasifikasi" SHALL fall by Rp 45.000, need SHALL rise by Rp 45.000 and the waiting figure SHALL fall by Rp 45.000

### Requirement: Safe-to-Spend Today
Beranda's hero figure SHALL be "Aman dipakai sampai gajian": the remaining amount for the whole period (masuk less keluar less scheduled), never a per-day or per-week amount. In an open cycle with an expected next salary date after today it SHALL show the days left and that date. The API's `per_day` SHALL stay as defined for other consumers, unavailable with a reason otherwise.

#### Scenario: Available
- **GIVEN** masuk Rp 9.800.000, keluar Rp 3.164.500, scheduled Rp 1.713.500, today 2 November and an expected salary date of 25 November
- **WHEN** the owner opens the safe-to-spend explanation
- **THEN** the hero SHALL read Rp 4.922.000 with "23 hari lagi · gaji ± 25 Nov", its explanation SHALL list masuk, Pengeluaran, Ditabung & investasi and Terjadwal, and no per-day amount SHALL be shown

#### Scenario: Next salary date unknown
- **GIVEN** an open cycle whose confirmed receipts do not form a regular pattern
- **WHEN** the owner opens Beranda
- **THEN** the hero SHALL still show the remaining amount with its lines and SHALL say "Tanggal gaji berikutnya belum diketahui" instead of a day count

#### Scenario: Salary is late
- **GIVEN** the expected salary date was yesterday and no new receipt is confirmed
- **WHEN** the owner opens Beranda
- **THEN** the hero SHALL show the remaining amount and say that the salary has not been recorded yet, and no negative or zero day count SHALL be used

#### Scenario: Spent more than the remaining amount
- **WHEN** keluar plus scheduled exceeds masuk by Rp 300.000 with 10 days left
- **THEN** the hero SHALL say that the period is Rp 300.000 over instead of showing a negative safe amount

### Requirement: Scheduled Amount
"Terjadwal" SHALL be the unpaid occurrences of the owner's active non-income plans that are due or overdue up to the day before the expected salary date, plus the unfunded part of the current period's goal set-asides. Paid, linked or skipped occurrences SHALL NOT count, and a set-aside already moved into the goal's accounts this period SHALL NOT count again.

#### Scenario: Linked payment leaves Terjadwal
- **GIVEN** a Rp 600.000 plan due on 5 November inside the window
- **WHEN** its payment is linked to the plan
- **THEN** "Terjadwal" SHALL fall by Rp 600.000 while "Keluar" rises by the payment, so the remaining amount does not count it twice

#### Scenario: Goal set-aside already funded
- **GIVEN** a goal with a Rp 600.000 set-aside for the period and Rp 400.000 already moved into its pocket this period
- **WHEN** the scheduled amount is computed
- **THEN** the goal SHALL contribute Rp 200.000 and its explanation line SHALL show both amounts

#### Scenario: Without an expected salary date
- **WHEN** the expected salary date is unknown
- **THEN** "Terjadwal" SHALL cover the next 30 days, and its explanation SHALL name that window

### Requirement: Uang Siklus Ini Card
Beranda SHALL show "Uang siklus ini": masuk for the period and a segmented bar of "Pengeluaran", "Ditabung & investasi", "Terjadwal" and "Aman dipakai" (remaining). Each segment SHALL carry its amount as text with a label beside it, every amount SHALL be explainable, and the bar SHALL NOT be the only carrier of the values.

#### Scenario: Segments add up
- **WHEN** the card shows masuk Rp 9.800.000
- **THEN** the four labelled amounts SHALL add up to Rp 9.800.000, or the card SHALL say by how much the period is over when they exceed it

#### Scenario: Screen reader
- **WHEN** a screen reader reaches the card
- **THEN** it SHALL read each segment's name and amount as text, and the decorative bar SHALL be hidden from it

### Requirement: Kerjakan Dulu
Beranda SHALL list what needs the owner first: open questions, awaiting or ambiguous transfers, groups that need an answer, and plan occurrences that are due or due within two days. Each item SHALL name the merchant or plan, amount and time and offer one action. A due plan SHALL keep its link, skip and record actions. When the Beranda cards form one column, "Perlu tindakan" SHALL come directly after the KPI cards, before "Uang siklus ini", in both the visual and the reading and focus order.

#### Scenario: Items and order
- **GIVEN** one open question, two awaiting transfer legs, one group that needs an answer and a plan due tomorrow
- **WHEN** the owner opens Beranda
- **THEN** "Perlu tindakan" SHALL show a count of 4 Tinjau items, list the question first and the plan last, open Tinjau at the matching item from each review action, and show "Buka Tinjau"

#### Scenario: One-column Beranda
- **WHEN** the Beranda main column is 900 px wide or narrower above the phone layout
- **THEN** "Perlu tindakan" SHALL follow the KPI cards directly and come before "Uang siklus ini", and keyboard focus SHALL reach it in that order

#### Scenario: Recording a due plan
- **WHEN** the owner chooses to record a due plan from "Perlu tindakan"
- **THEN** the same confirmation as before the redesign SHALL appear, same-amount captures SHALL be offered for linking first, and nothing SHALL be posted without the owner's explicit confirmation

#### Scenario: Nothing to do
- **WHEN** nothing waits and no plan is due
- **THEN** the card SHALL say "Semua sudah ditinjau" and SHALL show the count of rows recorded automatically today with an optional link to check them

### Requirement: Recent Activity on Beranda
Beranda SHALL list up to eight transactions from today and yesterday, newest first, with merchant or movement, account, time, Kakeibo bucket or status, provenance and amount in its semantic colour, and a link to all transactions. Selecting a row SHALL open the shared transaction drawer of Transaksi for that row, with the same detail and actions.

#### Scenario: Waiting row in the list
- **WHEN** a row with an open question is listed
- **THEN** it SHALL show "Menunggu jawaban" as its status instead of a pillar and SHALL show its amount

#### Scenario: Movement row
- **WHEN** a "Pindah Saldo" between two of the owner's accounts is listed
- **THEN** it SHALL appear once as "source → target" with a sky amount, and SHALL say "Tidak dihitung" instead of a pillar

#### Scenario: Row opens the transaction drawer
- **WHEN** the owner selects "Kedai Awan Rp 30.000" in "Aktivitas terbaru"
- **THEN** the transaction drawer SHALL open for that row with its provenance, "Ubah", "Tandai sudah dicek" where allowed and "Hapus"
- **AND** closing it SHALL return focus to the row, and a delete confirmed there SHALL refresh Beranda's figures

#### Scenario: Row deleted elsewhere
- **GIVEN** the row was deleted in another tab
- **WHEN** the owner selects it on Beranda
- **THEN** the drawer SHALL say "Transaksi ini sudah tidak ada" and Beranda's list SHALL refresh

### Requirement: Kakeibo and Ketahanan Dana Cards
Beranda SHALL show the period's Kakeibo split: "Pengeluaran" as its total over the five spending buckets, then "Tabungan" apart, each with amount, linked to the ledger. Ketahanan Dana SHALL be a full card with months covered, the emergency balance and the target from the emergency-coverage contract. An unknown coverage SHALL stay unknown.

#### Scenario: Unknown essential cost
- **WHEN** the essential monthly cost is unknown
- **THEN** Ketahanan Dana SHALL say that it cannot be computed yet and why, and SHALL NOT show a number of months or a sentinel

#### Scenario: Unclassified link
- **WHEN** "Belum diklasifikasi" holds Rp 40.000
- **THEN** the Kakeibo card SHALL list it with its amount and link to the ledger with `kakeibo=unclassified` for the period

### Requirement: Beranda Loading and Failure
Beranda SHALL show placeholders of the final layout while loading, and when the summary request fails it SHALL say that the figures could not be loaded and offer a retry, without showing any amount.

#### Scenario: Summary request fails
- **WHEN** `GET /api/dashboard/cycle-summary` fails
- **THEN** the summary, KPI strip and "Uang siklus ini" SHALL show the failure message and "Coba lagi", SHALL show no Rp 0, and cards with their own data SHALL still render

### Requirement: Cycle Summary API
`GET /api/dashboard/cycle-summary` SHALL return, for the authenticated owner only, the Beranda period, keluar, spending, fresh saving, masuk, waiting, scheduled, remaining, safe-to-spend and net-worth figures as integer rupiah, each with explanation lines that add up to it, plus status and reason codes for unavailable figures. It SHALL accept session or Bearer authentication.

#### Scenario: Lines add up
- **WHEN** the summary is returned
- **THEN** for every figure that has lines, the lines SHALL add up exactly to the figure, `spending + fresh_saving` SHALL equal `keluar`, and `remaining` SHALL equal `masuk - keluar - scheduled`

#### Scenario: Unavailable figure
- **WHEN** the expected salary date is unknown
- **THEN** `safe_to_spend.status` SHALL be `unavailable` with `reason` `no_expected_salary`, `per_day` SHALL be `null`, and no field SHALL hold a placeholder number

#### Scenario: Another owner's data
- **GIVEN** two owners with transactions on the same day
- **WHEN** one owner requests the summary
- **THEN** no figure or line SHALL include the other owner's rows, plans, goals or accounts

#### Scenario: Unauthenticated
- **WHEN** the request has no valid session or Bearer key
- **THEN** the API SHALL respond 401 and return no figures

### Requirement: Beranda KPI Cards
Beranda SHALL open with KPI cards instead of a written summary: "Aman dipakai sampai gajian" as the largest figure, then Masuk, Pengeluaran, Ditabung & investasi and Kekayaan bersih. Each card SHALL carry a short label, the figure in large bold tabular type and at most one muted line, and each figure SHALL open its explanation. No card SHALL compare with the same day of an earlier period.

#### Scenario: Cards on a desktop
- **WHEN** the owner opens Beranda at 1440 px
- **THEN** five KPI cards SHALL be shown in that order above the other cards, the safe amount SHALL be the largest figure, and no sentence-style summary SHALL be shown

#### Scenario: No same-date comparison
- **WHEN** the previous period had less spending in its first days
- **THEN** no card SHALL show a chip such as "vs 6 hari pertama periode lalu"

#### Scenario: Figure unavailable
- **WHEN** the net worth cannot be computed
- **THEN** its card SHALL say so in words and SHALL show no Rp 0

#### Scenario: Phone order
- **WHEN** Beranda is shown at 390 px
- **THEN** the safe amount SHALL come first, the other four figures SHALL follow as a two-by-two grid, and "Perlu tindakan" SHALL come next
