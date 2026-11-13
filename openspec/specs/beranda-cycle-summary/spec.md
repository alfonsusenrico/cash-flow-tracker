# beranda-cycle-summary Specification

## Purpose
Beranda tells the owner, in a few checkable numbers, where the current period stands: what went out, what is safe to spend today, what still waits for an answer, and what to do first, on desktop and on phones.

## Requirements

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
Beranda's hero figure SHALL be "Aman dipakai": the money in daily-money pockets, minus everything owed on credit accounts, minus the plans and set-asides paid from daily money before the next salary, never below Rp 0 and never a per-day or per-week amount. Its line SHALL give the days left and the expected salary date in an open cycle with that date after today, and the reason otherwise. The API's `per_day` SHALL divide this amount, unavailable with a reason otherwise.

#### Scenario: Available
- **GIVEN** daily-money pockets holding Rp 4.420.334, Spaylater owing Rp 3.046.014, no plan due, today 13 November and an expected salary date of 30 November
- **WHEN** the owner opens Beranda
- **THEN** the hero SHALL read Rp 1.374.320 with the line "17 hari lagi sampai gajian 30 Nov", and no per-day amount SHALL be shown

#### Scenario: Next salary date unknown
- **GIVEN** an open cycle whose confirmed receipts do not form a regular pattern
- **WHEN** the owner opens Beranda
- **THEN** the hero SHALL still show the safe amount with its Rincian lines and its line SHALL say "Tanggal gaji berikutnya belum diketahui" instead of a day count

#### Scenario: Salary is late
- **GIVEN** the expected salary date was yesterday and no new receipt is confirmed
- **WHEN** the owner opens Beranda
- **THEN** the hero SHALL show the safe amount and its line SHALL say that the salary has not been recorded yet, and no negative or zero day count SHALL be used

#### Scenario: Spent more than the remaining amount
- **WHEN** credit owed plus scheduled exceeds daily money by Rp 300.000
- **THEN** the hero SHALL show Rp 0 and its line SHALL say "Kurang Rp 300.000 untuk tagihan kredit dan rencana", instead of showing a negative safe amount

#### Scenario: Money carried over and returned counts
- **GIVEN** Rp 398.836 left in daily pockets from before the salary and Rp 300.000 returned by friends for last cycle's spending
- **WHEN** the safe amount is computed
- **THEN** both SHALL be part of daily money, because the figure reads balances, not the cycle's flows

#### Scenario: Savings and investment cash are not daily money
- **WHEN** the owner holds Rp 4.531.291 in a `tabungan` pocket and Rp 100.000 in an RDN (`dana_investasi`)
- **THEN** neither SHALL count in daily money

### Requirement: Scheduled Amount
"Terjadwal" SHALL be the unpaid occurrences of active non-income plans due up to the day before the expected salary date, plus the unfunded part of this period's goal set-asides. Paid, linked or skipped occurrences, plans paid from savings or investment cash, and transfer plans into credit accounts SHALL NOT count.

#### Scenario: Linked payment leaves Terjadwal
- **GIVEN** a Rp 600.000 plan from a daily pocket due on 5 November inside the window
- **WHEN** its payment is linked to the plan
- **THEN** "Terjadwal" SHALL fall by Rp 600.000 and the pocket's balance by the payment, so the safe amount does not count it twice

#### Scenario: Goal set-aside already funded
- **GIVEN** a goal with a Rp 600.000 set-aside for the period and Rp 400.000 already moved into its pocket this period
- **WHEN** the scheduled amount is computed
- **THEN** the goal SHALL contribute Rp 200.000 and its explanation line SHALL show both amounts

#### Scenario: Without an expected salary date
- **WHEN** the expected salary date is unknown
- **THEN** "Terjadwal" SHALL cover the next 30 days, and its explanation SHALL name that window

#### Scenario: Credit-bill plan is not counted twice
- **GIVEN** SPayLater owing Rp 470.000 and a Rp 470.000 transfer plan Jago → SPayLater due in the window
- **WHEN** the safe amount is computed
- **THEN** the plan SHALL contribute Rp 0 to "Terjadwal" and be shown as already in "Tagihan kredit", and "Aman dipakai" SHALL be lower by Rp 470.000 only once

#### Scenario: Credit-bill plan for opening debt
- **GIVEN** Kredivo opened owing Rp 300.000 with nothing paid, and two Rp 200.000 transfer plan occurrences into Kredivo in the window
- **WHEN** the safe amount is computed
- **THEN** neither occurrence SHALL count in "Terjadwal", and the Rp 300.000 SHALL be subtracted once through "Tagihan kredit"

#### Scenario: Plan paid from savings
- **GIVEN** a Rp 250.000 insurance plan whose source is a `tabungan` pocket, due in the window
- **WHEN** the safe amount is computed
- **THEN** it SHALL NOT count in "Terjadwal"

### Requirement: Kerjakan Dulu
Under "Perlu tindakan" Beranda SHALL list the first three of what needs the owner, ranked: open questions, plans and credit bills past their due date, those due today, awaiting transfers, ambiguous pairs, groups that need an answer, automatically linked plans to check, and plans and credit bills due within two days. Each row SHALL name the item, its kind and time and its amount where it has one, and the whole row SHALL be its one action.

#### Scenario: Items and order
- **GIVEN** one open question, two awaiting transfer legs, one group that needs an answer and a plan due tomorrow
- **WHEN** the owner opens Beranda
- **THEN** the header SHALL show "Buka Tinjau · 4", and the rows SHALL be the question, then the awaiting transfers, then the group
- **AND** each review row SHALL open Tinjau at the matching item, and the plan due tomorrow SHALL be reached through the bell and Target & Tagihan

#### Scenario: Due plans come before checks and pairing
- **GIVEN** one open question, an awaiting transfer leg, four automatically linked plans, a plan past its due date and a plan due today
- **WHEN** the owner opens Beranda
- **THEN** the rows SHALL be the question, the overdue plan and the plan due today, and "N rencana ditautkan otomatis" and the pairing row SHALL be reached through "Buka Tinjau"

#### Scenario: One-column Beranda
- **WHEN** Beranda is shown at any width, including one column below 1200 px
- **THEN** "Perlu tindakan" SHALL follow the hero directly and come before "Aktivitas terbaru" in the visual, reading and focus order

#### Scenario: Recording a due plan
- **WHEN** the owner selects a plan due today in "Perlu tindakan"
- **THEN** "Rencana jatuh tempo" SHALL open with the same link, skip and record actions as before the redesign, same-amount captures SHALL be offered for linking first, and nothing SHALL be posted without the owner's explicit confirmation

#### Scenario: Nothing to do
- **WHEN** nothing waits and no plan is due
- **THEN** the section SHALL say "Tidak ada yang perlu dijawab." in one line, and its header link SHALL read "Buka Tinjau" without a count

#### Scenario: Credit bill ranked with plans
- **GIVEN** an open question, a SPayLater bill due today with Rp 434.000 left and a plan due today
- **WHEN** the owner opens Beranda
- **THEN** the rows SHALL be the question, then the plan and the bill due today ordered by due date and then name, the bill reading "Tagihan SPayLater", "Jatuh tempo hari ini · Kredit" and Rp 434.000

### Requirement: Recent Activity on Beranda
Under "Aktivitas terbaru" Beranda SHALL list up to eight transactions from today and yesterday that do not wait for an answer, newest first, under one heading per day. Each row SHALL show the time, the title (the note, else the merchant, else the category) or a movement's note or route, the account or route, the pillar, status or saving direction, and the amount with its sign and type colour, expenses in red, plus a link to all transactions. A row SHALL open the shared transaction dialog.

#### Scenario: Waiting row in the list
- **WHEN** a row from today has an open question
- **THEN** it SHALL NOT be listed under "Aktivitas terbaru", and it SHALL be reachable from "Perlu tindakan" or Tinjau

#### Scenario: Day headings
- **WHEN** today and yesterday both have rows
- **THEN** they SHALL be grouped under "KAMIS, 5 NOVEMBER · HARI INI" and "RABU, 4 NOVEMBER · KEMARIN", and each row SHALL show its time, such as "18.23", and not its date

#### Scenario: Movement row
- **WHEN** a "Pindah Saldo" between two of the owner's accounts is listed
- **THEN** it SHALL appear once, titled by its note with "source → target" as its meta, or by "source → target" when it has no note, with a sky amount, and SHALL show its saving direction ("Ditabung", "Diambil dari tabungan", "Investasi", "Bayar utang") or "—" instead of a pillar, never "Tidak dihitung"
- **AND** a pocket SHALL be named with its parent, for example "Jago · Main Pocket → BCA · ATM"

#### Scenario: Expense and income rows
- **WHEN** an expense of Rp 30.000 and an income of Rp 9.518.400 are listed
- **THEN** the expense SHALL read "−Rp 30.000" in the expense red with its pillar, and the income SHALL read "+Rp 9.518.400" in green with "Uang masuk"

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

#### Scenario: Expense in Perlu tindakan
- **WHEN** "Perlu tindakan" lists an open question about an expense of Rp 187.500
- **THEN** its amount SHALL read "−Rp 187.500" in the expense red through the same shared amount as "Aktivitas terbaru"

### Requirement: Beranda Loading and Failure
Beranda SHALL show static placeholders of the final layout while loading. When the summary request fails, it SHALL say in the hero's place that the figures could not be loaded and offer a retry, without showing any amount, while the blocks with their own data still render.

#### Scenario: Loading
- **WHEN** the summary is still loading
- **THEN** Beranda SHALL be marked busy and show placeholders for the hero, the rows and the panel, without a pulse and without any amount

#### Scenario: Summary request fails
- **WHEN** `GET /api/dashboard/cycle-summary` fails
- **THEN** the hero's place SHALL show the failure message and "Coba lagi" and no Rp 0
- **AND** "Perlu tindakan", "Aktivitas terbaru" and Ketahanan Dana SHALL still render, and Kekayaan bersih and Kakeibo SHALL be left out

### Requirement: Cycle Summary API
`GET /api/dashboard/cycle-summary` SHALL return, for the authenticated owner only, the Beranda period, keluar, spending, fresh saving, masuk, waiting, scheduled, the safe amount, safe-to-spend and net-worth figures as integer rupiah, each with lines that add up to it, plus status and reason codes for unavailable figures. It SHALL accept session or Bearer authentication.

#### Scenario: Lines add up
- **WHEN** the summary is returned
- **THEN** for every figure that has lines, the lines SHALL add up exactly to the figure, `spending + fresh_saving` SHALL equal `keluar`, and `safe_amount.amount` SHALL equal `max(0, daily_money.total - credit_owed.total - scheduled.total)` with `short_by` holding any shortfall

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

### Requirement: Beranda Hero
Beranda SHALL open with one hero, "Aman dipakai", as the page's only 44 px figure, then one line, a split bar of daily money into Tagihan kredit, Terjadwal and Aman dipakai named with their shares, and the supporting figures Masuk, Pengeluaran and Ditabung & investasi, each one link with one caption. No other Beranda figure SHALL be 33 px or larger, and the hero figure SHALL NOT be a control.

#### Scenario: Hero on a desktop
- **WHEN** the owner opens Beranda at 1440 × 900
- **THEN** the hero SHALL start at `y = 192`, the supporting figures SHALL follow in one row in that order, and each figure SHALL appear once on the page

#### Scenario: Figures add up to Masuk
- **WHEN** daily money is Rp 4.420.334
- **THEN** Tagihan kredit, Terjadwal and the hero figure SHALL add up to Rp 4.420.334 in the split bar, or the hero line SHALL say how much is short

#### Scenario: Screen reader
- **WHEN** a screen reader reaches the split bar
- **THEN** it SHALL read one image named "Uang harian Rp 4.420.334" with each segment and its share, and every supporting figure SHALL be read as text with its label and caption

#### Scenario: Savings decrease
- **WHEN** the period's net fresh saving is −Rp 500.000
- **THEN** Ditabung & investasi SHALL show Rp 500.000 with the caption "Tabungan berkurang"

#### Scenario: Captions on one line
- **WHEN** Beranda is shown from 1024 to 1440 px, also with a ten-digit amount
- **THEN** every supporting-figure caption SHALL stay on one line

#### Scenario: Terjadwal caption
- **WHEN** the scheduled amount includes goal set-asides until 23 November
- **THEN** Terjadwal SHALL show in the split bar and as "Terjadwal sampai 23 Nov" in the Rincian, and SHALL NOT appear as a supporting figure with a caption

#### Scenario: No figure leaves its block
- **WHEN** Beranda is shown at any width from 320 px, including 1024 to 1199 px with the full sidebar
- **THEN** no hero, supporting or panel figure SHALL extend past its block or the page frame, and the page SHALL NOT scroll horizontally

### Requirement: Rincian of the Safe Amount
The hero SHALL carry the page's only "Rincian" control. It SHALL open the calculation of the safe amount: the rule, then Uang harian, Tagihan kredit and Terjadwal with signed amounts, and the total equal to the hero figure. Each line SHALL open its parts in place.

#### Scenario: Lines and total
- **GIVEN** daily money Rp 4.420.334, credit owed Rp 3.046.014 and Terjadwal Rp 0
- **WHEN** the owner activates "Rincian"
- **THEN** a dialog titled "Aman dipakai" SHALL list Uang harian Rp 4.420.334, Tagihan kredit −Rp 3.046.014 and Terjadwal −Rp 0, and its total "Aman dipakai" SHALL be Rp 1.374.320, equal to the hero

#### Scenario: A line opens its parts
- **WHEN** the owner opens a line in the dialog
- **THEN** Uang harian SHALL list every daily-money pocket with its parent and balance, largest first
- **AND** Tagihan kredit SHALL list every credit account with what it owes and its next due date
- **AND** Terjadwal SHALL show each bill with its due date and each goal set-aside with its monthly amount and what is already funded

#### Scenario: Period over
- **GIVEN** daily money Rp 1.000.000, credit owed Rp 1.200.000 and Terjadwal Rp 100.000
- **WHEN** the owner activates "Rincian"
- **THEN** after the three lines the dialog SHALL read "Kurang" Rp 300.000, then "Aman dipakai" Rp 0, equal to the hero
- **AND** no negative "Aman dipakai" SHALL be shown

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

### Requirement: Credit Figures in the Cycle Summary
The cycle summary SHALL return `spending.credit` with two figures. `spent_on_credit` is the period's reportable expenses on credit accounts, a part of `spending.amount`. `paid_to_credit` is the cash moved into credit accounts in the period, from accounts that are not credit. Each SHALL have per-account lines with `label_source` `user`. Neither SHALL change Pengeluaran, Keluar, Masuk, Ditabung & investasi or Aman dipakai.

#### Scenario: Purchase and payment in one cycle
- **GIVEN** Pengeluaran of Rp 1.000.000 including a Rp 120.000 SPayLater dinner, and a Rp 470.000 movement Jago → SPayLater in the cycle
- **WHEN** the cycle summary is read
- **THEN** `spent_on_credit` SHALL be Rp 120.000, `paid_to_credit` Rp 470.000, and Pengeluaran SHALL stay Rp 1.000.000

#### Scenario: No credit accounts
- **WHEN** an owner has no credit account
- **THEN** both figures SHALL be 0 with no lines, and every other figure SHALL equal its value before this change

#### Scenario: Payment from a savings account
- **GIVEN** a credit account and a role-`tabungan` pocket
- **WHEN** the owner moves Rp 200.000 from that pocket into the credit account
- **THEN** `paid_to_credit` SHALL grow by Rp 200.000, and "Ditabung & investasi" SHALL fall by Rp 200.000, as for any money leaving savings

### Requirement: Supporting Figures Open Their Detail
Masuk, Pengeluaran and Ditabung & investasi SHALL link to Transaksi with Beranda's period (`period=cycle` in a salary cycle, else `period=month`) and `type` `income`, `expense` or `saving`. Each link's accessible name SHALL name the figure and where it goes.

#### Scenario: Opening Pengeluaran
- **WHEN** the owner selects Pengeluaran Rp 5.395.841 in a salary cycle
- **THEN** Transaksi SHALL open at `/ledger?period=cycle&type=expense` and its Pengeluaran SHALL read Rp 5.395.841

#### Scenario: Opening Ditabung & investasi
- **WHEN** the owner selects Ditabung & investasi
- **THEN** Transaksi SHALL open with `type=saving` for the same period and its hero SHALL show the same Ditabung & investasi

#### Scenario: Keyboard
- **WHEN** the owner tabs to a supporting figure and presses Enter
- **THEN** it SHALL show a visible focus ring and open its target

### Requirement: Credit Bills Figure
When the owner has an active credit account, Beranda SHALL show "Tagihan kredit" ("Credit bills"): the sum of what the credit accounts owe, with the caption "jatuh tempo {date}" for the nearest unpaid bill across accounts. It SHALL equal the credit line subtracted in Aman dipakai, and its dialog SHALL say so.

#### Scenario: Two credit accounts
- **GIVEN** Spaylater owes Rp 3.046.014 with a bill due 11 December and Kredivo owes Rp 2.200.000 with a bill due 20 November of Rp 400.000
- **WHEN** the owner opens Beranda on 13 November
- **THEN** "Tagihan kredit" SHALL read Rp 5.246.014 with "jatuh tempo 20 Nov", the same amount Rincian subtracts, and its dialog SHALL read "Sudah dikurangi dari Aman dipakai"

#### Scenario: No credit account
- **WHEN** the owner has no active credit account
- **THEN** Beranda SHALL show no "Tagihan kredit", and the safe amount SHALL subtract nothing for credit

#### Scenario: Nothing owed
- **WHEN** every credit account owes Rp 0
- **THEN** the figure SHALL read Rp 0 with "tidak ada tagihan"

#### Scenario: Hidden balances
- **WHEN** "Sembunyikan saldo" is on
- **THEN** the figure and its caption amount SHALL be hidden like every other amount
