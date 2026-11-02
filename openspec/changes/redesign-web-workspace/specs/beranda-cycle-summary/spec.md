# Spec Delta

## Purpose

Beranda tells the owner, in a few checkable numbers, where the current period stands: what went out, what is safe to spend today, what still waits for an answer, and what to do first, on desktop and on phones.

## ADDED Requirements

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

### Requirement: Three-Number Summary
Beranda SHALL open with a short written summary that contains exactly three explainable numbers: the period's keluar, the safe-to-spend figure for today and the amount waiting for an answer. Each number SHALL open its explanation. A sentence whose figure is unavailable or zero SHALL say so in words instead of showing Rp 0.

#### Scenario: Normal day
- **GIVEN** keluar Rp 3.164.500, a safe-to-spend figure of Rp 214.000 and 4 rows waiting worth Rp 581.000
- **WHEN** the owner opens Beranda at 1440 px
- **THEN** the summary SHALL contain these three amounts as underlined controls and no other amount, and activating each SHALL open the explanation of that figure

#### Scenario: Nothing waiting
- **WHEN** no row waits for an answer
- **THEN** the third sentence SHALL say that every transaction is settled and SHALL contain no amount

#### Scenario: Safe-to-spend unavailable
- **WHEN** the safe-to-spend figure is unavailable
- **THEN** the second sentence SHALL say why in words (for example that the next salary date is not known yet), SHALL show no daily amount and SHALL still offer an explanation

### Requirement: Keluar Figure
Beranda "Keluar" SHALL be the period's Kakeibo total: the five pillars including net fresh saving, plus "Belum diklasifikasi", net of applied reimbursements. Savings transfers and investment top-ups therefore count inside it, as on Analisis. Its explanation SHALL list each bucket with its amount and link each to the ledger filtered by period and pillar.

#### Scenario: Saving counts in Keluar
- **GIVEN** in the cycle Rp 1.420.000 need, Rp 610.000 want, Rp 245.000 culture, Rp 89.500 unexpected, Rp 800.000 net fresh saving and Rp 0 unclassified
- **WHEN** the owner opens the "Keluar" explanation
- **THEN** the six lines SHALL add up to Rp 3.164.500, the saving line SHALL say that it is moved to pockets or invested, and the total SHALL equal the Analisis Kakeibo total for the same period

#### Scenario: Internal movement is not Keluar
- **WHEN** the owner moves Rp 500.000 from a bank to an e-wallet that is neither a goal account nor an investment
- **THEN** "Keluar" SHALL NOT change, and its explanation SHALL say that moves between the owner's own accounts are not counted

### Requirement: Waiting-for-Answer Figure
The third number SHALL be the total amount of rows that count towards the Tinjau attention badge in the period. Its explanation SHALL list them by Tinjau section and SHALL state, per line, whether the amount is already inside "Keluar" and under which bucket. Waiting rows SHALL stay recorded and counted exactly as the ledger counts them.

#### Scenario: Question row is already counted
- **GIVEN** a merchant-less Rp 45.000 payment with an open "Apa ini?" question and a one-sided Rp 112.000 transfer leg awaiting its counterpart
- **WHEN** the owner opens the explanation of the waiting figure
- **THEN** it SHALL show Rp 157.000 in two lines, SHALL say that Rp 45.000 is already inside "Keluar" as "Belum diklasifikasi" and that Rp 112.000 is not inside "Keluar" yet, and SHALL link to Tinjau

#### Scenario: Answering changes the bucket, not the total
- **WHEN** the owner answers the Rp 45.000 question as "Makan & minum" (need)
- **THEN** "Keluar" SHALL stay the same, "Belum diklasifikasi" SHALL fall by Rp 45.000, need SHALL rise by Rp 45.000 and the waiting figure SHALL fall by Rp 45.000

### Requirement: Safe-to-Spend Today
"Aman dipakai hari ini" SHALL be available only in an open salary cycle with an expected next salary date after today (`salary-cycles`). It SHALL be the remaining amount (masuk less keluar less scheduled) divided by the days from today up to the day before the expected date, floored to Rp 1.000 and never below zero. Otherwise it SHALL be unavailable with a reason.

#### Scenario: Available
- **GIVEN** masuk Rp 9.800.000, keluar Rp 3.164.500, scheduled Rp 1.713.500, today 2 November and an expected salary date of 25 November
- **WHEN** the owner opens the safe-to-spend explanation
- **THEN** it SHALL show the remaining Rp 4.922.000, the 23 days from 2 to 24 November, the result Rp 214.000 per day after rounding down to Rp 1.000, and the basis of the expected date

#### Scenario: Next salary date unknown
- **GIVEN** an open cycle whose confirmed receipts do not form a regular pattern
- **WHEN** the owner opens Beranda
- **THEN** no daily amount SHALL be shown, the remaining amount SHALL still be shown with its lines, and the explanation SHALL say that a daily figure needs a regular salary date

#### Scenario: Salary is late
- **GIVEN** the expected salary date was yesterday and no new receipt is confirmed
- **WHEN** the owner opens Beranda
- **THEN** the daily figure SHALL be unavailable with the reason that the salary has not been recorded yet, and no negative or zero day count SHALL be used

#### Scenario: Spent more than the remaining amount
- **WHEN** keluar plus scheduled exceeds masuk by Rp 300.000 with 10 days left
- **THEN** the daily figure SHALL be Rp 0 and its explanation SHALL say that the cycle is Rp 300.000 over, without a negative daily amount

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
Beranda SHALL show "Uang siklus ini": masuk for the period and a segmented bar of "Udah dipakai" (keluar), "Terjadwal" and "Aman dipakai" (remaining). Each segment SHALL carry its amount as text with a label beside it, every amount SHALL be explainable, and the bar SHALL NOT be the only carrier of the values.

#### Scenario: Segments add up
- **WHEN** the card shows masuk Rp 9.800.000
- **THEN** the three labelled amounts SHALL add up to Rp 9.800.000, or the card SHALL say by how much the period is over when they exceed it

#### Scenario: Screen reader
- **WHEN** a screen reader reaches the card
- **THEN** it SHALL read each segment's name and amount as text, and the decorative bar SHALL be hidden from it

### Requirement: Kerjakan Dulu
Beranda SHALL list what needs the owner first: open questions, awaiting or ambiguous transfers, groups that need an answer, and plan occurrences that are due or due within two days. Each item SHALL name the merchant or plan, amount and time and offer one action. A due plan SHALL keep its link, skip and record actions.

#### Scenario: Items and order
- **GIVEN** one open question, two awaiting transfer legs, one group that needs an answer and a plan due tomorrow
- **WHEN** the owner opens Beranda
- **THEN** "Kerjakan dulu" SHALL show a count of 4 Tinjau items, list the question first and the plan last, open Tinjau at the matching item from each review action, and show "Buka Tinjau"

#### Scenario: Recording a due plan
- **WHEN** the owner chooses to record a due plan from "Kerjakan dulu"
- **THEN** the same confirmation as before the redesign SHALL appear, same-amount captures SHALL be offered for linking first, and nothing SHALL be posted without the owner's explicit confirmation

#### Scenario: Nothing to do
- **WHEN** nothing waits and no plan is due
- **THEN** the card SHALL say "Tinjau udah beres" and SHALL show the count of rows recorded automatically today with an optional link to check them

### Requirement: Recent Activity on Beranda
Beranda SHALL list up to eight transactions from today and yesterday, newest first, with merchant or movement, account, time, Kakeibo bucket or status, provenance and amount in its semantic colour, and a link to all transactions. Selecting a row SHALL open its detail.

#### Scenario: Waiting row in the list
- **WHEN** a row with an open question is listed
- **THEN** it SHALL show "Nunggu jawaban" as its status instead of a pillar and SHALL show its amount

#### Scenario: Movement row
- **WHEN** a "Pindah Saldo" between two of the owner's accounts is listed
- **THEN** it SHALL appear once as "source → target" with a sky amount, and SHALL say "Nggak dihitung" instead of a pillar

### Requirement: Kakeibo and Ketahanan Dana Cards
Beranda SHALL show the period's Kakeibo split with the total equal to "Keluar", all six buckets with amount and share, each linked to the ledger, and Ketahanan Dana with months covered, the emergency balance and the target from the emergency-coverage contract. An unknown coverage SHALL stay unknown.

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
`GET /api/dashboard/cycle-summary` SHALL return, for the authenticated owner only, the Beranda period, keluar, masuk, waiting, scheduled, remaining, safe-to-spend and net-worth figures as integer rupiah, each with explanation lines that add up to it, plus status and reason codes for unavailable figures. It SHALL accept session or Bearer authentication.

#### Scenario: Lines add up
- **WHEN** the summary is returned
- **THEN** for every figure that has lines, the lines SHALL add up exactly to the figure, and `remaining` SHALL equal `masuk - keluar - scheduled`

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
