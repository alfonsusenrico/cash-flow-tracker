# credit-accounts Specification

## Purpose
Credit accounts such as paylater and credit cards: a top-level account whose negative balance is the amount owed. Purchases on it are spending when made, paying the bill is a movement into it, a bill fee is its own "Biaya kredit" expense, and what it owes counts as utang in net worth.

## Requirements

### Requirement: Credit Account Type
An owner SHALL be able to create and edit a top-level account of type `credit` ("Kredit") with an amount owed today (stored as a non-positive `initial_balance`), an optional `credit_limit` above 0, and an optional `payment_day` and `statement_day`, each from 1 to 31. A credit account SHALL NOT have pockets or a parent, and only a credit account SHALL accept a negative `initial_balance`, a limit, a payment day or a statement day.

#### Scenario: Adding SPayLater
- **WHEN** the owner adds "SPayLater" as Kredit with "Utang saat ini" Rp 350.000, limit Rp 2.000.000 and payment day 11
- **THEN** the account SHALL be stored with `initial_balance` −350000, `credit_limit` 2000000 and `payment_day` 11, and its tile in Rekening & Kantong SHALL show −Rp 350.000 owed and "Sisa limit Rp 1.650.000"

#### Scenario: Pocket under a credit account
- **WHEN** a request creates an account whose parent is a credit account
- **THEN** it SHALL be refused with 422 and no account SHALL be created

#### Scenario: Negative balance on a bank account
- **WHEN** a request creates a bank account with `initial_balance` −1000 or with a `credit_limit`
- **THEN** it SHALL be refused with 422

#### Scenario: Another owner's credit account
- **WHEN** one owner edits or reads another owner's credit account
- **THEN** the request SHALL be answered 404 and nothing SHALL change

#### Scenario: Bill day
- **WHEN** the owner sets `statement_day` 1 on SPayLater, or sends `statement_day` 1 for a bank account
- **THEN** SPayLater SHALL return `statement_day` 1, and the bank account request SHALL be refused with 422 `credit_fields_not_allowed`

### Requirement: Credit Purchases
An expense on a credit account SHALL be recorded at its own date with its own category and Kakeibo pillar, SHALL count in Pengeluaran, Kakeibo and "Aman dipakai" like any expense, and SHALL make the amount owed grow. No sufficient-funds check SHALL apply. Going over the limit SHALL show "Melewati limit" on the account and in the save response, and SHALL NOT be refused. An income on a credit account (a refund) SHALL lower the amount owed.

#### Scenario: Dinner paid with SPayLater
- **GIVEN** SPayLater owes Rp 350.000 and the cycle's Pengeluaran is Rp 1.000.000
- **WHEN** the owner records a Rp 120.000 dinner on SPayLater in category Makan
- **THEN** SPayLater SHALL owe Rp 470.000, Pengeluaran SHALL be Rp 1.120.000 with Rp 120.000 under its pillar, "Aman dipakai" SHALL fall by Rp 120.000, and no liquid balance SHALL change

#### Scenario: Over the limit
- **WHEN** a purchase makes SPayLater owe more than its limit
- **THEN** it SHALL be saved, and the response and the account SHALL carry `over_limit: true` shown as "Melewati limit"

### Requirement: Credit Payments and Fees
A movement from a liquid account into a credit account SHALL lower the amount owed and SHALL NOT count as Pengeluaran, Masuk or Ditabung. A movement out of a credit account SHALL raise it. A payment MAY carry a fee, booked as its own expense on the credit account in "Biaya kredit". A one-sided leg in Tinjau SHALL be completable with a credit account as its other side.

#### Scenario: Paying the bill from Bayar Kredit
- **GIVEN** SPayLater owes Rp 470.000 and Jago · Bayar Kredit holds Rp 500.000
- **WHEN** the owner moves Rp 470.000 from Bayar Kredit to SPayLater
- **THEN** SPayLater SHALL owe Rp 0, Bayar Kredit SHALL hold Rp 30.000, and the cycle's Pengeluaran, Masuk and Ditabung SHALL be unchanged

#### Scenario: Payment with interest
- **WHEN** the owner pays Rp 480.000 to SPayLater with a fee of Rp 10.000 and SPayLater owed Rp 470.000
- **THEN** a Rp 10.000 expense in "Biaya kredit" SHALL be booked on SPayLater, SPayLater SHALL owe Rp 0, and Pengeluaran SHALL grow by Rp 10.000 only

#### Scenario: Completing a Jago debit as a credit payment
- **WHEN** a one-sided Jago expense of Rp 470.000 waits in "Pindah saldo belum lengkap" and the owner completes it with SPayLater
- **THEN** one movement Jago → SPayLater SHALL exist and SPayLater SHALL owe Rp 470.000 less

### Requirement: Credit Accounts in Pickers
Pickers for an expense's account, a movement's source and target and a one-sided completion SHALL list credit accounts in their own "Kredit" group. Investment funding, obligation payments and "Cocokkan saldo" SHALL NOT offer credit accounts, and the server SHALL refuse a credit account there.

#### Scenario: Investment funding
- **WHEN** a request funds an investment buy from a credit account
- **THEN** it SHALL be refused with 422 and nothing SHALL be booked

#### Scenario: Cocokkan saldo
- **WHEN** Rekening & Kantong shows a credit account
- **THEN** it SHALL NOT offer "Cocokkan saldo"

### Requirement: Credit Purchase Terms
An expense on a credit account SHALL have terms: paid in full by default, or an `installment_count` from 2 to 36, with an optional first due date set by the owner. Terms SHALL be settable when the purchase is created and changeable or resettable later. They SHALL never change the purchase's amount, date, category, pillar or how Pengeluaran counts it, which stays in full on the purchase date.

#### Scenario: Six instalments at purchase
- **WHEN** the owner records a Rp 1.200.000 purchase on SPayLater with `credit_terms` `installment_count` 6
- **THEN** the purchase SHALL have six instalments of Rp 200.000, and Pengeluaran SHALL grow by Rp 1.200.000 on the purchase date

#### Scenario: Changing terms later
- **WHEN** the owner changes a paid-in-full purchase to 3 instalments and later resets it
- **THEN** its schedule SHALL show 3 instalments and then one, and no figure SHALL change at either step

#### Scenario: Terms on a purchase that is not on credit
- **WHEN** a request sets terms on a bank expense, on an income, or with `installment_count` 40
- **THEN** it SHALL be refused with 422 (`credit_terms_not_credit_purchase` or `credit_terms_count_range`) and nothing SHALL change

#### Scenario: Purchase moved off the credit account
- **WHEN** the owner moves a 3-instalment purchase from SPayLater to a bank account
- **THEN** its terms SHALL be removed in the same save

#### Scenario: Another owner's purchase
- **WHEN** one owner sets terms on another owner's transaction
- **THEN** the request SHALL be answered 404 and nothing SHALL change

### Requirement: Credit Due Dates
A purchase's first due date SHALL be the first `payment_day` on or after its statement close. The statement close is the first `statement_day` (1 when empty) after the purchase's local date, with days clamped to the month. An owner first due date SHALL replace it, and SHALL NOT be before the purchase date. Instalment k SHALL fall k months after the first. Amounts SHALL be whole rupiah, with the remainder on the first instalment.

#### Scenario: SPayLater billed on the 1st, due on the 11th
- **GIVEN** SPayLater with `statement_day` 1 and `payment_day` 11
- **WHEN** purchases are made on 5 November and on 1 December
- **THEN** the first SHALL be due on 11 December and the second on 11 January

#### Scenario: Bill on the 25th, due on the 5th
- **GIVEN** a credit account with `statement_day` 25 and `payment_day` 5
- **WHEN** purchases are made on 20 November and on 25 November
- **THEN** the first SHALL be due on 5 December and the second on 5 January

#### Scenario: Instalment amounts
- **WHEN** a Rp 1.000.000 purchase on 5 November is split into 3 instalments on SPayLater (1, 11)
- **THEN** the instalments SHALL be Rp 333.334 due 11 December, Rp 333.333 due 11 January and Rp 333.333 due 11 February

#### Scenario: Owner corrects the first due date
- **WHEN** the owner sets the first due date of that purchase to 1 December
- **THEN** the instalments SHALL be due on 1 December, 1 January and 1 February, and a first due date before 5 November SHALL be refused with 422 `credit_terms_due_before_purchase`

#### Scenario: No payment day
- **WHEN** a credit account has no `payment_day`
- **THEN** its overview SHALL report `due_dates_unavailable` with no bills, and its owed amount and limit SHALL still be shown

### Requirement: Due Dates Kept After a Day Change
Changing a credit account's `payment_day` or `statement_day` SHALL keep the due date of every purchase whose first due date was on or before today, and SHALL move only bills not yet due. Changing a purchase's date SHALL clear a kept due date. It SHALL also clear an owner due date that falls before the new date, and the response SHALL say so. Changing only a purchase's instalment count SHALL keep its kept due date as kept, not as the owner's.

#### Scenario: Payment day moves from 11 to 5
- **GIVEN** a purchase due and paid on 11 November and a purchase due on 11 December, with today 20 November
- **WHEN** the owner changes the payment day to 5
- **THEN** the first SHALL stay due on 11 November and paid on time, and the second SHALL be due on 5 December

#### Scenario: Purchase date moved past the owner's due date
- **WHEN** the owner moves a purchase with an owner first due date of 1 December to 3 December
- **THEN** the purchase SHALL get its derived due date, and the response SHALL carry `credit_terms_reset: true`

#### Scenario: Count changed on a purchase with a kept due date
- **GIVEN** the first purchase above, kept due on 11 November after the payment day moved to 5
- **WHEN** the owner changes only its instalments from 1 to 3 and saves
- **THEN** `PUT /api/transactions/{id}/credit-terms` SHALL carry `first_due_date` 11 November, the stored terms SHALL keep 11 November with `first_due_date_set_by` `system`, and its instalments SHALL fall on 11 November, 11 December and 11 January

### Requirement: Credit Bills
For each credit account the server SHALL compute bills at read time, one per due date. A bill holds the instalments, fees, cash advances and opening debt due that day, with its amount, paid, left to pay, statement date and status: `open`, `upcoming`, `due_today`, `overdue`, `paid` or `paid_late`. The sum left to pay across bills SHALL equal what the account owes.

#### Scenario: November purchases make the December bill
- **GIVEN** SPayLater (1, 11) owing nothing, a Rp 120.000 dinner on 5 November and a Rp 80.000 purchase on 20 November
- **WHEN** the owner reads SPayLater on 21 November
- **THEN** there SHALL be one bill due 11 December of Rp 200.000 with status `open` and statement date 1 December, and SPayLater SHALL owe Rp 200.000

#### Scenario: Opening debt
- **GIVEN** SPayLater created on 3 November owing Rp 350.000, with payment day 11
- **WHEN** the owner reads SPayLater
- **THEN** a Rp 350.000 opening item SHALL be due on 11 November

#### Scenario: Overdue bill
- **WHEN** the 11 December bill still has Rp 50.000 left on 12 December
- **THEN** its status SHALL be `overdue` with left to pay Rp 50.000

#### Scenario: Bills always add up to what is owed
- **WHEN** any mix of purchases, instalments, fees, refunds, cash advances and payments is recorded, edited or deleted
- **THEN** the bills' left to pay minus any overpayment SHALL equal the account's owed amount exactly

### Requirement: Credit Bills Across Accounts
`GET /api/credit/bills` SHALL list the bills of all the owner's credit accounts, and the credit rows of `GET /api/accounts` SHALL carry `statement_day`, `limit_left` and `next_due`.

#### Scenario: Bills due soon across accounts
- **WHEN** the owner lists bills due by 13 December
- **THEN** SPayLater's 11 December bill and Kredivo's 1 December bill SHALL be listed with their account names and left to pay

### Requirement: Payments Settle the Oldest Due First
Payments into a credit account and refunds on it SHALL settle the account's items in due-date order, then by purchase date, without the owner linking anything. A partial payment SHALL settle what it covers. A payment beyond what is due SHALL settle later instalments early. A payment beyond everything owed SHALL be kept as an overpayment for later items. A refund SHALL never count as cash paid to credit.

#### Scenario: Paying the December bill in two parts
- **GIVEN** the Rp 200.000 bill due 11 December
- **WHEN** the owner moves Rp 120.000 on 10 December and Rp 80.000 on 13 December into SPayLater
- **THEN** the dinner SHALL be settled on 10 December on time, the Rp 80.000 purchase on 13 December late, and the bill SHALL be `paid_late`

#### Scenario: Early payment in full of six instalments
- **GIVEN** six Rp 200.000 instalments due from 11 December
- **WHEN** the owner pays Rp 1.200.000 on 15 November
- **THEN** every instalment SHALL be settled on 15 November, on time, and SPayLater SHALL owe nothing

#### Scenario: Payment with a fee
- **WHEN** the owner pays Rp 480.000 with a Rp 10.000 fee and Rp 470.000 was due
- **THEN** the fee and the Rp 470.000 bill SHALL both be settled by that payment

### Requirement: Settlement Facts per Item
Each purchase, instalment and fee SHALL expose its due date, the date it was settled, the days from the purchase to that date, and whether it was settled by its due date. These SHALL be data only: the server SHALL NOT score, rank or advise on them.

#### Scenario: Dinner settled after 35 days
- **WHEN** the 5 November dinner is settled by a payment on 10 December
- **THEN** it SHALL report `settled_on` 10 December, `days_to_settle` 35 and `settled_on_time` true

#### Scenario: Not yet settled
- **WHEN** an instalment is not settled
- **THEN** `settled_on`, `days_to_settle` and `settled_on_time` SHALL be null

### Requirement: Credit Facts on Ledger Rows
Ledger rows SHALL carry a `credit` block. It is null unless the row is on a credit account or is a movement into or out of one. The block SHALL give the role (`purchase`, `fee`, `refund`, `payment` or `cash_advance`) and the account. A purchase SHALL also give its instalment count, instalments settled, next due date, left to pay and settlement facts. The account filter SHALL be the per-provider view.

#### Scenario: Purchase row
- **WHEN** the ledger lists the unpaid 5 November dinner
- **THEN** its `credit` block SHALL be `purchase`, SPayLater, one instalment, none settled, next due 11 December, left to pay Rp 120.000

#### Scenario: Payment row
- **WHEN** the ledger lists the movement Jago → SPayLater
- **THEN** its `credit` block SHALL be `payment` with SPayLater, and the row SHALL stay one movement row

#### Scenario: Per-provider view
- **WHEN** the owner filters the ledger by SPayLater
- **THEN** it SHALL list SPayLater's purchases, fees, refunds, cash advances and payments only

#### Scenario: Ordinary row
- **WHEN** the ledger lists a bank expense
- **THEN** its `credit` block SHALL be null

### Requirement: Credit Account Overview
`GET /api/credit/accounts/{id}` SHALL return the account's owed amount, limit, limit left (which may be negative), overpayment, days, next due bill, its bills with their items, and period totals (spending on credit, fees, refunds, cash paid in, items settled and settled on time) over local days (`from`, `to`) or the ledger list's window (`period` and `period_offset`, or `from_date` and `to_date`) as `GET /api/transactions` resolves it.

#### Scenario: Overview totals
- **GIVEN** this cycle: Rp 200.000 spent on SPayLater, a Rp 10.000 fee, and Rp 480.000 paid from Jago
- **WHEN** the owner reads SPayLater's overview for the cycle
- **THEN** the totals SHALL be spending on credit Rp 210.000, fees Rp 10.000 and cash paid Rp 480.000

#### Scenario: The ledger list's window
- **GIVEN** a cycle that started with the salary received at 08.02 on 27 October, SPayLater spending of Rp 459.000 so far, a Rp 20.000 purchase at 07.30 on 27 October and a Rp 77.000 purchase timed 23.00 today while it is 12.39
- **WHEN** the owner reads SPayLater's overview with `period=cycle&period_offset=0`
- **THEN** spending on credit SHALL be Rp 459.000, the same as the `type=credit` list for that account and window, and the overview with `from=2026-10-27&to=<today>` SHALL still count whole local days

#### Scenario: Two period forms at once
- **WHEN** the overview is asked with `period=cycle` and `from=2026-11-01`, or with `period=month` and `to_date`
- **THEN** it SHALL be refused with 422 `period_with_dates`

#### Scenario: Not a credit account
- **WHEN** the overview is asked for a bank account, or for another owner's credit account
- **THEN** it SHALL be refused with 422 `credit_account_required`, or answered 404

#### Scenario: Error in the account's language
- **WHEN** an English account sends terms on a bank expense
- **THEN** the 422 SHALL carry `code` `credit_terms_not_credit_purchase` and an English `message`, and the code SHALL exist in both server catalogues

### Requirement: Credit Account Tile
A credit tile SHALL show Utang, "Sisa limit {amount}" ("Limit left") or "Melewati limit", and the next bill "jatuh tempo {date} · {left}".

#### Scenario: SPayLater with an open bill
- **GIVEN** SPayLater owes Rp 893.000 with a limit of Rp 3.000.000, and its next bill is due today with Rp 434.000 left to pay
- **WHEN** the owner opens Rekening & Kantong
- **THEN** its tile SHALL show −Rp 893.000, "Sisa limit Rp 2.107.000" and "jatuh tempo hari ini · Rp 434.000", from `credit_owed`, `limit_left` and `next_due`

#### Scenario: Over the limit
- **WHEN** a credit account's `over_limit` is true
- **THEN** its tile SHALL read "Melewati limit" in words instead of the limit left

#### Scenario: No due day
- **WHEN** a credit account has no `payment_day`
- **THEN** its tile SHALL say that the due day is not set, and SHALL show no due date

### Requirement: Credit Account Detail
The credit dialog SHALL list the bills per due date with their amount, paid, left and status (open, upcoming, due today, overdue, paid, paid late), and link to Transaksi filtered by the account.

#### Scenario: Bills of SPayLater
- **WHEN** the owner opens SPayLater's detail
- **THEN** it SHALL read `GET /api/credit/accounts/{id}`, list each bill with its due date, amount, paid, left to pay and status word, and "Lihat di Transaksi" SHALL open `/ledger?account={id}`

#### Scenario: Items of a bill
- **WHEN** the owner expands the bill due 11 December
- **THEN** each item SHALL show its name or kind, "cicilan k dari n" for an instalment, its amount, and once settled its settled date, days to settle and whether it was paid on time

#### Scenario: Cycle facts
- **WHEN** the detail is shown
- **THEN** it SHALL list the limit, the limit left, the next bill, both credit days, "Pengeluaran pakai kredit", "Tagihan kredit dibayar" and "Dilunasi tepat waktu" for the current cycle, and SHALL offer no "Cocokkan saldo", "Sesuaikan saldo" or "Tambah kantong"

#### Scenario: Bills unavailable
- **WHEN** the overview request fails, or the account has no due day
- **THEN** the detail SHALL say so with a retry or "Ubah kredit", and the rest of the detail SHALL stay usable

### Requirement: Credit Days in the Account Form
The account form SHALL edit "Tagihan terbit tiap tgl" and "Jatuh tempo tiap tgl", and SHALL say that past bills keep their due dates.

#### Scenario: Setting both days
- **WHEN** the owner sets "Tagihan terbit tiap tgl" 25 and "Jatuh tempo tiap tgl" 5 on SPayLater and saves
- **THEN** the request SHALL carry `statement_day` 25 and `payment_day` 5, and the form SHALL have shown "Tagihan yang sudah terbit tetap memakai tanggal jatuh temponya."

#### Scenario: Day out of range
- **WHEN** the owner enters 32 in either day
- **THEN** the form SHALL name the error under that field and SHALL send nothing

#### Scenario: Not a credit account
- **WHEN** the owner edits a bank account
- **THEN** the form SHALL NOT show the credit days, and the request SHALL carry neither day

### Requirement: Credit Bills Beside Plans
"Tagihan 30 hari" SHALL list unpaid credit bills from `GET /api/credit/bills` as "Tagihan {account}" with the due date and what is left. It SHALL mark them "sudah dihitung di Pengeluaran" ("already counted in Spending") and SHALL NOT add them to Terjadwal. A credit-bill plan SHALL show its full amount and the part counted elsewhere.

#### Scenario: Unpaid SPayLater bill
- **GIVEN** `GET /api/credit/bills?from={today}&to={window_end}&status=open,upcoming,due_today,overdue` returns a bill with `account_name` "SPayLater", `due_date` 11 December and `left_to_pay` 434000, and no plan pays it
- **WHEN** the owner opens `/goals`
- **THEN** "Tagihan 30 hari ke depan" SHALL list "Tagihan SPayLater" on 11 Des with Rp 434.000 and "sudah dihitung di Pengeluaran", and the section's count and total and Beranda's Terjadwal SHALL not include it

#### Scenario: Credit-bill plan
- **GIVEN** a plan line for SPayLater with `plan_amount` 434000, `counted_elsewhere` 434000, `credit_account_id` of SPayLater and the bill's due date
- **WHEN** the owner opens `/goals`
- **THEN** one row SHALL be shown for that bill, the plan's, reading Rp 434.000 with "Rp 434.000 sudah dihitung di Pengeluaran", and the bill SHALL not be listed a second time

#### Scenario: Bills unavailable
- **WHEN** `GET /api/credit/bills` fails or lists an account under `due_dates_unavailable`
- **THEN** the plan rows SHALL still be listed, a failure SHALL be named in one line with "Coba lagi", and an account without due dates SHALL be named with "tanggal jatuh tempo belum diisi"

### Requirement: Credit in Utang & kredit
Each credit account SHALL show its Utang and its next due bill.

#### Scenario: Credit row
- **GIVEN** SPayLater with `credit_owed` 893000, `limit_left` 2107000 and `next_due` {`due_date` today, `left_to_pay` 434000}
- **WHEN** the owner opens `/goals`
- **THEN** "Utang & kredit" SHALL show the row "SPayLater · kredit" with what it owes, a bar of the limit used, "Jatuh tempo hari ini · sisa Rp 434.000" in the warning text and "Sisa limit Rp 2.107.000", and the row SHALL open the account's detail dialog in place

#### Scenario: Over the limit and no limit
- **WHEN** a credit account has `over_limit` true, or no `credit_limit`
- **THEN** its row SHALL read "Melewati limit" in the expense text, or show no bar and no limit line

### Requirement: Due Credit Bills Need Action
Beranda's "Perlu tindakan" and the bell SHALL list overdue credit bills, then bills due today, then bills due within two days, ranked with plans of the same urgency. Selecting one SHALL open Catat's Perpindahan into that account with the amount left.

#### Scenario: Paying from Perlu tindakan
- **GIVEN** an overdue BCA credit card bill with `left_to_pay` 1250000
- **WHEN** the owner selects it in "Perlu tindakan"
- **THEN** Perpindahan SHALL open with the card as "Ke" and Rp 1.250.000 filled, "Dari" empty, and nothing SHALL be recorded until the owner saves

#### Scenario: Bill already paid
- **WHEN** a bill's `left_to_pay` is 0 or its status is `paid` or `paid_late`
- **THEN** it SHALL appear neither in "Perlu tindakan" nor in the bell

### Requirement: Credit Lines in the Pengeluaran Breakdown
Beranda's Rincian of Pengeluaran SHALL show "Pengeluaran pakai kredit" as a part of Pengeluaran, and "Tagihan kredit dibayar" as cash out that is not counted again.

#### Scenario: Credit lines
- **GIVEN** the cycle summary's `spending.credit` with `spent_on_credit` 579000, `paid_to_credit` 470000 and one line per credit account
- **WHEN** the owner opens "Rincian" on Beranda and opens the Pengeluaran line
- **THEN** after the five buckets it SHALL list "Pengeluaran pakai kredit" Rp 579.000 noted "sudah termasuk di atas" and "Tagihan kredit dibayar" Rp 470.000 noted "uang keluar, tidak dihitung lagi", each note naming its accounts with their amounts, and the Pengeluaran total SHALL be unchanged

#### Scenario: No credit use
- **WHEN** `spending.credit` is absent or both amounts are 0
- **THEN** the Pengeluaran line SHALL list the five buckets only

### Requirement: Paying With Credit
When an expense's account is a credit account, Catat SHALL show "Bayar pakai kredit" ("Pay with credit") with "Sisa limit", a choice of "Sekali bayar · 3x · 6x · 12x · Lainnya" ("Pay in full · 3 · 6 · 12 · Other"), and the first due date. It SHALL send `credit_terms`.

#### Scenario: Choosing instalments
- **GIVEN** SPayLater with `statement_day` 1, `payment_day` 11 and Rp 1.686.000 limit left
- **WHEN** the owner records a Rp 1.200.000 expense on SPayLater on 5 November and chooses "6x"
- **THEN** Catat SHALL show "Sisa limit Rp 1.686.000" and "Jatuh tempo pertama" 11 December, and `POST /api/transactions` SHALL carry `credit_terms` `{installment_count: 6}`

#### Scenario: Other count
- **WHEN** the owner chooses "Lainnya" and enters 18
- **THEN** the request SHALL carry `installment_count` 18, and 1 or 40 SHALL be refused in the form

#### Scenario: Not a credit account
- **WHEN** the owner switches "Bayar dari" to a bank account
- **THEN** the credit choices SHALL disappear and the request SHALL carry no `credit_terms`

### Requirement: Paying a Credit Bill
Perpindahan into a credit account SHALL offer "Bayar sisa tagihan {amount}" ("Pay the amount left {amount}") for the next bill, and SHALL keep the "Biaya / bunga" field.

#### Scenario: Paying the amount left
- **GIVEN** SPayLater's next bill has Rp 434.000 left to pay, due today
- **WHEN** the owner chooses Perpindahan from Jago to SPayLater and "Bayar sisa tagihan Rp 434.000", enters a fee of Rp 2.500 and saves
- **THEN** the dialog SHALL have said "Tagihan SPayLater Rp 434.000 jatuh tempo hari ini", and `POST /api/movements` SHALL carry `amount` 434000 and `fee` 2500

#### Scenario: Opened from a due bill
- **WHEN** a caller opens Catat with `openMovementCapture({ targetAccountId: SPayLater, amount: 434000 })`
- **THEN** Perpindahan SHALL show Ke SPayLater and 434.000, and no request SHALL be sent until the owner chooses "Pindahkan saldo"

#### Scenario: No open bill
- **WHEN** Ke is a credit account without a next bill
- **THEN** "Bayar sisa tagihan" SHALL not be shown and the dialog SHALL say there is no open bill

### Requirement: Credit Terms From Catat
For an expense on a credit account, Catat SHALL send `credit_terms.installment_count` (1 for "Sekali bayar") and SHALL send `first_due_date` only when the owner set or edited it. The shown first due date and instalment amounts SHALL follow "Credit Due Dates" (0% interest) and SHALL never be sent unless edited. A first due date before the purchase date SHALL be refused.

#### Scenario: Pay in full with the bill date
- **WHEN** the owner keeps "Sekali bayar" and the shown date
- **THEN** the request SHALL carry `credit_terms` `{installment_count: 1}` without `first_due_date`

#### Scenario: Owner's date
- **WHEN** the owner changes "Jatuh tempo pertama" to 1 December for a purchase on 5 November
- **THEN** the request SHALL carry `first_due_date` "2026-12-01", and 1 November SHALL be refused with "Jatuh tempo pertama tidak boleh sebelum tanggal transaksi."

#### Scenario: Preview equals the server
- **WHEN** a purchase is saved with the shown date and count
- **THEN** its ledger row's `next_due_date` SHALL equal the date Catat showed, and its instalments SHALL equal the amounts Catat showed

#### Scenario: No payment day
- **WHEN** the credit account has no `payment_day`
- **THEN** the date SHALL start empty with a hint to set it, and saving without a date SHALL send no `first_due_date`

### Requirement: Credit Purchase Says No Cash Left
Catat SHALL label an expense on a credit account "dibayar nanti · {account} · jatuh tempo {date}" ("pay later · {account} · due {date}"), or with the instalment count, first date and amount per instalment, and SHALL mark a credit account in the sentence preview as "dibayar nanti". The purchase SHALL still count in Pengeluaran on its date.

#### Scenario: Pay later line
- **WHEN** the owner records Rp 120.000 on SPayLater (statement day 1, payment day 11) on 5 November with "Sekali bayar"
- **THEN** the form SHALL read "dibayar nanti · SPayLater · jatuh tempo 11 Des", and after saving Pengeluaran for 5 November SHALL be Rp 120.000 higher

#### Scenario: Instalment line with a remainder
- **WHEN** the owner chooses "3x" for Rp 1.000.000
- **THEN** the form SHALL read "dibayar nanti · 3 cicilan mulai 11 Des · Rp 333.334 lalu Rp 333.333 per cicilan · tanpa bunga"

### Requirement: Unpaid Credit Purchase Colour
A credit purchase that is not fully settled SHALL show its amount in the credit colour (`--credit-fg`) wherever a transaction row amount is shown, with its minus sign and its "pay later" text. Once fully settled it SHALL show in the expense colour with its "paid {date}" text and SHALL remain a credit purchase. No total, balance or bill SHALL change.

#### Scenario: Unpaid purchase
- **GIVEN** an unpaid Rp 120.000 purchase on SPayLater
- **WHEN** the owner reads Transactions in either theme
- **THEN** its amount SHALL read "−Rp 120.000" in `--credit-fg` and its row SHALL say "pay later"

#### Scenario: Partly paid instalments
- **GIVEN** a 6-instalment purchase with 2 instalments settled
- **WHEN** the row is shown
- **THEN** its amount SHALL stay in `--credit-fg`

#### Scenario: Fully paid
- **WHEN** a purchase's last item is settled on 4 December
- **THEN** its amount SHALL be in the expense colour, its row SHALL say "paid 4 Dec", and it SHALL still appear under the Credit filter

#### Scenario: Ordinary spending
- **WHEN** a bank expense is shown
- **THEN** its amount SHALL be in the expense colour

#### Scenario: Figures unchanged
- **WHEN** the colour is applied
- **THEN** Spending, safe to spend, balances and bills SHALL equal their values before this change

#### Scenario: Readable and distinct
- **WHEN** `--credit-fg` is measured in either theme
- **THEN** it SHALL have at least 4.5:1 contrast on canvas, surface and surface-2, SHALL be at least 14 OKLab units from `--out-fg`, and SHALL be its own token, separate from `--warn-fg`

### Requirement: Payment Lists What It Paid
The detail of a payment into a credit account SHALL list the items that payment settled, in settlement order, with the share it paid of each, and the part kept for later. The list SHALL be computed from the account's schedule and SHALL NOT be stored. The shares plus the part kept for later SHALL equal the payment.

#### Scenario: One payment clears a bill
- **GIVEN** SPayLater purchases of Rp 1.000.000, Rp 500.000 and Rp 1.002.500 and instalment 3 of 6 at Rp 100.000, all due on one bill
- **WHEN** the owner opens the Rp 2.602.500 payment
- **THEN** the detail SHALL list those four items with their full amounts, `kept_for_later` SHALL be 0, and instalments 4 to 6 SHALL NOT be listed

#### Scenario: Two payments share a purchase
- **GIVEN** one Rp 300.000 purchase paid by Rp 100.000 and then Rp 200.000
- **WHEN** the owner opens the first payment
- **THEN** it SHALL list the purchase with `amount` 100000 and `item_amount` 300000

#### Scenario: Payment above what is owed
- **WHEN** a Rp 500.000 payment covers Rp 470.000 of items
- **THEN** `kept_for_later` SHALL be 30000

#### Scenario: Opened from either side
- **WHEN** the detail is requested for either leg of the payment movement
- **THEN** both SHALL return the same list

#### Scenario: Purchase deleted after payment
- **WHEN** a listed purchase is deleted
- **THEN** the payment's list SHALL be recomputed without it and the freed amount SHALL settle the next items or be kept for later

#### Scenario: Account without a payment day
- **WHEN** the payment's credit account has no payment day
- **THEN** the list SHALL be empty and `due_dates_unavailable` SHALL be true

#### Scenario: Foreign payment
- **WHEN** an owner requests another owner's payment
- **THEN** the response SHALL be 404 and SHALL reveal no items

#### Scenario: Rows and the list endpoint
- **WHEN** the ledger list is requested
- **THEN** its payment rows SHALL NOT carry `settled_items`
