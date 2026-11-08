# credit-accounts Specification

## Purpose
Credit accounts such as paylater and credit cards: a top-level account whose negative balance is the amount owed. Purchases on it are spending when made, paying the bill is a movement into it, a bill fee is its own "Biaya kredit" expense, and what it owes counts as utang in net worth.

## Requirements

### Requirement: Credit Account Type
An owner SHALL be able to create and edit a top-level account of type `credit` ("Kredit") with an amount owed today (stored as a non-positive `initial_balance`), an optional `credit_limit` above 0, and an optional `payment_day` and `statement_day`, each from 1 to 31. A credit account SHALL NOT have pockets or a parent, and only a credit account SHALL accept a negative `initial_balance`, a limit, a payment day or a statement day.

#### Scenario: Adding SPayLater
- **WHEN** the owner adds "SPayLater" as Kredit with "Utang saat ini" Rp 350.000, limit Rp 2.000.000 and payment day 11
- **THEN** the account SHALL be stored with `initial_balance` −350000, `credit_limit` 2000000 and `payment_day` 11, and Rekening & Kantong SHALL show "Utang Rp 350.000", "Rp 350.000 dari Rp 2.000.000" and "dibayar tiap tgl 11"

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
Changing a credit account's `payment_day` or `statement_day` SHALL keep the due date of every purchase whose first due date was on or before today, and SHALL move only bills not yet due. Changing a purchase's date SHALL clear a kept due date. It SHALL also clear an owner due date that falls before the new date, and the response SHALL say so.

#### Scenario: Payment day moves from 11 to 5
- **GIVEN** a purchase due and paid on 11 November and a purchase due on 11 December, with today 20 November
- **WHEN** the owner changes the payment day to 5
- **THEN** the first SHALL stay due on 11 November and paid on time, and the second SHALL be due on 5 December

#### Scenario: Purchase date moved past the owner's due date
- **WHEN** the owner moves a purchase with an owner first due date of 1 December to 3 December
- **THEN** the purchase SHALL get its derived due date, and the response SHALL carry `credit_terms_reset: true`

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
`GET /api/credit/accounts/{id}` SHALL return the account's owed amount, limit, limit left (which may be negative), overpayment, days, next due bill, its bills with their items, and totals for a period: spending on credit, fees, refunds, cash paid to it, and items settled and settled on time. `GET /api/credit/bills` SHALL list bills across the owner's credit accounts. Credit rows of `GET /api/accounts` SHALL gain `statement_day`, `limit_left` and `next_due`.

#### Scenario: Overview totals
- **GIVEN** this cycle: Rp 200.000 spent on SPayLater, a Rp 10.000 fee, and Rp 480.000 paid from Jago
- **WHEN** the owner reads SPayLater's overview for the cycle
- **THEN** the totals SHALL be spending on credit Rp 210.000, fees Rp 10.000 and cash paid Rp 480.000

#### Scenario: Bills due soon across accounts
- **WHEN** the owner lists bills due by 13 December
- **THEN** SPayLater's 11 December bill and Kredivo's 1 December bill SHALL be listed with their account names and left to pay

#### Scenario: Not a credit account
- **WHEN** the overview is asked for a bank account, or for another owner's credit account
- **THEN** it SHALL be refused with 422 `credit_account_required`, or answered 404

#### Scenario: Error in the account's language
- **WHEN** an English account sends terms on a bank expense
- **THEN** the 422 SHALL carry `code` `credit_terms_not_credit_purchase` and an English `message`, and the code SHALL exist in both server catalogues
