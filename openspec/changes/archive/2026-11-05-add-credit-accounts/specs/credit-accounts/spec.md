## ADDED Requirements

### Requirement: Credit Account Type
An owner SHALL be able to create and edit a top-level account of type `credit` ("Kredit") with an amount owed today (stored as a non-positive `initial_balance`), an optional `credit_limit` above 0 and an optional `payment_day` from 1 to 31. A credit account SHALL NOT have pockets or a parent, and only a credit account SHALL accept a negative `initial_balance`, a limit or a payment day.

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
