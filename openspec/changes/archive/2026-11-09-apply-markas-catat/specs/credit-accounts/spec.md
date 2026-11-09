## ADDED Requirements

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
