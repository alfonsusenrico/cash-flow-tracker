## ADDED Requirements

### Requirement: Paying Later in Catat
When the owner has an active credit account, an expense in Catat SHALL show "Kapan bayar" ("When to pay") with "Bayar sekarang" ("Pay now") and "Bayar nanti (kredit)" ("Pay later (credit)") next to "Bayar dari". "Bayar nanti" SHALL be selected exactly when the chosen account is a credit account. Under "Bayar nanti" the account picker SHALL list only credit accounts; under "Bayar sekarang" only cash accounts. Without an active credit account the choice SHALL NOT be shown.

#### Scenario: Choosing to pay later
- **GIVEN** the owner has Cash and SPayLater
- **WHEN** the owner chooses "Bayar nanti (kredit)" on an expense
- **THEN** "Bayar dari" SHALL be SPayLater, list no cash account, and the credit payment choices SHALL be shown

#### Scenario: Back to paying now
- **WHEN** the owner then chooses "Bayar sekarang"
- **THEN** "Bayar dari" SHALL be the first cash account, list no credit account, and the request SHALL carry no `credit_terms`

#### Scenario: No credit account
- **WHEN** the owner has no active credit account
- **THEN** Catat SHALL show no "Kapan bayar" choice and "Bayar dari" SHALL be unchanged

## MODIFIED Requirements

### Requirement: Credit Accounts in Pickers
Pickers for an expense's account, a movement's source and target and a one-sided completion SHALL list credit accounts in their own "Kredit" group, except Catat's expense picker, which SHALL offer them under "Bayar nanti (kredit)". Investment funding, obligation payments and "Cocokkan saldo" SHALL NOT offer credit accounts, and the server SHALL refuse a credit account there.

#### Scenario: Investment funding
- **WHEN** a request funds an investment buy from a credit account
- **THEN** it SHALL be refused with 422 and nothing SHALL be booked

#### Scenario: Cocokkan saldo
- **WHEN** Rekening & Kantong shows a credit account
- **THEN** it SHALL NOT offer "Cocokkan saldo"

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
- **WHEN** the owner switches "Kapan bayar" to "Bayar sekarang"
- **THEN** the credit choices SHALL disappear and the request SHALL carry no `credit_terms`
