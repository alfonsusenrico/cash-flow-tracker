## MODIFIED Requirements

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

## ADDED Requirements

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
