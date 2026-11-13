## ADDED Requirements

### Requirement: Credit Bills Dialog
Selecting "Tagihan kredit" on Beranda SHALL open a centered dialog with one section per active credit account, most owed first. Each section SHALL show the account's Utang, limit left and next bill, then its transactions from its oldest unpaid item to today, oldest first, and end with "Lihat semua di Transaksi" to `/ledger?account={id}`. A row SHALL open the shared transaction dialog.

#### Scenario: Spaylater section
- **GIVEN** Spaylater owes Rp 3.046.014 for seven purchases from 4 to 11 November, all in the bill due 11 December, with no payment yet
- **WHEN** the owner opens the dialog
- **THEN** the Spaylater section SHALL list the seven purchases from 4 November to 11 November, each in the credit amber with "dibayar nanti · Spaylater · jatuh tempo 11 Des"

#### Scenario: Payments are listed
- **GIVEN** a bill partly paid on 11 December by a Rp 1.000.000 movement from Jago · Bayar Kredit
- **WHEN** the owner opens the dialog
- **THEN** the payment SHALL be listed at its date as a bill payment, settled purchases SHALL read "lunas {date}", and opening the payment SHALL show what it paid

#### Scenario: Nothing owed
- **WHEN** a credit account owes Rp 0
- **THEN** its section SHALL say "Tidak ada tagihan berjalan" and still offer "Lihat semua di Transaksi"

#### Scenario: One section fails
- **WHEN** one account's transactions fail to load
- **THEN** that section SHALL name the failure with "Coba lagi" and the other sections SHALL stay usable

#### Scenario: Keyboard and focus
- **WHEN** the dialog opens and is closed with Esc
- **THEN** focus SHALL move into the dialog on open and return to "Tagihan kredit" on close
