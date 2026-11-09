## ADDED Requirements

### Requirement: Perpindahan Default Accounts
When Catat opens Perpindahan without a chosen target, "Ke" SHALL default to the first liquid account that is not "Dari", is not an account with pockets, and is not the pocket that "Dari" resolves to (its default pocket, or any of its pockets when it has none). When no account qualifies, "Ke" SHALL stay empty. "Dari" and any target given by the caller SHALL keep their current defaults.

#### Scenario: Parent account first in the list
- **GIVEN** liquid accounts listed as "BCA" (default pocket "Utama"), its pockets "Utama" and "Harian", then "Jago"
- **WHEN** the owner opens Perpindahan
- **THEN** "Dari" SHALL be "BCA", "Ke" SHALL be "Harian", and saving with an amount SHALL send `POST /api/movements` that the server accepts, never a `same_account` refusal

#### Scenario: Parent without a default pocket
- **GIVEN** "BCA" has the pockets "Utama" and "Harian" and no default pocket, followed by "Jago"
- **WHEN** the owner opens Perpindahan with "Dari" "BCA"
- **THEN** "Ke" SHALL be "Jago"

#### Scenario: Only one account family
- **GIVEN** the only liquid accounts are "BCA" (default pocket "Utama") and its pockets "Utama" and "Harian"
- **WHEN** the owner opens Perpindahan
- **THEN** "Ke" SHALL be "Harian"

#### Scenario: A caller's target is kept
- **WHEN** "Bayar tagihan" opens Perpindahan for "Kartu Kredit Contoh"
- **THEN** "Ke" SHALL be "Kartu Kredit Contoh" as before

## MODIFIED Requirements

### Requirement: Credit Provider View
The type "Kredit" SHALL list the credit accounts as providers to choose from. With one credit account chosen as the account filter, the page SHALL show that account's Utang, next bill, "Pengeluaran pakai kredit" and "Tagihan kredit dibayar" from `GET /api/credit/accounts/{id}` over the same window as the listed rows, and the Rekening filter SHALL list credit accounts in a "Kredit" group.

#### Scenario: Provider view
- **WHEN** the owner chooses "Kredit" and then "SPayLater"
- **THEN** the URL SHALL carry `type=credit&account=<id>`, the list SHALL hold SPayLater's purchases, fees, refunds and payments only, and the header SHALL show the four figures of the endpoint for the page's period, hidden with "Sembunyikan saldo"

#### Scenario: A purchase timed later today
- **GIVEN** the cycle view of SPayLater at 12.39 with Rp 459.000 spent, and a Rp 77.000 SPayLater purchase timed 23.00 today
- **WHEN** the owner opens the provider view
- **THEN** the header's "Pengeluaran pakai kredit" and the hero's Pengeluaran SHALL both read Rp 459.000, because the header requests `period=cycle&period_offset=0` as the list does

#### Scenario: A custom range
- **WHEN** the owner filters SPayLater's rows from 1 to 15 October
- **THEN** the header SHALL request the list's `from_date` and `to_date` instants, and its "Pengeluaran pakai kredit" SHALL equal the hero's Pengeluaran

### Requirement: Credit Terms in the Transaction Dialog
The dialog of a credit purchase SHALL show how it is paid, its next due date, its instalments with due dates and state, and its settlement date and days. "Ubah transaksi" SHALL change the instalment count and the first due date through `PUT /api/transactions/{id}/credit-terms`, and return it to paid in full through `DELETE`. Paying in full SHALL stay the default. A stored first due date that the owner did not change SHALL be sent back as it is, whoever set it.

#### Scenario: Change the instalments
- **WHEN** the owner changes a paid-in-full SPayLater purchase to 3 instalments and saves
- **THEN** the web SHALL send today's `PATCH` and then `PUT /api/transactions/{id}/credit-terms` with `{installment_count: 3, first_due_date: null}`, and the dialog SHALL list three instalments

#### Scenario: Back to paid in full
- **WHEN** the owner chooses "Sekali bayar" without a first due date for a purchase with terms and no kept due date
- **THEN** the web SHALL send `DELETE /api/transactions/{id}/credit-terms`

#### Scenario: Count changed on a purchase with a kept due date
- **GIVEN** a purchase whose stored first due date 11 November was kept by the system after a payment-day change
- **WHEN** the owner changes only the instalments to 3, or back to "Sekali bayar", and saves
- **THEN** the web SHALL send `PUT /api/transactions/{id}/credit-terms` with that count and `first_due_date` 11 November, and no `DELETE`
