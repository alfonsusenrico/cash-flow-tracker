## ADDED Requirements

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
- **THEN** after the five buckets it SHALL list "Pengeluaran pakai kredit" Rp 579.000 noted "sudah termasuk di atas" and "Tagihan kredit dibayar" Rp 470.000 noted "uang keluar, tidak dihitung lagi", each opening its accounts, and the Pengeluaran total SHALL be unchanged

#### Scenario: No credit use
- **WHEN** `spending.credit` is absent or both amounts are 0
- **THEN** the Pengeluaran line SHALL list the five buckets only
