## MODIFIED Requirements

### Requirement: Goal Progress Action Presentation
The goal dialog SHALL present progress actions according to goal backing: a standalone goal offers "Tambah progres", which changes no account or ledger balance; an account-backed goal says that progress follows its linked balances and offers no deposit. Changing the progress source SHALL be explained and confirmed before saving, and no progress-only adjustment SHALL be labelled a transfer or ask for a source account.

#### Scenario: Required behavior and constraints
- **WHEN** an owner views progress actions for a savings goal
- **THEN** the following required behavior and constraints hold:

  The Goals interface SHALL present progress actions according to goal backing:
  1. A standalone goal SHALL offer a clearly labeled progress adjustment that changes no account or ledger balance.
  2. An account-backed goal SHALL display that progress follows linked account balances and SHALL not offer a synthetic goal deposit action.
  3. Linking or unlinking accounts SHALL explain the resulting progress source before saving.
  4. The interface SHALL never label a progress-only adjustment as an account transfer or ask for a source account that will not be debited.

#### Scenario: Viewing an account-backed goal
- **WHEN** a goal has one or more linked accounts
- **THEN** the interface identifies those accounts as the progress source and omits standalone deposit controls

#### Scenario: Adjusting a standalone goal
- **WHEN** a user records progress for a goal without linked accounts
- **THEN** the confirmation states that no account transaction will be created

#### Scenario: Progress source in the goal dialog
- **WHEN** the owner opens "Ubah target tabungan" for a goal and switches "Sumber progres" from "Manual" to "Otomatis dari saldo kantong" with two pockets chosen
- **THEN** the dialog SHALL show "Total saldo terhubung" counting a pocket and its parent once, and SHALL ask for confirmation before saving, as before the redesign

### Requirement: Target & Tagihan Layout
From 1200 px Target & Tagihan SHALL show the hero "Ketahanan Dana" at full width, then a reading column with "Tagihan 30 hari ke depan" and "Target tabungan" beside a 352 px panel "Utang & kredit", gap 24. Below 1200 px the panel SHALL follow the reading column. Goal, debt and plan details SHALL open in centered dialogs, and no section SHALL sit in a card inside a card.

#### Scenario: Balanced desktop layout
- **WHEN** the owner opens `/goals` at 1440 px with one debt, two goals and no bill in the next 30 days
- **THEN** the hero SHALL be followed by the reading column and the 352 px panel, "Tagihan 30 hari ke depan" SHALL say that no bill is due, and the goals SHALL be listed under "Target tabungan"

#### Scenario: Unknown essential cost
- **WHEN** the essential monthly cost is unknown
- **THEN** Ketahanan Dana SHALL say why it cannot be computed and SHALL show no number of months

#### Scenario: Order at 1024 px
- **WHEN** the owner opens `/goals` at 1024 px
- **THEN** the hero, Tagihan, Target tabungan and Utang & kredit SHALL follow each other in one column in that visual, reading and focus order, with no horizontal overflow

## ADDED Requirements

### Requirement: Target & Tagihan Hero
The hero SHALL show "Ketahanan Dana" with the page's only "Rincian", `coverage_months` as months, one line naming the basis and the target months, an 8 px bar of the server's `coverage_pct`, and four figures: Dana darurat, Target (`target_amount`), Biaya pokok / bulan and Jika pemasukan berhenti (`runway_days`). The page SHALL compute none of these figures.

#### Scenario: Known coverage
- **GIVEN** `coverage_months` 5.4, `target_months` 6, `coverage_pct` 90, `emergency_fund_balance` Rp 19.412.400, `target_amount` Rp 21.546.000, `monthly_primary_expense` Rp 3.591.000 and `runway_days` 162
- **WHEN** the owner opens `/goals`
- **THEN** the hero SHALL read "5,4 bulan" and "biaya pokok tertutup dana darurat · target 6 bulan", the bar SHALL fill 90 % with "90% dari target dana darurat" as its name, and the figures SHALL read Rp 19.412.400 "90% dari target", Rp 21.546.000 "6 × biaya pokok", Rp 3.591.000 and "162 hari"

#### Scenario: No designated reserve
- **WHEN** `balance_basis` is `all_liquid`
- **THEN** the hero line SHALL read "Belum ada dana darurat khusus — dihitung dari seluruh saldo kas · target 6 bulan", and the first figure SHALL be labelled "Saldo kas"

#### Scenario: Rincian
- **WHEN** the owner activates "Rincian"
- **THEN** a 480 px dialog "Ketahanan Dana" SHALL list the reserve balance with its goals, the essential cost with each period linked to its ledger rows, the minimum instalments and the coverage, and focus SHALL return to "Rincian" when it closes

#### Scenario: Phone figures
- **WHEN** `/goals` is shown at 390 px
- **THEN** the hero SHALL show Dana darurat and Target only, and Biaya pokok / bulan and Jika pemasukan berhenti SHALL be reachable in the "Rincian"

### Requirement: Tagihan in the Next 30 Days
"Tagihan 30 hari ke depan" SHALL list the plan lines of the cycle summary's window and the detected recurring charges no plan covers, by date: date, name, status in words, schedule, "perkiraan" for a detected charge, and the amount. It SHALL show five rows and "dan N tagihan lain", a header with the count and total of those rows, and "Kelola", which opens "Rencana & Pengingat". Each row SHALL open its plan.

#### Scenario: Eight bills
- **GIVEN** seven plan lines and one detected charge in the window, one plan overdue and one due today
- **WHEN** the owner opens `/goals`
- **THEN** five rows SHALL be shown, "terlewat" in the expense text and "jatuh tempo hari ini" in the warning text, the header SHALL read "8 · {total of the eight}", and "dan 3 tagihan lain" SHALL show the rest in place

#### Scenario: Window until the next salary
- **WHEN** the cycle summary's `window_basis` is the expected salary
- **THEN** the section SHALL be titled "Tagihan sampai gaji berikutnya"

#### Scenario: No bill
- **WHEN** no plan or charge falls in the window
- **THEN** the section SHALL say "Tidak ada tagihan sampai 30 hari ke depan." in one line

### Requirement: Savings Goal Rows
Each active savings goal SHALL be one whole-row button with its name, a meta line (target date and monthly pace, "Tercapai", "Tanpa tanggal" or "Belum dapat dihitung", then "{n} kantong" or "dicatat manual"), a 4 px progress bar and the saved amount with "dari {target} · {pct}%". Opening it SHALL open "Ubah target tabungan".

#### Scenario: Goal in words
- **GIVEN** "Liburan ke Jepang" at Rp 5.100.000 of Rp 18.000.000 until 1 April 2027 with pace Rp 2.580.000 and one linked pocket
- **WHEN** the owner opens `/goals`
- **THEN** its row SHALL read "sampai 1 Apr 2027 · Rp 2.580.000/bln · 1 kantong" and "Rp 5.100.000", "dari Rp 18.000.000 · 28%", with the bar at 28 %

#### Scenario: Reached goal
- **WHEN** a goal's remaining amount is zero
- **THEN** its meta SHALL start with "Tercapai" in the income text and its bar SHALL be full in the income mark

### Requirement: Emergency Goal Placement
When exactly one active goal is flagged as the emergency fund, it SHALL be left out of "Target tabungan" and opened from "…" › "Ubah target dana darurat". With no flagged goal, "…" SHALL offer "Buat target dana darurat", which opens the goal form with the flag set. With several flagged goals, each SHALL stay listed with "Dana darurat" in its meta.

#### Scenario: One emergency goal
- **GIVEN** "Dana Darurat" flagged and three other goals
- **WHEN** the owner opens `/goals`
- **THEN** "Target tabungan" SHALL list three goals with "3 target", and "…" › "Ubah target dana darurat" SHALL open "Dana Darurat" in "Ubah target tabungan"

#### Scenario: No emergency goal
- **WHEN** no goal is flagged
- **THEN** "…" › "Buat target dana darurat" SHALL open "Buat target tabungan baru" with "Tandai sebagai dana darurat" checked, and saving SHALL send `is_emergency: true` as today's form does

### Requirement: Goal and Debt Dialogs
Goals and debts SHALL open in centered 560 px dialogs (bottom sheets below 768 px): "Ubah target tabungan", "Buat target tabungan baru", "Ubah utang", "Tambah utang" and "Pembayaran: {name}". Each SHALL keep today's fields, validation, confirmations and requests, trap focus, close with Esc and return focus to its opener.

#### Scenario: Archiving a goal
- **WHEN** the owner chooses "Arsipkan" in "Ubah target tabungan" and confirms
- **THEN** `DELETE /goals/{id}` SHALL be sent once, the confirmation SHALL say that account balances do not change, and focus SHALL return to the goal list

#### Scenario: Paying a debt
- **WHEN** the owner opens "Ubah utang" for "Cicilan Motor" and chooses "Kaitkan atau catat pembayaran"
- **THEN** "Pembayaran: Cicilan Motor" SHALL offer recorded expenses to link first and a manual record second, with today's requests

### Requirement: Target & Tagihan Loading, Failure and Empty States
While loading, each section SHALL show skeleton rows with `aria-busy`. A failed request SHALL show the SPEC error box with "Coba lagi" in the section it feeds, and the other sections SHALL keep working. An empty section SHALL say so in one line, and "Target tabungan" SHALL offer "Tambah target".

#### Scenario: Net worth fails
- **WHEN** `GET /dashboard/net-worth` fails and the goals load
- **THEN** the hero SHALL show "Ketahanan Dana belum dapat dihitung." with "Coba lagi", no figure SHALL read Rp 0, and the goals and debts SHALL be listed

### Requirement: Target & Tagihan in Both Languages
Target & Tagihan, its dialogs and "Rencana & Pengingat" SHALL take their copy from the `goals` and `plans` catalogues in English and Indonesian and their dates and amounts from the account's region. `/goals` SHALL be a translated route without the untranslated notice. Goal, debt, plan and account names SHALL be shown as stored.

#### Scenario: English account
- **WHEN** an account with English opens `/goals`
- **THEN** the page SHALL read "Goals & Bills", "Safety net", "Bills in the next 30 days", "Savings goals" and "Debts & credit", with no `lang="id"` region, and every figure SHALL equal the Indonesian page's figure
