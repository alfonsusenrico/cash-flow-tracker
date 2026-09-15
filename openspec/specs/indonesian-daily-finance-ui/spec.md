# indonesian-daily-finance-ui Specification

## Purpose
TBD - created by archiving change indonesian-daily-finance-terms. Update Purpose after archive.
## Requirements
### Requirement: Default Bahasa Indonesia Localization
The user interface SHALL render all text in Bahasa Indonesia by default, without requiring users to configure language settings.

#### Scenario: First-time user opens application
- **WHEN** any user visits the web app or login page
- **THEN** all navigational labels, form placeholders, metrics, and buttons display in natural Bahasa Indonesia

### Requirement: Intuitive Daily Finance Terminology
The application SHALL use simple, everyday consumer finance vocabulary rather than technical, literal, or bureaucratic financial jargon:
1. Transaction directions:
   - Expense SHALL be labeled `Uang Keluar`
   - Income SHALL be labeled `Uang Masuk`
   - Internal Transfer SHALL be labeled `Pindah Saldo`
2. Metrics and allowances:
   - Daily spending comparison SHALL be labeled `Pengeluaran Harian` (not "Laju Pengeluaran")
   - Monthly category limits SHALL be labeled `Batas Anggaran Kategori`
   - Realized spending in categories SHALL be labeled `Terpakai` (not "Realisasi")
   - Net cash balance explanation SHALL be labeled `Total Kekayaan Bersih (Total saldo kas dikurangi sisa cicilan & utang)`
   - Savings portfolio SHALL be labeled `Target Tabungan` (not "Portofolio Target Tabungan")
   - Debt and liabilities SHALL be labeled `Tagihan & Cicilan` or `Pelunasan Cicilan & Utang`
3. Reconciliation and Accuracy:
   - Account reconciliation SHALL be labeled `Sesuaikan Saldo`
   - The application SHALL NOT use informal slang words (such as "Boncos", "Tekor", or "Jebol")

#### Scenario: User views category budget breakdown
- **WHEN** a user navigates to the Insights page
- **THEN** the table column displays "Terpakai" instead of "Realisasi", and the section header displays "Batas Anggaran Kategori"

#### Scenario: User views net worth and account reconciliation
- **WHEN** a user views an account card on the Accounts page
- **THEN** the reconciliation action is labeled "Sesuaikan" / "Sesuaikan Saldo", retaining natural and accurate banking Indonesian

### Requirement: Friendly Form Input and Action Buttons
All transaction entry forms, account creation dialogs, and settings modals SHALL provide clear, helpful Indonesian labels:
1. Quick capture CTA: `Catat Transaksi` (shortcut: `N`)
2. Form fields: `Nominal Uang`, `Kategori`, `Dari Rekening`, `Ke Rekening`, `Catatan (Opsional)`, `Tanggal Transaksi`
3. Primary submit action: `Simpan Transaksi`
4. Dismiss action: `Batal`

#### Scenario: Recording a new transaction
- **WHEN** a user opens the transaction capture modal
- **THEN** the modal is titled "Catat Transaksi Baru" with intuitive labels and a "Simpan Transaksi" submission button

### Requirement: Automatic Thousands Dot Separation on Number and Amount Inputs
The user interface SHALL automatically format all numeric currency and amount input fields with Indonesian thousand separator periods (`.`) in real time as the user types:
1. When typing digits, the displayed value SHALL dynamically group digits with dots (e.g. `20000000` is displayed as `20.000.000`).
2. When deleting digits, backspacing, or clearing the input, the dots SHALL automatically readjust to reflect the remaining digits.
3. Submitting the form or mutating state SHALL parse out all non-digit characters so that API payloads receive clean integer amounts.
4. If a user pastes numbers containing dots, commas, or spaces, the input SHALL extract only digits and reformat cleanly with thousand dots.
5. All numeric input fields across Goals, Obligations, Accounts, Dashboard Transfers, Ledger Edits, and Category Budgets SHALL utilize this automatic dot formatting.

#### Scenario: User enters large amount in goal target field
- **WHEN** a user enters `20000000` into the Target Nominal input field in the Goals modal
- **THEN** the input display automatically shows `20.000.000` in real time without requiring manual punctuation

#### Scenario: User edits or backspaces formatted number
- **WHEN** a user deletes the last digit from an input showing `20.000.000`
- **THEN** the input immediately reformats and displays `2.000.000`

#### Scenario: Form submission sends clean integer
- **WHEN** a user submits a goal or transaction with `20.000.000` in the amount input
- **THEN** the application sends integer `20000000` to the backend API

