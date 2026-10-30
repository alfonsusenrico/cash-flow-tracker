## MODIFIED Requirements

### Requirement: Intuitive Daily Finance Terminology
The application SHALL use simple, everyday consumer finance vocabulary rather than technical, literal, or bureaucratic financial jargon.

#### Scenario: Required behavior and constraints
- **WHEN** the application presents finance labels or transaction names
- **THEN** it SHALL label expense `Uang Keluar`, income `Uang Masuk`, and internal transfer `Pindah Saldo`
- **AND** it SHALL use `Pengeluaran Harian`, `Batas Anggaran Kategori`, `Terpakai`, `Target Tabungan`, `Tagihan & Cicilan` or `Pelunasan Cicilan & Utang`, and `Sesuaikan Saldo` where applicable
- **AND** net worth SHALL be explained as total assets less outstanding installments and debt, not cash alone
- **AND** the application SHALL avoid informal slang such as `Boncos`, `Tekor`, or `Jebol`

#### Scenario: User views category budget breakdown
- **WHEN** a user navigates to the Insights page
- **THEN** the table uses `Terpakai` and the budget section uses `Batas Anggaran Kategori`

#### Scenario: User views net worth and account reconciliation
- **WHEN** a user views an account card
- **THEN** the reconciliation action is labeled `Sesuaikan` or `Sesuaikan Saldo` in natural Indonesian
