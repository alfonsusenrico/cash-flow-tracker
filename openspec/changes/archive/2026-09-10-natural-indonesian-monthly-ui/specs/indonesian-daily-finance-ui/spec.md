## MODIFIED Requirements

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
