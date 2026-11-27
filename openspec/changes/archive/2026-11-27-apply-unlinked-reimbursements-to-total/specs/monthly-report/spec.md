## ADDED Requirements

### Requirement: Unlinked Reimbursements Line in Breakdowns
When unlinked reimbursements without a category reduce a period's `Pengeluaran`, Laporan's pillar-and-category table, Analisis's Kakeibo breakdown, and Beranda/Analisis Rincian SHALL show one deduction line ("Ganti uang belum ditautkan" / "Unlinked reimbursements", formatted as `−Rp …` and masked when balances are hidden) so the visible lines add up to `Pengeluaran`. When zero, no such line SHALL appear.

#### Scenario: Laporan table reconciles with unlinked reimbursements
- **GIVEN** a closed period with Rp 1.000.000 of Kebutuhan expenses and Rp 150.000 of unlinked reimbursements
- **WHEN** Laporan is displayed
- **THEN** Kebutuhan SHALL read Rp 1.000.000, the table SHALL show "Ganti uang belum ditautkan" `−Rp 150.000`, and Total Pengeluaran SHALL read Rp 850.000

#### Scenario: No unlinked reimbursements hides the line
- **WHEN** a period has Rp 0 of unlinked reimbursements
- **THEN** neither Laporan nor Analisis SHALL render an unlinked-reimbursements row
