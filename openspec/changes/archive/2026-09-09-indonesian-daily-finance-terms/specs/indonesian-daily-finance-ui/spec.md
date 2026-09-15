## ADDED Requirements

### Requirement: Default Bahasa Indonesia Localization
The user interface SHALL render all text in Bahasa Indonesia by default, without requiring users to configure language settings.

#### Scenario: First-time user opens application
- **WHEN** any user visits the web app or login page
- **THEN** all navigational labels, form placeholders, metrics, and buttons display in natural Bahasa Indonesia

### Requirement: Intuitive Daily Finance Terminology
The application SHALL use simple, everyday consumer finance vocabulary rather than technical financial jargon:
1. Transaction directions:
   - Expense SHALL be labeled `Uang Keluar`
   - Income SHALL be labeled `Uang Masuk`
   - Internal Transfer SHALL be labeled `Pindah Saldo`
2. Metrics and allowances:
   - Safe to spend today SHALL be labeled `Batas Belanja Aman Hari Ini`
   - Spent today SHALL be labeled `Pengeluaran Hari Ini`
   - Cycle cadence SHALL be labeled `Laju Pengeluaran`
   - Liquid net worth / balance SHALL be labeled `Total Saldo Tersedia`
   - Days until payday SHALL be labeled `X hari menuju gajian`

#### Scenario: User checks daily allowance on mobile
- **WHEN** a user views the primary allowance card
- **THEN** they see "Batas Belanja Aman Hari Ini" with clear monetary values, eliminating technical jargon

### Requirement: Friendly Form Input and Action Buttons
All transaction entry forms, account creation dialogs, and settings modals SHALL provide clear, helpful Indonesian labels:
1. Quick capture CTA: `Catat Transaksi` (shortcut: `N`)
2. Form fields: `Nominal Uang`, `Kategori`, `Dari Rekening`, `Ke Rekening`, `Catatan (Opsional)`, `Tanggal Transaksi`
3. Primary submit action: `Simpan Transaksi`
4. Dismiss action: `Batal`

#### Scenario: Recording a new transaction
- **WHEN** a user opens the transaction capture modal
- **THEN** the modal is titled "Catat Transaksi Baru" with intuitive labels and a "Simpan Transaksi" submission button
