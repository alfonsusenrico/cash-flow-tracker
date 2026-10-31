## MODIFIED Requirements

### Requirement: Friendly Form Input and Action Buttons
All transaction entry forms, account creation dialogs, and the settings page SHALL provide clear, helpful Indonesian labels:
1. Quick capture CTA: `Catat Transaksi` (shortcut: `N`)
2. Form fields: `Nominal Uang`, `Kategori`, `Dari Rekening`, `Ke Rekening`, `Catatan (Opsional)`, `Tanggal Transaksi`
3. Primary submit action: `Simpan Transaksi`
4. Dismiss action: `Batal`

#### Scenario: Recording a new transaction
- **WHEN** a user opens the transaction capture modal
- **THEN** the modal is titled "Catat Transaksi Baru" with intuitive labels and a "Simpan Transaksi" submission button
