## ADDED Requirements

### Requirement: Tampilan Settings Section
Pengaturan SHALL have a Tampilan section at `/settings/tampilan` with the theme as a labelled radio group ("Ikuti sistem", "Terang", "Gelap"), the "Pintasan satu tombol" switch and "Sembunyikan saldo". Each control SHALL use the same stored setting as its existing counterpart (the theme control, the help dialog's `shortcuts` switch and the account menu's `hideBalances`), apply immediately and stay in sync with that counterpart without a reload.

#### Scenario: Choosing a theme in Tampilan
- **WHEN** the owner selects "Gelap" in Tampilan
- **THEN** the page SHALL switch to the dark theme at once, the account menu's theme control SHALL show "Gelap", and a reload SHALL render dark from the first paint

#### Scenario: Turning off single-key shortcuts
- **WHEN** the owner turns off "Pintasan satu tombol" in Tampilan
- **THEN** single-key shortcuts SHALL stop on every page, the help dialog's switch SHALL show off, and ⌘K/Ctrl+K and Esc SHALL keep working

#### Scenario: Hiding balances
- **WHEN** the owner turns on "Sembunyikan saldo" in Tampilan
- **THEN** every balance and amount the app masks today SHALL be masked on every page, and the account menu SHALL show the same state

#### Scenario: Storage unavailable
- **WHEN** browser storage throws while the owner changes a Tampilan control
- **THEN** the change SHALL apply for the open page, no error SHALL be shown, and the next visit SHALL start from the defaults
