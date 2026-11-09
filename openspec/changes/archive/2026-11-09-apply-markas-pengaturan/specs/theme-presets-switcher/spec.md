## MODIFIED Requirements

### Requirement: Tampilan Settings Section
Pengaturan SHALL have a Tampilan section at `/settings/tampilan` with, in order, the language choice, the theme as a labelled segment ("Terang", "Gelap", "Ikuti sistem"), the "Pintasan satu tombol" and "Sembunyikan saldo" switches, and "Bantuan & pintasan". Each control SHALL use the same stored setting as its shell counterpart, apply immediately and stay in sync with it without a reload.

#### Scenario: Choosing a theme in Tampilan
- **WHEN** the owner selects "Gelap" in Tampilan
- **THEN** the page SHALL switch to the dark theme at once, the top bar's theme menu SHALL show "Gelap" checked, and a reload SHALL render dark from the first paint

#### Scenario: Turning off single-key shortcuts
- **WHEN** the owner turns off "Pintasan satu tombol" in Tampilan
- **THEN** single-key shortcuts SHALL stop on every page, ? SHALL no longer open "Bantuan & pintasan", and ⌘K/Ctrl+K and Esc SHALL keep working

#### Scenario: Hiding balances
- **WHEN** the owner turns on "Sembunyikan saldo" in Tampilan
- **THEN** every balance and amount the app masks today SHALL be masked on every page, and the top bar's "Sembunyikan saldo" button SHALL show the pressed state

#### Scenario: Storage unavailable
- **WHEN** browser storage throws while the owner changes a Tampilan control
- **THEN** the change SHALL apply for the open page, no error SHALL be shown, and the next visit SHALL start from the defaults

#### Scenario: Switches and segment semantics
- **WHEN** a screen reader user reaches the theme and the two switches
- **THEN** the theme SHALL be a radio group with the chosen option checked, and each switch SHALL have `role="switch"`, its row title as name and its state as `aria-checked`
