## MODIFIED Requirements

### Requirement: Sectioned Settings Page
Settings SHALL be the page `/settings`, not a dialog, with nine sections in this order: HP & perangkat and Kebiasaan (group Perangkat), Tampilan (group Tampilan), then Profil, Gaji & siklus, Mata uang & anggaran, Pemetaan notifikasi, Nama pengirim and Sesi uji (group Akun). Each section SHALL have its own address. From 768 px the grouped rail SHALL sit beside the section; below it, above the section. `/settings` SHALL show HP & perangkat.

#### Scenario: Section addresses
- **WHEN** a section is opened
- **THEN** its address SHALL be `/settings/perangkat`, `/settings/kebiasaan`, `/settings/tampilan`, `/settings/profil`, `/settings/gaji`, `/settings/anggaran`, `/settings/pemetaan`, `/settings/pengirim` or `/settings/sesi-uji`, in the order of the rail

#### Scenario: Choosing a section on desktop
- **GIVEN** a 1280 px wide window with the sidebar open on `/settings`
- **WHEN** the owner selects "Kebiasaan" in the rail
- **THEN** the address SHALL become `/settings/kebiasaan`, that section SHALL be shown in the 720 px form column beside the 200 px rail, 48 px apart, the rail SHALL mark it as the current page with the selection bar, and focus SHALL move to the section title

#### Scenario: Opening a section on a phone
- **GIVEN** a 390 px wide window on `/settings`
- **WHEN** the owner selects "Nama pengirim" in the rail
- **THEN** the rail SHALL stay above and the section SHALL be shown under it with its title in view, with no back link and no horizontal scroll

#### Scenario: Unknown section address
- **WHEN** the owner opens a `/settings/` address that names no section
- **THEN** the application SHALL show its not-found page instead of an empty section

#### Scenario: Existing section addresses keep working
- **WHEN** the owner opens a bookmarked `/settings/perangkat`, `/settings/profil`, `/settings/gaji`, `/settings/anggaran`, `/settings/pemetaan` or `/settings/pengirim`
- **THEN** the same section SHALL open

#### Scenario: Grouped list for assistive technology
- **WHEN** a screen reader user reaches the rail
- **THEN** it SHALL be a navigation labelled "Bagian pengaturan" ("Settings sections") whose entries are grouped in lists labelled Perangkat, Tampilan and Akun, and the current section SHALL carry `aria-current="page"`

### Requirement: Section-Scoped Settings Save
A settings section with editable fields SHALL have its own Save action under its fields that sends only that section's fields. Profil SHALL save the display name and bank-notification names; Mata uang & anggaran SHALL save region, currency, emergency-fund months and "Batas pengeluaran bulanan". Validation and error messages SHALL apply only to the fields of the section being saved.

#### Scenario: Saving the profile leaves budget settings untouched
- **WHEN** the owner changes the display name and saves Profil
- **THEN** the request SHALL contain only `name` and `name_aliases`, and currency, region, emergency-fund months and the monthly limit SHALL keep their saved values

#### Scenario: Saving budget settings leaves the profile untouched
- **WHEN** the owner sets "Batas pengeluaran bulanan" to Rp 10.000.000 and saves Mata uang & anggaran
- **THEN** the request SHALL contain only `currency`, `region`, `emergency_fund_multiplier` and `monthly_spending_budget`, and the budget, coverage and dashboard views SHALL be refreshed

#### Scenario: Clearing the limit
- **WHEN** the owner empties "Batas pengeluaran bulanan" and saves
- **THEN** the request SHALL send `monthly_spending_budget` as null, as before

#### Scenario: Invalid value blocks only its own section
- **WHEN** the owner enters an emergency-fund target of 40 months and saves Mata uang & anggaran
- **THEN** the section SHALL show "Target dana darurat harus antara 1 dan 36 bulan" ("The emergency fund target must be between 1 and 36 months") and no request SHALL be sent

### Requirement: Consistent Settings Form Layout
Every settings field SHALL have its label above the control and any hint or error below it, programmatically associated. Sections SHALL have no card: a 20 px title, one description line, then parts 48 px apart. Save actions SHALL sit under their fields. Fields SHALL show focus as a 1 px edge with a 3 px soft ring and no coloured or 2 px border. Targets SHALL be at least 44 px on a phone; empty lists SHALL say so plainly.

#### Scenario: Keyboard pass through a section
- **WHEN** a keyboard user tabs through any settings section
- **THEN** every control SHALL be reached in visual order with a visible focus indicator, and every field's hint SHALL be announced with it

#### Scenario: Field focus
- **WHEN** a field in any section receives focus, in either theme
- **THEN** its edge SHALL be 1 px `--field-focus` with a 3 px `--ring`, and no border, outline or ring SHALL be 2 px or coloured

#### Scenario: Empty mapping list
- **WHEN** the owner has no notification mappings
- **THEN** Pemetaan notifikasi SHALL say "Belum ada pemetaan." ("No mappings yet.") and show no placeholder rows

### Requirement: Help and Phone Status in Pengaturan
Pengaturan SHALL hold what left the sidebar: HP & perangkat SHALL start with a "Status HP" box holding the companion status line and each paired phone's readiness, and Tampilan SHALL hold "Bantuan & pintasan" with a button that opens the help. The `?` key SHALL still open the same help from every page while shortcuts are on.

#### Scenario: Finding help without the sidebar entry
- **WHEN** the owner opens Pengaturan · Tampilan and chooses "Buka bantuan"
- **THEN** "Bantuan & pintasan" SHALL open, and pressing `?` on Beranda SHALL open the same help

#### Scenario: Phone status in Pengaturan
- **WHEN** the owner opens Pengaturan · HP & perangkat with a paired phone that last reported yesterday at 16.05
- **THEN** the section SHALL start with the "Status HP" box reading "HP terhubung · Terakhir menangkap kemarin 16.05" as text, followed by that phone's checks

## ADDED Requirements

### Requirement: Sign Out in Pengaturan
The Pengaturan rail SHALL end with a rule and "Keluar" ("Sign out"), which SHALL sign out exactly as the account menu does. The phone's Lainnya sheet SHALL no longer list "Keluar"; the desktop account menu SHALL keep it.

#### Scenario: Signing out from the rail
- **WHEN** the owner chooses "Keluar" in the Pengaturan rail
- **THEN** the app SHALL send `POST /api/auth/logout` once and open the sign-in page

#### Scenario: Sign out on a phone
- **WHEN** the owner opens Lainnya at 390 px
- **THEN** it SHALL not list "Keluar", and Lainnya › Pengaturan SHALL show "Keluar" at the end of the rail

### Requirement: Pengaturan in Both Languages
Pengaturan, its sections and its dialogs SHALL render in the interface language: English for an English account and neutral Indonesian for an Indonesian one, with no untranslated notice. Dates and amounts SHALL follow the account region. User data (phone names, mapping texts, sender names, masks, descriptions) SHALL be shown as stored.

#### Scenario: English account
- **WHEN** an English account opens `/settings/anggaran`
- **THEN** the page SHALL read "Settings", "Currency & budget" and "Monthly spending limit", and SHALL not show "This page is in Indonesian for now."

#### Scenario: Same values in both languages
- **WHEN** the same account's Pengaturan is read in English and in Indonesian with region Indonesia
- **THEN** every saved value, list entry, count, date and amount SHALL be identical

### Requirement: Hidden Balances in Pengaturan
With "Sembunyikan saldo" on, every amount in Pengaturan SHALL read "Rp ••••••" and be announced as hidden. "Batas pengeluaran bulanan" SHALL then be read-only with a hint to turn hiding off, and saving the section SHALL send the stored limit unchanged.

#### Scenario: Saving with balances hidden
- **GIVEN** a stored limit of Rp 4.800.000 and balances hidden
- **WHEN** the owner changes the emergency months to 9 and saves Mata uang & anggaran
- **THEN** the request SHALL carry `monthly_spending_budget` 4800000 and `emergency_fund_multiplier` 9

#### Scenario: Masked salary amounts
- **WHEN** balances are hidden on Gaji & siklus
- **THEN** each confirmed salary's amount SHALL read "Rp ••••••" and the rows SHALL keep their names and dates
