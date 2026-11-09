# currency-conversion-and-settings Specification

## Purpose
Define user profile settings and currency display conversion using maintained exchange-rate information.

## Requirements

### Requirement: Streamlined User Settings & Profile Information
The system SHALL persist and present user settings and profile information through aligned registration and settings contracts, including the account's interface language (`en` or `id`) and region (`ID` or `US`), which registration, sign-in, the profile endpoint and the settings endpoint SHALL return or accept.

#### Scenario: Required behavior and constraints
- **WHEN** an owner registers, views or updates profile and settings information
- **THEN** registration SHALL persist a submitted display name or omit the control, and settings SHALL display username and permit updating display name
- **AND** settings SHALL support salary-source configuration, IDR and USD, an emergency-fund multiplier, an optional monthly spending budget, the interface language and the region
- **AND** unsupported currencies, languages, regions and invalid numeric ranges SHALL be rejected by both form and server without changing saved settings
- **AND** successful changes SHALL refresh all affected consumers and the interface SHALL omit redundant toggles and unconfigured integrations

#### Scenario: Registering with a display name
- **WHEN** a new user submits a valid optional display name during registration
- **THEN** it is persisted and appears in authenticated profile settings

#### Scenario: Rejecting an unsupported currency
- **WHEN** a client attempts to save a currency other than IDR or USD
- **THEN** the server rejects the request and preserves the existing currency

#### Scenario: Updating user profile settings
- **WHEN** a user updates the emergency multiplier and monthly budget
- **THEN** both values are persisted and the affected coverage and budget views are refreshed

#### Scenario: Registering with language and region
- **WHEN** a person registers from the English sign-up page with region Indonesia
- **THEN** `/auth/me` SHALL return `language` `en` and `region` `ID`

#### Scenario: Registering without a language
- **WHEN** a client registers without `language` and with `Accept-Language: id-ID`
- **THEN** the account's language SHALL be `id` and its region `ID`

#### Scenario: Rejecting an unsupported language or region
- **WHEN** a client saves `language` `fr` or `region` `SG`
- **THEN** the server SHALL respond 422 with `detail.code` `invalid_language` or `invalid_region`, and the saved language and region SHALL be unchanged

#### Scenario: Another account's settings
- **WHEN** one account saves its language
- **THEN** no other account's language or region SHALL change

### Requirement: Automated Daily Currency Conversion
The system SHALL support dynamic currency conversion and formatting between `IDR` and `USD`.

#### Scenario: Required behavior and constraints
- **WHEN** the system refreshes exchange rates or displays a currency amount
- **THEN** the following required behavior and constraints hold:

  The system SHALL support dynamic currency conversion and formatting between `IDR` and `USD`:
  1. The currency selector in Settings SHALL offer a dropdown containing `IDR` (default) and `USD`, accompanied by the current exchange rate hint (e.g. `1 USD ≈ Rp 16.500`).
  2. The backend SHALL fetch the daily exchange rate for `USDIDR=X` from market data, cache it, and expose it via `/auth/currency/rates`.
  3. When the user selects `USD` as primary currency:
     - All monetary balances, transaction amounts, goals, obligations, and budget metrics stored in base IDR SHALL be converted dynamically (`value_usd = value_idr / usdidr_rate`).
     - Converted values SHALL be formatted using US dollar currency notation (e.g., `$1,208.27` or `$1,208.00`).
     - If balance hiding is enabled (`hideBalances = true`), values SHALL be masked as `$ ••••••` instead of `Rp ••••••`.
     - Chart axes and tooltips SHALL format values using dollar scale benchmarks ($k, $M).
  4. When the user selects `IDR` as primary currency:
     - Monetary values SHALL be formatted using Indonesian Rupiah notation (`Rp 19.936.520`) with dot thousand separators and no decimal places.
     - If balance hiding is enabled (`hideBalances = true`), values SHALL be masked as `Rp ••••••`.

#### Scenario: Switching primary currency to USD
- **WHEN** the user selects "USD" in the currency dropdown and saves settings
- **THEN** the system updates the user's currency preference, refetches exchange rates, and all balances across the dashboard, goals, accounts, transactions, and insights are converted and displayed with the "$" symbol and 2 decimal places

#### Scenario: Viewing savings goals in USD
- **WHEN** a user with primary currency set to "USD" views the Goals page
- **THEN** all savings goal amounts (current amount, target amount, linked account balances, monthly target pace) and obligation balances are rendered in USD (e.g. "$1,208.27 of $1,212.12") instead of IDR

#### Scenario: Switching back to IDR
- **WHEN** the user reverts their primary currency preference to "IDR"
- **THEN** all balances and metrics immediately re-render in Indonesian Rupiah with "Rp" prefix and dot thousand separators without conversion loss

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

### Requirement: Confirmed Removal and Search of Notification Names
Removing a sender name or a notification mapping in Settings SHALL require a second, explicit confirmation in the same row, and cancelling SHALL change nothing. When more than eight sender names exist, Nama pengirim SHALL offer a search field that filters the list by mask, name, bank or account and states how many names match.

#### Scenario: Confirming a sender removal
- **WHEN** the owner selects "Hapus" for a sender name and then "Ya, hapus"
- **THEN** the sender name SHALL be removed and the next matching transfer SHALL ask again

#### Scenario: Cancelling a removal
- **WHEN** the owner selects "Hapus" for a notification mapping and then "Batal" or presses Escape
- **THEN** no removal request SHALL be sent and focus SHALL return to that row's "Hapus" button

#### Scenario: Filtering a long sender list
- **GIVEN** twelve saved sender names
- **WHEN** the owner types "jago" in "Cari nama pengirim"
- **THEN** only the names whose mask, name, bank or account contain "jago" SHALL be listed, with the number of matches announced

#### Scenario: Short sender list
- **GIVEN** five saved sender names
- **THEN** no search field SHALL be shown

### Requirement: Help and Phone Status in Pengaturan
Pengaturan SHALL hold what left the sidebar: HP & perangkat SHALL start with a "Status HP" box holding the companion status line and each paired phone's readiness, and Tampilan SHALL hold "Bantuan & pintasan" with a button that opens the help. The `?` key SHALL still open the same help from every page while shortcuts are on.

#### Scenario: Finding help without the sidebar entry
- **WHEN** the owner opens Pengaturan · Tampilan and chooses "Buka bantuan"
- **THEN** "Bantuan & pintasan" SHALL open, and pressing `?` on Beranda SHALL open the same help

#### Scenario: Phone status in Pengaturan
- **WHEN** the owner opens Pengaturan · HP & perangkat with a paired phone that last reported yesterday at 16.05
- **THEN** the section SHALL start with the "Status HP" box reading "HP terhubung · Terakhir menangkap kemarin 16.05" as text, followed by that phone's checks

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
