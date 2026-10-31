# currency-conversion-and-settings Specification

## Purpose
Define user profile settings and currency display conversion using maintained exchange-rate information.

## Requirements

### Requirement: Streamlined User Settings & Profile Information
The system SHALL persist and present user settings and profile information through aligned registration and settings contracts.

#### Scenario: Required behavior and constraints
- **WHEN** an owner registers, views or updates profile and settings information
- **THEN** registration SHALL persist a submitted display name or omit the control, and settings SHALL display username and permit updating display name
- **AND** settings SHALL support salary-source configuration, IDR and USD, an emergency-fund multiplier and an optional monthly spending budget
- **AND** unsupported currencies and invalid numeric ranges SHALL be rejected by both form and server without changing saved settings
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
Web Settings SHALL be a page at `/settings`, not a dialog, divided into six sections with their own address: Profil, Gaji & siklus, Mata uang & anggaran, Perangkat & akses, Pemetaan notifikasi and Nama pengirim. Each section SHALL be one card with a title and a one-line purpose. At 1024 px and wider a section list SHALL stay beside the selected section; narrower screens SHALL show the list first and open each section on its own screen with a way back.

#### Scenario: Choosing a section on desktop
- **GIVEN** a 1280 px wide window on `/settings`
- **WHEN** the owner selects "Perangkat & akses" in the section list
- **THEN** the address SHALL become `/settings/perangkat`, that section's card SHALL be shown beside the list, and the list SHALL mark it as the current page

#### Scenario: Opening a section on a phone
- **GIVEN** a 390 px wide window on `/settings`
- **WHEN** the owner selects "Nama pengirim"
- **THEN** only that section SHALL be shown with a "Semua pengaturan" link back to the list, and the page SHALL not scroll horizontally

#### Scenario: Unknown section address
- **WHEN** the owner opens a `/settings/` address that names no section
- **THEN** the application SHALL show its not-found page instead of an empty settings card

### Requirement: Section-Scoped Settings Save
A settings section with editable fields SHALL have its own Save action at the foot of its card that sends only that section's fields. Profil SHALL save display name and bank-notification name aliases; Mata uang & anggaran SHALL save currency, emergency-fund months and the monthly operational limit. Validation and error messages SHALL apply only to the fields of the section being saved.

#### Scenario: Saving the profile leaves budget settings untouched
- **WHEN** the owner changes the display name and saves Profil
- **THEN** the request SHALL contain only `name` and `name_aliases`, and currency, emergency-fund months and the monthly limit SHALL keep their saved values

#### Scenario: Saving budget settings leaves the profile untouched
- **WHEN** the owner sets the monthly operational limit to Rp 10.000.000 and saves Mata uang & anggaran
- **THEN** the request SHALL contain only `currency`, `emergency_fund_multiplier` and `monthly_spending_budget`, and the budget, coverage and dashboard views SHALL be refreshed

#### Scenario: Invalid value blocks only its own section
- **WHEN** the owner enters an emergency-fund target of 40 months and saves Mata uang & anggaran
- **THEN** the section SHALL show "Target dana darurat harus antara 1 dan 36 bulan" and no request SHALL be sent

### Requirement: Consistent Settings Form Layout
Every settings form field SHALL have its label above the control and any help text below it, programmatically associated with the control. Fields within one section SHALL share one width, section actions SHALL be right-aligned at the foot of the card, interactive targets SHALL be at least 44 by 44 CSS pixels with visible keyboard focus, money and counts SHALL use tabular figures, and empty lists SHALL say plainly that nothing exists yet.

#### Scenario: Keyboard pass through a section
- **WHEN** a keyboard user tabs through any settings section
- **THEN** every control SHALL be reached in visual order with a visible focus indicator, and every field's help text SHALL be announced with it

#### Scenario: Empty mapping list
- **WHEN** the owner has no notification mappings
- **THEN** Pemetaan notifikasi SHALL say "Belum ada pemetaan." and show no placeholder rows

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
