## ADDED Requirements

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
