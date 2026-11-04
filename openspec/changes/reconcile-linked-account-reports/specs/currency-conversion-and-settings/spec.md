## MODIFIED Requirements

### Requirement: Sectioned Settings Page
Settings SHALL be the page `/settings`, not a dialog, with eight sections in this order: HP & perangkat, Kebiasaan, Tampilan, then Akun: Profil, Gaji & siklus, Mata uang & anggaran, Pemetaan notifikasi, Nama pengirim. Each section SHALL have its own address and be one card with a title and neutral purpose. From 1024 px a grouped list SHALL sit beside the section and `/settings` SHALL show HP & perangkat; narrower screens SHALL show the list, then each section with a way back.

#### Scenario: Section addresses
- **WHEN** a section is opened
- **THEN** its address SHALL be `/settings/perangkat`, `/settings/kebiasaan`, `/settings/tampilan`, `/settings/profil`, `/settings/gaji`, `/settings/anggaran`, `/settings/pemetaan` or `/settings/pengirim`, in the order of the list

#### Scenario: Choosing a section on desktop
- **GIVEN** a 1280 px wide window on `/settings`
- **WHEN** the owner selects "Kebiasaan" in the section list
- **THEN** the address SHALL become `/settings/kebiasaan`, that section's card SHALL be shown beside the list, and the list SHALL mark it as the current page

#### Scenario: Opening a section on a phone
- **GIVEN** a 390 px wide window on `/settings`
- **WHEN** the owner selects "Nama pengirim"
- **THEN** only that section SHALL be shown with a "Semua pengaturan" link back to the list, and the page SHALL not scroll horizontally

#### Scenario: Unknown section address
- **WHEN** the owner opens a `/settings/` address that names no section
- **THEN** the application SHALL show its not-found page instead of an empty settings card

#### Scenario: Existing section addresses keep working
- **WHEN** the owner opens a bookmarked `/settings/perangkat`, `/settings/profil`, `/settings/gaji`, `/settings/anggaran`, `/settings/pemetaan` or `/settings/pengirim`
- **THEN** the same section SHALL open, with "Perangkat & akses" now titled "HP & perangkat"

#### Scenario: Grouped list for assistive technology
- **WHEN** a screen reader user reaches the section list
- **THEN** it SHALL be a labelled navigation whose Akun entries are grouped under a heading "Akun", and the current section SHALL carry `aria-current="page"`

## REMOVED Requirements

### Requirement: Notifikasi Ganda Section
**Reason**: Skip patterns are retired. Account links are learned from Tinjau and shown in the Rekening detail.
**Migration**: V47 converts active patterns into links. `/settings/notifikasi-ganda` shows the not-found page.
