## MODIFIED Requirements

### Requirement: Sectioned Settings Page
Web Settings SHALL be a page at `/settings`, not a dialog, divided into eight sections with their own address, listed in this order: HP & perangkat (`/settings/perangkat`), Kebiasaan (`/settings/kebiasaan`), Tampilan (`/settings/tampilan`), and the group Akun with Profil (`/settings/profil`), Gaji & siklus (`/settings/gaji`), Mata uang & anggaran (`/settings/anggaran`), Pemetaan notifikasi (`/settings/pemetaan`) and Nama pengirim (`/settings/pengirim`). Each section SHALL be one card with a title and a one-line purpose written impersonally in neutral Indonesian (`ui-copy-register`). At 1024 px and wider a grouped section list SHALL stay beside the selected section, and `/settings` itself SHALL show HP & perangkat; narrower screens SHALL show the list first and open each section on its own screen with a way back.

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
