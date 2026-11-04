## MODIFIED Requirements

### Requirement: Sectioned Settings Page
Settings SHALL be the page `/settings`, not a dialog, with nine sections in this order: HP & perangkat, Kebiasaan, Tampilan, then Akun: Profil, Gaji & siklus, Mata uang & anggaran, Pemetaan notifikasi, Notifikasi ganda, Nama pengirim. Each section SHALL have its own address and be one card with a title and neutral purpose. From 1024 px a grouped list SHALL sit beside the section and `/settings` SHALL show HP & perangkat; narrower screens SHALL show the list, then each section with a way back.

#### Scenario: Section addresses
- **WHEN** a section is opened
- **THEN** its address SHALL be `/settings/perangkat`, `/settings/kebiasaan`, `/settings/tampilan`, `/settings/profil`, `/settings/gaji`, `/settings/anggaran`, `/settings/pemetaan`, `/settings/notifikasi-ganda` or `/settings/pengirim`, in the order of the list

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

## ADDED Requirements

### Requirement: Notifikasi Ganda Section
The section "Notifikasi ganda" SHALL list the owner's active skip patterns with their app names, direction, source, date and how often each skipped a report, SHALL stop one through "Berhenti melewati" with the two-step confirmation, and SHALL add one through "Tambah pola" from apps that have sent the owner a notification.

#### Scenario: Adding the known pattern by hand
- **GIVEN** an owner who has received notifications from Bank Jago and GoPay and has no pattern
- **WHEN** the owner chooses "Bank Jago" as "Aplikasi yang dilewati", "Uang masuk" as "Jenis" and "GoPay" as "Aplikasi yang dipakai", then "Simpan pola"
- **THEN** the pattern SHALL be saved with source `manual` and listed as "Bank Jago, uang masuk" with "Dilewati bila GoPay sudah melaporkan nominal yang sama"

#### Scenario: Invalid pattern
- **WHEN** the owner submits the same app twice, an app that never sent them a notification, or a pattern that is already active
- **THEN** `POST /api/ingest/skip-patterns` SHALL answer 422 `invalid_skip_pattern` or 409 `skip_pattern_exists`, nothing SHALL be saved, and the form SHALL say why

#### Scenario: Stopping a pattern
- **WHEN** the owner chooses "Berhenti melewati" and confirms
- **THEN** the pattern SHALL stop matching from that moment, reports it skipped earlier SHALL stay skipped, and it SHALL disappear from the list

#### Scenario: Another owner's pattern
- **WHEN** another owner requests `DELETE /api/ingest/skip-patterns/{id}` for that pattern
- **THEN** the response SHALL be 404 `skip_pattern_not_found` and the pattern SHALL stay active

#### Scenario: No patterns yet
- **WHEN** the owner opens the section with no active pattern
- **THEN** it SHALL say how patterns are added, from "Benar, satu transaksi" in Tinjau or with "Tambah pola", and offer the form
