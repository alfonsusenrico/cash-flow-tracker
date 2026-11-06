## MODIFIED Requirements

### Requirement: Dedicated Mobile Home Information Architecture
Below 768 px the web app SHALL render a dedicated single-column Beranda built for phones, not the desktop layout stacked.

#### Scenario: Required behavior and constraints
- **WHEN** the application renders Beranda below 768 pixels
- **THEN** it SHALL start with the shell's phone header (brand, search, "Sembunyikan saldo" and the bell) and then the title block with the date and the period line; theme, the account's actions and the companion status SHALL NOT be in the header
- **AND** the first card SHALL be "Aman dipakai hari ini" (or its unavailable state) with the remaining amount and days, a labelled Dipakai / Terjadwal / Aman split and the waiting amount, each figure explainable as a bottom sheet
- **AND** "Perlu tindakan" SHALL follow, then the period's keluar and masuk, Kekayaan bersih, today's transactions and one insight
- **AND** Kakeibo SHALL make all five classifications and "Belum diklasifikasi" identifiable where it is shown
- **AND** secondary analytical charts SHALL remain on Analisis, and the phone tab bar SHALL never cover the last card

#### Scenario: Viewing home dashboard on mobile phone
- **WHEN** a user opens Beranda at 390 px
- **THEN** the interface shows the safe-to-spend card, "Perlu tindakan", the period figures and today's activity in one column without the sidebar, without horizontal scrolling, and with every target at least 24 by 24 CSS pixels

#### Scenario: Phone figures match desktop
- **WHEN** the same owner opens Beranda at 390 px and at 1440 px at the same moment
- **THEN** keluar, masuk, the waiting amount, the remaining amount and the safe-to-spend figure SHALL be identical in both layouts

### Requirement: Phone Page Actions and Targets
On phones, the page actions of the money pages SHALL sit in the title block: "Tambah rekening" and "Tambah target" visible, "Perbarui harga" and "Tambah utang" in its "…" menu. Every target SHALL be at least 44 by 44 CSS pixels, and a focused control SHALL not be hidden by the sticky phone header or the tab bar.

#### Scenario: Adding a target on a phone
- **WHEN** the owner opens `/goals` at 390 px and tabs to "Tambah target" in the title block
- **THEN** the control SHALL be fully visible below the phone header and above the tab bar with a visible focus outline, and activating it SHALL open the goal form as a bottom sheet

#### Scenario: Adding a debt on a phone
- **WHEN** the owner opens the "…" menu of `/goals` at 390 px and chooses "Tambah utang"
- **THEN** the menu SHALL open as a bottom sheet, and choosing the item SHALL open the debt form as a bottom sheet with focus inside it
