# mobile-native-layout Specification

## Purpose
Define dedicated mobile home, quick-capture and ledger compositions for everyday finance tracking.

## Requirements

### Requirement: Dedicated Mobile Home Information Architecture
Below 768 px Beranda SHALL be one column in the phone order: the hero with Pengeluaran and Ditabung & investasi, "Perlu tindakan", "Aktivitas terbaru" with two-line rows, and the context panel. It SHALL use phone-sized targets.

#### Scenario: Required behavior and constraints
- **WHEN** the application renders Beranda below 768 pixels
- **THEN** it SHALL start with the shell's phone header (brand, search, "Sembunyikan saldo" and the bell) and then the title block with the date and the period line; theme, the account's actions and the companion status SHALL NOT be in the header
- **AND** the first block SHALL be "Aman dipakai sampai gajian" with its line, the split bar, Pengeluaran and Ditabung & investasi, and its "Rincian" opening as a bottom sheet
- **AND** "Perlu tindakan" SHALL follow, then "Aktivitas terbaru", then Kekayaan bersih, Ketahanan Dana and Kakeibo
- **AND** Kakeibo SHALL make all four spending pillars and "Belum diklasifikasi" identifiable
- **AND** secondary analytical charts SHALL remain on Analisis, and the phone tab bar SHALL never cover the last block

#### Scenario: Viewing home dashboard on mobile phone
- **WHEN** a user opens Beranda at 390 px or 320 px
- **THEN** the interface shows the hero, "Perlu tindakan", the activity and the panel in one column without the sidebar, without horizontal scrolling, with every row and button at least 44 by 44 CSS pixels and every inline link at least 24 by 24

#### Scenario: Phone figures match desktop
- **WHEN** the same owner opens Beranda at 390 px and at 1440 px at the same moment
- **THEN** Pengeluaran, Ditabung & investasi, masuk, the waiting amount and the safe amount SHALL be identical in both layouts, with masuk, Terjadwal and the waiting amount read in the Rincian at 390 px

### Requirement: Mobile Bottom-Sheet Quick Capture
The application SHALL render the canonical transaction-capture form as a touch-first bottom sheet on mobile screens.

#### Scenario: Required behavior and constraints
- **WHEN** an owner opens quick transaction capture on a mobile screen
- **THEN** the following required behavior and constraints hold:

  The application SHALL render the canonical transaction-capture form as a touch-first bottom sheet on mobile screens:
  1. The sheet SHALL have rounded top corners, a visible handle, contained scrolling, accessible dialog semantics, and safe focus behavior.
  2. Amount entry SHALL use high-visibility tabular figures and a touch-appropriate numeric interaction without preventing hardware-keyboard entry.
  3. Category selection SHALL use horizontally scrollable, named, stateful icon choices filtered by transaction type.
  4. Kakeibo choices SHALL be one-tap targets whose selected state is exposed programmatically and whose default follows the selected category.
  5. Account, date, notes, goal or obligation context, receipt attachment, errors, and submission results SHALL be functionally equivalent to the desktop canonical form.
  6. The sheet SHALL not force mobile keyboard focus immediately when doing so obscures context or causes unwanted viewport movement.
  7. The sheet SHALL expose the `Perpindahan` mode with eligible source and destination accounts, amount, seconds-precise timestamp, notes, and movement validation through the same atomic movement contract as desktop Quick Capture.

#### Scenario: Opening quick capture on mobile
- **WHEN** a user opens Quick Capture on a 390px viewport
- **THEN** the complete canonical transaction fields are available in a touch-first bottom sheet without losing keyboard or assistive-technology operation

#### Scenario: Selecting a category on mobile
- **WHEN** a user selects a category chip classified as `want`
- **THEN** the chip announces its selected state and the Kakeibo selection defaults to `want`

#### Scenario: Recording a movement on mobile
- **WHEN** a user selects `Perpindahan` and submits a valid movement in the mobile bottom sheet
- **THEN** the same atomic movement contract is used, with no transaction-only values included

### Requirement: Mobile Date-Grouped Transaction Ledger
Below 768 px the `/ledger` screen SHALL show, in order: the hero "Pengeluaran", "Masuk" and "Bersih" side by side, a horizontally scrolling type control with counts, the search field with "Filter", and two-line rows in sticky day groups. A row SHALL open the transaction dialog as a bottom sheet. From 768 px up the phone layout SHALL NOT be shown.

#### Scenario: Required behavior and constraints
- **WHEN** an owner views or interacts with the ledger on a mobile screen
- **THEN** the following required behavior and constraints hold:

  The `/ledger` screen SHALL display transactions in chronological date groups on mobile viewports:
  1. Transactions SHALL be grouped under sticky day headings below the 56 px phone header, labelled with the weekday and date and "hari ini" or "kemarin" when they apply.
  2. Each row SHALL be at least 64 px tall with two lines, the title and "{time} · {pillar, category or status}", and the signed amount at the right; tapping it SHALL open a bottom sheet with full details, category, accounts, provenance and the "Hapus" and "Ubah" actions.
  3. Filtering SHALL be reachable through the type control (`Semua`, `Pengeluaran`, `Masuk`, `Pindah saldo`, `Investasi`, `Tabungan & investasi`, each with its count) and the "Filter transaksi" sheet for accounts, categories, pillars, status, source and the period mode.
  4. Every target SHALL be at least 44 by 44 CSS pixels, and the page SHALL NOT scroll horizontally at 390 px or 320 px.

#### Scenario: Inspecting transaction detail on mobile
- **WHEN** a user taps a transaction item in the mobile ledger
- **THEN** a bottom sheet displays the transaction details, linked accounts, receipt state and action buttons without navigating away from the list

#### Scenario: Phone order
- **WHEN** the owner opens Transaksi at 390 px
- **THEN** the hero SHALL come first, then "Masuk" and "Bersih", then the type control, then the search field with "Filter", then the day groups, and no rail SHALL be shown

#### Scenario: Back closes the sheet
- **WHEN** the owner opens a row at 390 px and presses the browser's back
- **THEN** the sheet SHALL close, the list SHALL keep its filters and scroll position, and focus SHALL return to the row

#### Scenario: Merge on a phone
- **WHEN** the owner holds an eligible row for one second at 390 px
- **THEN** selection mode SHALL start with that row checked, and the selection bar SHALL stay visible below the phone header

### Requirement: Phone Rekening & Kantong Composition
At 760 px and below, Rekening & Kantong SHALL use a composition built for phones: the KPI cards first as a two-by-two grid, then "Komposisi aset", then the account cards in one column with their pockets inside, each pocket's balance on its own line beside its name. Every card head and pocket row SHALL open its actions in a bottom sheet, and reordering SHALL use "Ubah urutan" with move controls.

#### Scenario: Rekening on a phone
- **WHEN** the owner opens `/accounts` at 390 px
- **THEN** the KPI cards SHALL come first, no page SHALL overflow horizontally, and tapping a pocket SHALL open its bottom sheet with its actions without scrolling the page

#### Scenario: Closing a sheet returns focus
- **WHEN** a keyboard or switch user closes a row's bottom sheet with its close control or Escape
- **THEN** focus SHALL return to the row that opened it

### Requirement: Phone Target & Tagihan Composition
At 760 px and below, Target & Tagihan SHALL show Ketahanan Dana first, then Target tabungan, Utang and the bills up to the next salary, each goal and debt opening a bottom sheet.

#### Scenario: Target & Tagihan on a phone
- **WHEN** the owner opens `/goals` at 390 px
- **THEN** Ketahanan Dana SHALL come first and each goal row SHALL show its progress text without truncating the amounts

### Requirement: Phone Page Actions and Targets
On phones, the page actions of the money pages SHALL sit in the title block: "Tambah rekening" and "Tambah target" visible, "Perbarui harga" and "Tambah utang" in its "…" menu. Every target SHALL be at least 44 by 44 CSS pixels, and a focused control SHALL not be hidden by the sticky phone header or the tab bar.

#### Scenario: Adding a target on a phone
- **WHEN** the owner opens `/goals` at 390 px and tabs to "Tambah target" in the title block
- **THEN** the control SHALL be fully visible below the phone header and above the tab bar with a visible focus outline, and activating it SHALL open the goal form as a bottom sheet

#### Scenario: Adding a debt on a phone
- **WHEN** the owner opens the "…" menu of `/goals` at 390 px and chooses "Tambah utang"
- **THEN** the menu SHALL open as a bottom sheet, and choosing the item SHALL open the debt form as a bottom sheet with focus inside it
