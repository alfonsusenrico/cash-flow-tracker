# mobile-native-layout Specification

## Purpose
Define dedicated mobile home, quick-capture and ledger compositions for everyday finance tracking.

## Requirements

### Requirement: Dedicated Mobile Home Information Architecture
At 760 px and below the web app SHALL render a dedicated single-column Beranda built for phones, not the desktop layout stacked.

#### Scenario: Required behavior and constraints
- **WHEN** the application renders Beranda at 760 pixels or narrower
- **THEN** it SHALL show a compact sticky header with the date, the period line, theme and the account menu, and the companion status under it
- **AND** the first card SHALL be "Aman dipakai hari ini" (or its unavailable state) with the remaining amount and days, a labelled Dipakai / Terjadwal / Aman split and the waiting amount, each figure explainable as a bottom sheet
- **AND** "Kerjakan dulu" SHALL follow, then the period's keluar and masuk, Kekayaan bersih, today's transactions and one insight
- **AND** Kakeibo SHALL make all five classifications and "Belum diklasifikasi" identifiable where it is shown
- **AND** secondary analytical charts SHALL remain on Analisis, and the phone tab bar SHALL never cover the last card

#### Scenario: Viewing home dashboard on mobile phone
- **WHEN** a user opens Beranda at 390 px
- **THEN** the interface shows the safe-to-spend card, "Kerjakan dulu", the period figures and today's activity in one column without the sidebar, without horizontal scrolling, and with every target at least 24 by 24 CSS pixels

#### Scenario: Phone figures match desktop
- **WHEN** the same owner opens Beranda at 390 px and at 1440 px at the same moment
- **THEN** keluar, masuk, the waiting amount, the remaining amount and the safe-to-spend figure SHALL be identical in both layouts

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
The `/ledger` screen SHALL display transactions in chronological date groups on mobile viewports.

#### Scenario: Required behavior and constraints
- **WHEN** an owner views or interacts with the ledger on a mobile screen
- **THEN** the following required behavior and constraints hold:

  The `/ledger` screen SHALL display transactions in chronological date groups on mobile viewports:
  1. Transactions SHALL be grouped under sticky date headers (`Hari Ini`, `Kemarin`, `D MMMM YYYY`).
  2. Tapping any transaction row SHALL open a bottom-sheet drawer displaying full details, category tagging, account breakdown, and 1-tap delete/edit options.
  3. Filtering SHALL be accessible via horizontal type filter chips (`Semua`, `Pengeluaran`, `Pemasukan`, `Transfer`) and a bottom filter sheet for accounts and categories.

#### Scenario: Inspecting transaction detail on mobile
- **WHEN** a user taps a transaction item in the mobile ledger
- **THEN** a bottom sheet displays the transaction details, linked accounts, receipt image, and action buttons without navigating away from the list
