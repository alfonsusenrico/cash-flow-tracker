# mobile-native-layout Specification

## Purpose
Define dedicated mobile home, quick-capture and ledger compositions for everyday finance tracking.

## Requirements

### Requirement: Dedicated Mobile Home Information Architecture
The web application SHALL render a dedicated mobile-first layout when viewed on viewports below 1024px.

#### Scenario: Required behavior and constraints
- **WHEN** the application renders the home screen below 1024 pixels
- **THEN** the following required behavior and constraints hold:

  The web application SHALL render a dedicated mobile-first layout when viewed on viewports below 1024px:
  1. The top header SHALL feature a compact sticky bar containing greeting, statement month stepper pill, balance visibility toggle, and settings trigger.
  2. The primary balance tile SHALL present liquid funds and invested assets with a toggle and immediate action pills: `[ + Catat ]`, `[ ⇄ Pindah Saldo ]`, `[ ⚡ Alokasi ]`.
  3. The Kakeibo 50/30/20 allocation SHALL be presented as a compact, single-row segmented pace bar showing Kebutuhan, Keinginan, and Tabungan distributions.
  4. Recent activity SHALL be positioned directly below the pulse glance in a clean, scroll-efficient timeline with colored transaction glyphs and counterparty details.
  5. Secondary analytical charts (e.g. cumulative trendlines, spending heatmaps, burn cadence) SHALL be deferred to dedicated sub-views or the Analisis screen to eliminate infinite vertical scrolling on the home view.

#### Scenario: Viewing home dashboard on mobile phone
- **WHEN** a user accesses the dashboard on a smartphone (e.g. 390px width)
- **THEN** the interface displays the compact balance card, thumb-zone action buttons, Kakeibo pace bar, and recent transactions without requiring multi-page scrolling
- **AND** desktop command-center sidebars and multi-column matrices are not displayed

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
