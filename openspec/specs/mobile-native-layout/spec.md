# mobile-native-layout Specification

## Purpose
TBD - created by archiving change mobile-native-ui-and-layout. Update Purpose after archive.

## Requirements

### Requirement: Dedicated Mobile Home Information Architecture
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
The application SHALL render the quick capture transaction entry interface as a native bottom sheet on mobile screens:
1. The bottom sheet SHALL slide up from the bottom edge with rounded top corners (`rounded-t-3xl`) and include a touch-friendly drag-down handle bar.
2. The numeric input SHALL feature high-visibility typography with tabular figures and a 3x4 touch keypad with arithmetic operators (`+`, `-`).
3. The category selector SHALL be rendered as a horizontal scrollable row of touch-friendly icon chips.
4. The Kakeibo classification pills (`Kebutuhan`, `Keinginan`, `Tabungan`) SHALL be accessible with 1-tap thumb targets.

#### Scenario: Opening quick capture on mobile
- **WHEN** a user taps the center `+` button on the mobile bottom navigation
- **THEN** the quick capture bottom sheet slides up smoothly from the bottom of the screen
- **AND** the numeric keypad and category chips are immediately interactable in the thumb zone

### Requirement: Mobile Date-Grouped Transaction Ledger
The `/ledger` screen SHALL display transactions in chronological date groups on mobile viewports:
1. Transactions SHALL be grouped under sticky date headers (`Hari Ini`, `Kemarin`, `D MMMM YYYY`).
2. Tapping any transaction row SHALL open a bottom-sheet drawer displaying full details, category tagging, account breakdown, and 1-tap delete/edit options.
3. Filtering SHALL be accessible via horizontal type filter chips (`Semua`, `Pengeluaran`, `Pemasukan`, `Transfer`) and a bottom filter sheet for accounts and categories.

#### Scenario: Inspecting transaction detail on mobile
- **WHEN** a user taps a transaction item in the mobile ledger
- **THEN** a bottom sheet displays the transaction details, linked accounts, receipt image, and action buttons without navigating away from the list
