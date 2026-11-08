## MODIFIED Requirements

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
