## MODIFIED Requirements

### Requirement: Phone Rekening & Kantong Composition
At 760 px and below, Rekening & Kantong SHALL use a composition built for phones: the KPI cards first as a two-by-two grid, then "Komposisi aset", then the account cards in one column with their pockets inside, each pocket's balance on its own line beside its name. Every card head and pocket row SHALL open its actions in a bottom sheet, and reordering SHALL use "Ubah urutan" with move controls.

#### Scenario: Rekening on a phone
- **WHEN** the owner opens `/accounts` at 390 px
- **THEN** the KPI cards SHALL come first, no page SHALL overflow horizontally, and tapping a pocket SHALL open its bottom sheet with its actions without scrolling the page

#### Scenario: Closing a sheet returns focus
- **WHEN** a keyboard or switch user closes a row's bottom sheet with its close control or Escape
- **THEN** focus SHALL return to the row that opened it
