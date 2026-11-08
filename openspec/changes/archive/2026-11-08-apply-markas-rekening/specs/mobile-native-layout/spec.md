## MODIFIED Requirements

### Requirement: Phone Rekening & Kantong Composition
Below 768 px, Rekening & Kantong SHALL show the hero, its legend and the two controls first, then Uang harian as a sideways-scrolling wallet of 280 px tiles, then every other group with one tile per row. Every tile SHALL open its detail in a bottom sheet, "Perbarui harga" SHALL sit in the "…" menu, and reordering SHALL use "Atur urutan" with move buttons.

#### Scenario: Rekening on a phone
- **WHEN** the owner opens `/accounts` at 390 px
- **THEN** the hero SHALL come first, the Uang harian tiles SHALL scroll sideways inside their own row, no page SHALL overflow horizontally, and tapping a tile SHALL open its bottom sheet with its actions without scrolling the page

#### Scenario: Closing a sheet returns focus
- **WHEN** a keyboard or switch user closes a tile's bottom sheet with its close control or Escape
- **THEN** focus SHALL return to the tile that opened it

#### Scenario: Smallest width
- **WHEN** the page is shown at 320 px
- **THEN** the hero figure SHALL be 32 px, the controls SHALL scroll inside their boxes, and nothing SHALL overflow the page
