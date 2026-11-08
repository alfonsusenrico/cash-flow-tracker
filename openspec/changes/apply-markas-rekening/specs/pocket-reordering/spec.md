## MODIFIED Requirements

### Requirement: Sibling Pocket Drag-and-Drop
The web SHALL let the owner reorder a parent's pockets as tiles inside its group in "Atur urutan" mode, by dragging on wide screens, by Alt + arrow keys, and by move buttons on phones. Only tiles of one group SHALL be reordered together, and the order SHALL persist through the saved layout. `POST /api/accounts/reorder` SHALL keep accepting and persisting a sibling id sequence.

#### Scenario: User drags a pocket to a new position within the same parent account
- **WHEN** in Atur urutan under Per rekening, the owner drags the tile "BBCA" before "BBRI" inside the "Stockbit" group
- **THEN** the page SHALL immediately show "BBCA" before "BBRI"
- **AND** the order SHALL be saved through the layout API
- **AND** the new position SHALL persist upon page refresh

#### Scenario: Dragging a pocket does not trigger parent account drag
- **WHEN** a user starts dragging a tile
- **THEN** only the tile SHALL move, and its group SHALL stay in place

#### Scenario: Visual feedback during pocket dragging
- **WHEN** a tile is actively being dragged
- **THEN** the dragged tile SHALL be distinguished from stationary tiles by more than colour
- **AND** the drop position SHALL be shown by a straight bar before the target

#### Scenario: Moving a pocket without dragging
- **WHEN** a keyboard or touch user moves a tile with Alt + an arrow key or its move buttons
- **THEN** that tile SHALL change position only within its group, the new order SHALL persist after refresh, focus SHALL stay on the moved tile or button, and unavailable boundary moves SHALL not be offered as active actions

## REMOVED Requirements

### Requirement: Account Card Alignment and Compact Height
**Reason**: Rekening & Kantong no longer shows top-level accounts as two-column cards holding pocket rows; every pocket, product and account is a tile of the same size in its group (`accounts-wallet-view` "Rekening Groups and Tiles").
**Migration**: Alignment, tabular figures and long lists are covered by "Rekening Groups and Tiles" and the measurement checklist of `apply-markas-rekening`; a parent with many pockets wraps its tiles in the grid.
