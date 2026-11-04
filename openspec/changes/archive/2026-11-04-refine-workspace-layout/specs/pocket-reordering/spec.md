## MODIFIED Requirements

### Requirement: Sibling Pocket Drag-and-Drop
The system SHALL allow users to reorder child pockets inside their parent account's card, by dragging a pocket's handle on wide screens and with explicit keyboard- and touch-operable move controls (shown on hover and focus on wide screens, in "Ubah urutan" on phones). Only sibling pockets SHALL be reordered, and the resulting sequence SHALL persist through `POST /api/accounts/reorder`.

#### Scenario: User drags a pocket to a new position within the same parent account
- **WHEN** a user drags pocket "BBCA" by its handle and drops it above pocket "BBRI" inside the "Stockbit" card
- **THEN** the UI SHALL immediately move "BBCA" ahead of "BBRI" in the list
- **AND** the system SHALL send the updated pocket ID sequence to `POST /api/accounts/reorder`
- **AND** the new position SHALL persist upon page refresh

#### Scenario: Dragging a pocket does not trigger parent account drag
- **WHEN** a user initiates a drag gesture on a child pocket row
- **THEN** event propagation SHALL be stopped so the parent account row is not dragged

#### Scenario: Visual feedback during pocket dragging
- **WHEN** a pocket row is actively being dragged
- **THEN** the dragged row SHALL be visibly distinguished from stationary rows by more than colour
- **AND** the hovered sibling drop target SHALL display a clear active highlight

#### Scenario: Moving a pocket without dragging
- **WHEN** a keyboard or touch user activates a pocket's move up or move down control
- **THEN** that pocket SHALL change position only among its siblings, the new order SHALL persist after refresh, focus SHALL stay on the moved pocket's control, and unavailable boundary moves SHALL not be offered as active actions

### Requirement: Account Card Alignment and Compact Height
The Accounts page SHALL present top-level accounts as cards in persisted order, flowing in two columns on wide screens without stretching a card to its neighbour's height. Each card SHALL hold its pockets or products as compact rows with name, optional detail line and balance right-aligned in tabular figures. Cards without pockets SHALL keep the same head layout as cards with pockets.

#### Scenario: Visual alignment of account card dividers
- **WHEN** multiple accounts are displayed in the Accounts page
- **THEN** every card head shows badge, kind and balance in the same positions whether or not it holds pockets, and pocket balances are right-aligned with tabular figures

#### Scenario: Long pocket list remains readable
- **WHEN** a parent account has twelve child pockets
- **THEN** all twelve pockets are listed inside its card and reachable by keyboard and touch, their values and detail actions remain readable, and the card stays identifiable as their parent for assistive technology
