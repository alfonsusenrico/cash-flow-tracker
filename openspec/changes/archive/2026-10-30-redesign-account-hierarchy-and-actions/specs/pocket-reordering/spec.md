# Spec Delta

## MODIFIED Requirements

### Requirement: Sibling Pocket Drag-and-Drop
The system SHALL allow users to reorder child pockets within an expanded parent account group using drag-and-drop and explicit keyboard- and touch-operable move controls. Only sibling pockets SHALL be reordered, and the resulting sequence SHALL persist through `POST /api/accounts/reorder`.

#### Scenario: User drags a pocket to a new position within the same parent account
- **WHEN** a user drags pocket "BBCA" and drops it above pocket "BBRI" under parent account "Stockbit"
- **THEN** the UI SHALL immediately move "BBCA" ahead of "BBRI" in the list
- **AND** the system SHALL send the updated pocket ID sequence to `POST /api/accounts/reorder`
- **AND** the new position SHALL persist upon page refresh

#### Scenario: Dragging a pocket does not trigger parent account drag
- **WHEN** a user initiates a drag gesture on a child pocket
- **THEN** event propagation SHALL be stopped so the parent account group is not dragged

#### Scenario: Visual feedback during pocket dragging
- **WHEN** a pocket row is actively being dragged
- **THEN** the dragged row SHALL be visibly distinguished from stationary rows
- **AND** the hovered sibling drop target SHALL display a clear active highlight

#### Scenario: Moving a pocket without dragging
- **WHEN** a keyboard or touch user activates a pocket's move up or move down control
- **THEN** that pocket SHALL change position only among its siblings, the new order SHALL persist after refresh, and unavailable boundary moves SHALL not be offered as active actions

### Requirement: Account Card Alignment and Compact Height
The Accounts page SHALL present top-level accounts as equal-height cards in persisted order. Each card SHALL keep its summary and actions visible above a bounded pocket area. An expanded card SHALL present child pockets as compact, scrollable rows without separate pocket mini-cards. Cards without pockets SHALL retain the same height and alignment.

#### Scenario: Visual alignment of account card dividers
- **WHEN** multiple account groups are displayed in the Accounts page
- **THEN** cards align at the top and bottom whether or not they contain pockets, with summary and actions in a consistent position

#### Scenario: Long pocket list remains readable
- **WHEN** a parent account has twelve child pockets and its group is expanded
- **THEN** all twelve pockets are reachable by keyboard and touch scrolling within the card, their values and actions remain readable, and the account summary remains visible
