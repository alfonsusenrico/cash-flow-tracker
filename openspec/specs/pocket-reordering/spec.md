# pocket-reordering Specification

## Purpose
Allow sibling pockets to be reordered while keeping account presentation aligned and compact.

## Requirements

### Requirement: Sibling Pocket Drag-and-Drop
The system SHALL allow users to reorder child pockets within their parent account in the Accounts page's "Ubah urutan" mode, using drag-and-drop and explicit keyboard- and touch-operable move controls. Only sibling pockets SHALL be reordered, and the resulting sequence SHALL persist through `POST /api/accounts/reorder`.

#### Scenario: User drags a pocket to a new position within the same parent account
- **WHEN** a user in "Ubah urutan" drags pocket "BBCA" and drops it above pocket "BBRI" under parent account "Stockbit"
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
- **WHEN** a keyboard or touch user activates a pocket's move up or move down control in "Ubah urutan"
- **THEN** that pocket SHALL change position only among its siblings, the new order SHALL persist after refresh, focus SHALL stay on the moved pocket's control, and unavailable boundary moves SHALL not be offered as active actions

### Requirement: Account Card Alignment and Compact Height
The Accounts page SHALL present top-level accounts as rows of the "Uang siap pakai" and "Investasi" tables in persisted order, with every row's name, kind and balance in the same columns. A parent's pockets SHALL be compact indented rows directly below it, without separate pocket cards. Rows with and without pockets SHALL share one row height for the account line.

#### Scenario: Visual alignment of account card dividers
- **WHEN** multiple accounts are displayed in the Accounts page
- **THEN** their names, kinds and balances align in the table's columns whether or not they contain pockets, and balances are right-aligned with tabular figures

#### Scenario: Long pocket list remains readable
- **WHEN** a parent account has twelve child pockets and its pocket rows are expanded
- **THEN** all twelve pockets are reachable by keyboard and touch, their values and drawer actions remain readable, and the parent's row stays identifiable as their parent for assistive technology
