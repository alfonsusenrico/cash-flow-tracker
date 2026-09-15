# Capability: Pocket Reordering

## ADDED Requirements

### Requirement: Sibling Pocket Drag-and-Drop
The system SHALL allow users to reorder child pockets within an expanded parent account card using drag-and-drop.

#### Scenario: User drags a pocket to a new position within the same parent account
- **WHEN** a user drags pocket "BBCA" and drops it above pocket "BBRI" under parent account "Stockbit"
- **THEN** the UI SHALL immediately move "BBCA" ahead of "BBRI" in the list
- **AND** the system SHALL send the updated pocket ID sequence to `POST /api/accounts/reorder`
- **AND** the new position SHALL persist upon page refresh

#### Scenario: Dragging a pocket does not trigger parent account drag
- **WHEN** a user initiates a drag gesture on a child pocket
- **THEN** event propagation SHALL be stopped so the parent account card is not dragged

#### Scenario: Visual feedback during pocket dragging
- **WHEN** a pocket card is actively being dragged
- **THEN** the dragged card SHALL display with reduced opacity and slight scale reduction
- **AND** the hovered sibling drop target SHALL display an active highlight ring

### Requirement: Account Card Alignment and Compact Height
The system SHALL ensure account cards in the grid have horizontally aligned divider lines and a compact pocket list height showing a minimum of 2 pocket cards.

#### Scenario: Visual alignment of account card dividers
- **WHEN** multiple account cards (e.g. BCA, Jago) are displayed side-by-side in the accounts grid
- **THEN** the divider line above the child pockets accordion SHALL align horizontally at the same vertical position
- **AND** the child pockets container SHALL limit its maximum height to approximately 240px to 260px, showing at least two pocket cards with scrollable overflow
