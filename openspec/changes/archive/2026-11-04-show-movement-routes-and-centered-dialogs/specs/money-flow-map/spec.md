## ADDED Requirements

### Requirement: Peta Aliran Details Open Centered
The node and link drawers of Peta Aliran SHALL open as a centered dialog above 760 px and as a bottom sheet at 760 px and below. The map SHALL keep its full width while one is open.

#### Scenario: Link detail on a wide window
- **WHEN** the owner opens the link from "Bank Utama" to Kebutuhan at 1440 px
- **THEN** its rows SHALL be listed in a centered dialog over the dimmed page, the map behind it SHALL keep its width, and Esc SHALL close the dialog and return focus to the link

#### Scenario: Node detail on a phone
- **WHEN** the owner opens a node at 390 px
- **THEN** its detail SHALL be a bottom sheet with focus inside it
