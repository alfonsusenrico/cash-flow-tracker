## MODIFIED Requirements

### Requirement: Floating Capsule Navigation
The system SHALL render a floating rounded capsule navigation dock on mobile screens (< 1024px) with an active pill indicator. The dock SHALL paint its fill and label inks from dedicated dock tokens, so that every label, active or inactive, keeps a text contrast of at least 4.5:1 against the dock in both the light and the dark theme.

#### Scenario: Mobile viewport navigation docking
- **GIVEN** the application is rendered on a viewport width below 1024px
- **WHEN** a user navigates between routes (`/`, `/ledger`, `/insights`, `/accounts`)
- **THEN** the active tab SHALL be wrapped in a highlighted pill container
- **AND** the dock SHALL float above the bottom edge with backdrop-filter blur.

#### Scenario: Inactive labels are readable in both themes
- **GIVEN** a 390 px viewport in the light or the dark theme
- **WHEN** the dock is shown on any page
- **THEN** each inactive label SHALL measure at least 4.5:1 against the dock fill composited over the page surface
