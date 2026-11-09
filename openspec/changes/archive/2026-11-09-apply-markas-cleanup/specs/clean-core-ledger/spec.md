## MODIFIED Requirements

### Requirement: Responsive Settings Entry
The application SHALL show one direct Settings entry in the sidebar and SHALL NOT duplicate it in the top bar. Below 768 px, where the sidebar is hidden, Settings SHALL be reachable from the phone tab bar's "Lainnya" sheet; there is no navigation drawer. Each Settings entry SHALL link to the `/settings` page. Icon-only controls SHALL have meaningful accessible names and visible keyboard focus.

#### Scenario: Opening Settings on desktop
- **WHEN** a desktop user views the application header and sidebar
- **THEN** Settings is available in the sidebar and no Settings gear is shown in the header
- **AND** activating the sidebar Settings entry navigates to `/settings`, where the entry is marked as the current page

#### Scenario: Opening Settings from mobile navigation
- **WHEN** a phone user at 390 px activates "Lainnya" in the tab bar and then "Pengaturan"
- **THEN** the sheet closes, the Settings page opens, no hamburger button or drawer exists, and the header shows no Settings gear
