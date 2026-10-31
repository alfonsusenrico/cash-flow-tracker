## MODIFIED Requirements

### Requirement: Responsive Settings Entry
The application SHALL show one direct Settings entry in the desktop sidebar and SHALL NOT duplicate it in the desktop top bar. On mobile/tablet, a top-bar menu button SHALL open the navigation drawer, where Settings SHALL remain reachable. Each Settings entry SHALL link to the `/settings` page. Icon-only menu and settings controls SHALL have meaningful accessible names and visible keyboard focus. This replaces the older mobile top-bar Settings shortcut without removing mobile access.

#### Scenario: Opening Settings on desktop
- **WHEN** a desktop user views the application header and sidebar
- **THEN** Settings is available in the sidebar and no Settings gear is shown in the header
- **AND** activating the sidebar Settings entry navigates to `/settings`, where the entry is marked as the current page

#### Scenario: Opening Settings from mobile navigation
- **WHEN** a mobile user activates the top-bar menu button and then the Settings control in the drawer
- **THEN** the drawer opens, the Settings page opens from that drawer and the drawer closes, and the header shows no duplicate Settings gear
