## MODIFIED Requirements

### Requirement: Dedicated Mobile Home Information Architecture
The web application SHALL render a dedicated mobile-first layout when viewed on viewports below 1024px.

#### Scenario: Required behavior and constraints
- **WHEN** the application renders the home screen below 1024 pixels
- **THEN** it SHALL show a compact sticky header with greeting, period control, balance visibility toggle and settings
- **AND** the balance tile SHALL present liquid funds and invested assets with explicit record and movement actions, without payroll bulk execution
- **AND** Kakeibo SHALL make five classifications identifiable and explain any optional grouped benchmark
- **AND** recent activity SHALL follow the primary summary in a compact timeline
- **AND** secondary analytical charts SHALL remain in dedicated sub-views or Analisis

#### Scenario: Viewing home dashboard on mobile phone
- **WHEN** a user accesses the dashboard on a smartphone
- **THEN** the interface displays the compact balance, relevant actions, classification summary and recent activity without desktop sidebars
