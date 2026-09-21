## MODIFIED Requirements

### Requirement: 3-Screen Information Architecture
The web application SHALL organize user navigation into a dual-mode information architecture:
1. On desktop viewports ($\ge 1024\text{px}$), the system SHALL host navigation inside a persistent left-hand sidebar accessing 5 executive workspaces (`/`, `/ledger`, `/insights`, `/accounts`, `/goals`) with a multi-column command center layout.
2. On mobile viewports ($< 1024\text{px}$), the system SHALL host navigation inside a dedicated bottom navigation bar (`BottomNav`) accessing the 4 core views (`Beranda`, `Transaksi`, `Analisis`, `Dompet`) plus a center quick-add trigger, replacing the slide-over hamburger drawer with native thumb-zone navigation.

#### Scenario: Navigating between executive dashboard screens
- **WHEN** a user selects any item from the persistent left sidebar navigation on desktop
- **THEN** the dashboard switches views instantly without full page reloads and maintains active timeframe filters

#### Scenario: Mobile responsive dashboard navigation
- **WHEN** a user opens the dashboard on a mobile viewport
- **THEN** the navigation displays a dedicated thumb-friendly bottom bar with active indicator badges
- **AND** the desktop sidebar and hamburger slide-over menu are hidden in favor of direct 1-tap tab switching
