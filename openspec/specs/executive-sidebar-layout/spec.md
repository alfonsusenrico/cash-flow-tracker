# executive-sidebar-layout Specification

## Purpose
TBD - created by archiving change bibit-executive-sidebar-and-power-tools. Update Purpose after archive.
## Requirements
### Requirement: Persistent Left-Hand Executive Sidebar Navigation
The web dashboard SHALL provide a persistent left-hand sidebar navigation bar dedicated to route navigation:
1. The sidebar SHALL NOT contain redundant branding titles, avatars ("CashFlow Financial OS"), or redundant quick transaction entry buttons. Quick transaction entry (`+ Catat Transaksi` / `N`) SHALL reside exclusively in the top global header.
2. The sidebar SHALL contain core navigation links with icons and active route highlighting rendered in simple, everyday Bahasa Indonesia:
   - `Beranda` (`/`)
   - `Transaksi` (`/ledger`)
   - `Analisis` (`/insights`)
   - `Rekening & Saldo` (`/accounts`)
   - `Target & Tagihan` (`/goals`)
3. The bottom user identity section SHALL contain the user's avatar initials, username, currency code, settings trigger, and integrated logout button (`Keluar`).
4. Privacy balance toggle and theme toggle buttons SHALL NOT be duplicated in the sidebar footer; they SHALL reside exclusively in the top global header.

#### Scenario: Navigating via streamlined sidebar
- **WHEN** the user navigates between views using the left sidebar
- **THEN** the sidebar displays only the route navigation links and bottom user identity with logout, without redundant branding banners or redundant "+ Catat Transaksi" buttons

#### Scenario: Logging out from user identity card
- **WHEN** the user clicks the logout icon within the sidebar user identity card
- **THEN** the active session terminates and redirects to the login screen

### Requirement: Collapsible Desktop Sidebar and Responsive Mobile Drawer
The application layout SHALL support flexible viewports:
1. On desktop viewports (width >= 1024px), the sidebar SHALL support a collapse/expand toggle, transitioning between a full sidebar (width ~260px) and a compact icon-only rail (width ~72px) with tooltip labels.
2. On mobile and tablet viewports (width < 1024px), the sidebar SHALL collapse into a slide-over drawer triggered by a top-bar hamburger button.

#### Scenario: Toggling sidebar collapse on desktop
- **WHEN** the user clicks the collapse button at the top of the desktop sidebar
- **THEN** the sidebar width animates to 72px, labels hide, icons remain centered, and hover tooltips appear over each item

#### Scenario: Opening mobile drawer
- **WHEN** the user taps the menu button on a mobile device
- **THEN** the navigation drawer slides in from the left over a dimmed backdrop, allowing immediate route selection

### Requirement: Fluid Full-Width Workbench Layout
The application main workspace SHALL extend to fill the entire horizontal display width without arbitrary container constraints (such as `max-w-7xl` or `max-w-2xl`), adopting the fluid grid layout architecture of `student-enrollment-analysis` (`w-full px-4 sm:px-6 lg:px-8 py-6`).

#### Scenario: Viewing dashboard on wide monitor
- **WHEN** the dashboard is viewed on a 1440px or wider monitor
- **THEN** the charts, bento cards, and tables fluidly expand across the available width, maintaining crisp data density and proportional column grids

