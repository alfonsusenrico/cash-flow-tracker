## ADDED Requirements

### Requirement: Persistent Left-Hand Executive Sidebar Navigation
The web dashboard SHALL replace the top tab navigation bar with a persistent left-hand sidebar navigation bar containing:
1. Brand header with application emblem and wordmark ("CashFlow").
2. Core navigation links with icons and active route highlighting:
   - `Overview` (`/`)
   - `Transactions` (`/ledger`)
   - `Analytics` (`/insights`)
   - `Vault & Accounts` (`/accounts`)
   - `Goals & Debts` (`/goals`)
3. Primary action trigger button (`+ Record Entry` / `N`).
4. Bottom user section containing username, currency code (`IDR`), privacy balance toggle, theme toggle, and logout button.

#### Scenario: Navigating via sidebar
- **WHEN** the user clicks any route in the left sidebar
- **THEN** the active indicator transitions to the selected route and the main workbench area renders the corresponding view without page reloading

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
