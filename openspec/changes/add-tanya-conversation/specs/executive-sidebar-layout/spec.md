# Spec Delta

## MODIFIED Requirements

### Requirement: Persistent Left-Hand Executive Sidebar Navigation
The web app SHALL provide a persistent left-hand sidebar inside the workspace window for route navigation, grouped and ordered as defined by `tactile-pulse-ui`, with count badges, companion status, Bantuan and Pengaturan.

#### Scenario: Required behavior and constraints
- **WHEN** an owner navigates the desktop dashboard
- **THEN** the following required behavior and constraints hold:

  The web app SHALL provide a persistent left-hand sidebar inside the workspace window:
  1. The top of the sidebar SHALL show the product mark and name and the collapse button. It SHALL NOT contain an avatar, a quick-capture button, a theme toggle or a balance-privacy toggle; "+ Catat", theme, notifications and the account menu SHALL reside in the workspace top bar.
  2. The sidebar SHALL list the workspace groups and links in Bahasa Indonesia with icons and active-route highlighting: Harian (`Beranda` `/`, `Tinjau` `/review`, `Transaksi` `/ledger`), AI (`Tanya` `/tanya`), Uang (`Rekening & Kantong` `/accounts`, `Target & Tagihan` `/goals`) and Laporan (`Analisis` `/insights`, `Laporan` `/laporan`).
  3. The active link SHALL use the lime fill with its lime ink and expose `aria-current="page"`.
  4. `Tinjau` SHALL carry the review attention count and `Laporan` a badge of 1 while the latest report is ready and still fresh by the rule the report-ready notice uses today, each also announced in words in the link's accessible name.
  5. The sidebar foot SHALL show the companion status, `Bantuan` (keyboard shortcuts and help) and `Pengaturan` (`/settings`).

#### Scenario: Navigating via streamlined sidebar
- **WHEN** the user navigates between views using the left sidebar
- **THEN** the sidebar shows the grouped links, the active link marked with `aria-current="page"`, the badges, the companion status, Bantuan and Pengaturan, and no quick-capture button, avatar or theme toggle

#### Scenario: Logging out from user identity card
- **WHEN** the user chooses "Keluar" in the account menu of the workspace top bar
- **THEN** the active session terminates and redirects to the login screen

#### Scenario: Tanya in the AI group
- **WHEN** the owner opens any page at 1440 px
- **THEN** the sidebar SHALL show the group AI between Harian and Uang with the single link "Tanya" to `/tanya`, and on `/tanya` that link SHALL carry `aria-current="page"`

### Requirement: Phone Tab Bar
At 760 px and below the app SHALL show a fixed bottom tab bar with Beranda, Tinjau (with its count badge), Catat (a dark centre button that opens capture), Transaksi and Lainnya. Lainnya SHALL open a sheet with every other page, Bantuan and Keluar. The bar SHALL respect the safe-area inset and never hide focused content.

#### Scenario: Reaching every page on a phone
- **WHEN** the owner opens "Lainnya" at 390 px
- **THEN** the sheet SHALL list Tanya, Rekening & Kantong, Target & Tagihan, Analisis, Laporan, Pengaturan, Bantuan and Keluar, trap focus while open and return focus to "Lainnya" when closed

#### Scenario: Focus not obscured
- **WHEN** a keyboard user tabs to the last row of a long list at 390 px
- **THEN** the focused row SHALL scroll fully above the tab bar

#### Scenario: Active tab
- **WHEN** the owner is on `/ledger` at 390 px
- **THEN** the Transaksi tab SHALL carry `aria-current="page"`, and every tab label SHALL measure at least 4.5:1 against the bar in both themes

### Requirement: Route Compatibility
The redesign SHALL keep every existing route and query parameter working: `/`, `/review`, `/ledger` with its filters, `/insights`, `/laporan` with `basis` and `start`, `/accounts`, `/goals`, `/settings` and its sub-pages, `/tanya`, `/auth/login` and the `/dashboard` redirect. A page not yet redesigned SHALL render its current content inside the new shell.

#### Scenario: Old ledger link
- **WHEN** the owner opens `/ledger?type=expense&kakeibo=unclassified`
- **THEN** Transaksi SHALL open with both filters active, inside the new shell

#### Scenario: Report-ready link
- **WHEN** the owner opens the `url` returned by `GET /api/reports/monthly/latest`
- **THEN** the matching report SHALL open

#### Scenario: Tanya route
- **WHEN** a signed-in owner opens `/tanya` directly
- **THEN** the Tanya page SHALL open inside the shell with an empty thread, and a signed-out visitor SHALL be sent to sign-in as on every other page
