# executive-sidebar-layout Specification

## Purpose
Define persistent desktop navigation, responsive mobile navigation and a usable full-width financial workbench.

## Requirements

### Requirement: Persistent Left-Hand Executive Sidebar Navigation
The web app SHALL provide a persistent left-hand sidebar inside the workspace window for route navigation, in the order defined by `tactile-pulse-ui`, with count badges, Pengaturan and the account card.

#### Scenario: Required behavior and constraints
- **WHEN** an owner navigates the desktop dashboard
- **THEN** the following required behavior and constraints hold:

  The web app SHALL provide a persistent left-hand sidebar inside the workspace window:
  1. The top of the sidebar SHALL show the product mark and name and the collapse button. It SHALL NOT contain an avatar, a quick-capture button, a theme toggle or a balance-privacy toggle; "+ Catat", theme, notifications and the account menu SHALL reside in the workspace top bar.
  2. The sidebar SHALL list, without group headings, `Beranda` `/`, `Tinjau` `/review`, `Transaksi` `/ledger`, `Analisis` `/insights`, `Laporan` `/laporan`, `Rekening & Kantong` `/accounts` and `Target & Tagihan` `/goals` in Bahasa Indonesia with icons and active-route highlighting.
  3. The active link SHALL use the coral primary fill with its contrast ink and expose `aria-current="page"`.
  4. `Tinjau` SHALL carry the review attention count and `Laporan` a badge of 1 while the latest report is ready and still fresh by the rule the report-ready notice uses today, each also announced in words in the link's accessible name.
  5. The sidebar foot SHALL show `Pengaturan` (`/settings`) and the account card with the owner's name and username, which opens the account menu. It SHALL NOT show the companion status or a `Bantuan` entry; both live in Pengaturan.

#### Scenario: Navigating via streamlined sidebar
- **WHEN** the user navigates between views using the left sidebar
- **THEN** the sidebar shows the seven links in order, the active link marked with `aria-current="page"`, the badges, Pengaturan and the account card, and no group heading, companion status, Bantuan, quick-capture button or theme toggle

#### Scenario: Logging out from user identity card
- **WHEN** the user chooses "Keluar" in the account menu opened from the sidebar's account card or the workspace top bar
- **THEN** the active session terminates and redirects to the login screen

#### Scenario: Tanya in the AI group
- **WHEN** the owner opens any page at 1440 px, or opens a bookmarked `/tanya`
- **THEN** the sidebar SHALL show no AI group and no Tanya link, and `/tanya` SHALL redirect to Beranda

### Requirement: Collapsible Desktop Sidebar and Responsive Mobile Drawer
The workspace SHALL adapt the sidebar to the viewport width:
1. Above 1180 px the sidebar SHALL be about 248 px wide with a collapse toggle to a 76 px icon rail; the choice SHALL persist in the browser.
2. From 761 px to 1180 px the sidebar SHALL always be the icon rail, without the toggle.
3. At 760 px and below the sidebar SHALL be hidden and the phone tab bar SHALL provide navigation; there SHALL be no slide-over drawer.

#### Scenario: Toggling sidebar collapse on desktop
- **WHEN** the user activates the collapse button at the top of the sidebar at 1440 px
- **THEN** the sidebar becomes the 76 px icon rail, group headings and labels hide, icons stay centred, every icon keeps an accessible name and a hover tooltip, badges show as small counters, and the choice is restored on the next visit

#### Scenario: Opening mobile drawer
- **WHEN** the user opens the app at 390 px
- **THEN** no hamburger button or slide-over drawer is shown, and the phone tab bar and its "Lainnya" sheet reach every page

### Requirement: Fluid Full-Width Workbench Layout
Above 760 px the signed-in pages SHALL render inside one floating workspace window on the canvas: rounded corners, the window shadow token, up to 1680 px wide and centred, holding the sidebar, the main column and, when open, a right-hand drawer. The main column SHALL adapt its grids to its own width. At 760 px and below the window SHALL fill the screen without corners or shadow.

#### Scenario: Viewing dashboard on wide monitor
- **WHEN** the dashboard is viewed at 1440 px or wider
- **THEN** the window sits on the canvas with a visible margin, the cards and tables fill the main column, the window is at most 1680 px wide, and no horizontal scrolling occurs at 1440, 1280, 1024 or 768 px

### Requirement: Workspace Top Bar
Every signed-in page SHALL start its main column with a top bar: the page title and a short subtitle, page-specific controls such as a period stepper, and the global actions search (with the ⌘K/Ctrl+K hint), theme, notifications, "+ Catat" and the account menu. At 760 px and below search, notifications and "+ Catat" SHALL move out of the bar.

#### Scenario: Search
- **WHEN** the owner types "warung" in the top-bar search and presses Enter
- **THEN** Transaksi SHALL open with `q=warung` applied and visible

#### Scenario: Account menu
- **WHEN** the owner opens the account menu
- **THEN** it SHALL show the username, "Sembunyikan saldo" or "Tampilkan saldo", "Pengaturan" and "Keluar", operable by keyboard with focus returned to the menu button on Esc

#### Scenario: Notifications
- **GIVEN** a monthly report is ready and a plan is due tomorrow
- **WHEN** the owner opens notifications
- **THEN** both SHALL be listed with their date and a link, and the bell SHALL announce the number of items in its accessible name

### Requirement: Phone Tab Bar
At 760 px and below the app SHALL show a fixed bottom tab bar with Beranda, Tinjau (with its count badge), Catat (a dark centre button that opens capture), Transaksi and Lainnya. Lainnya SHALL open a sheet with every other page and Keluar. The bar SHALL respect the safe-area inset and never hide focused content.

#### Scenario: Reaching every page on a phone
- **WHEN** the owner opens "Lainnya" at 390 px
- **THEN** the sheet SHALL list Rekening & Kantong, Target & Tagihan, Analisis, Laporan, Pengaturan and Keluar, without Tanya or Bantuan, trap focus while open and return focus to "Lainnya" when closed

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

### Requirement: Global Keyboard Shortcuts
The workspace SHALL support ⌘K/Ctrl+K to focus search, N to open capture, ? to open the shortcut help and Esc to close the top-most popover, sheet, drawer or dialog. Single-key shortcuts SHALL be ignored while typing in a field and SHALL be switchable off in the help dialog; the choice SHALL persist in the browser. The help SHALL also list the keys of the open page, and the switch SHALL cover them.

#### Scenario: Typing in a field
- **WHEN** the owner types "n" into the search field
- **THEN** capture SHALL NOT open

#### Scenario: Turning single-key shortcuts off
- **WHEN** the owner turns off "Pintasan satu tombol" and later presses N or ? outside a field
- **THEN** nothing SHALL happen, while ⌘K/Ctrl+K and Esc keep working, and the setting SHALL survive a reload

#### Scenario: Page keys in the help
- **GIVEN** a page that registers its own keys, such as Tinjau
- **WHEN** the owner opens the help on that page and on Beranda
- **THEN** that page's keys SHALL be listed under the page's name only while it is open, and the switch caption SHALL say that it also turns them off

### Requirement: Companion Status
Pengaturan · HP & perangkat SHALL show at its top whether a paired phone is ready, from the owner's devices and their latest health report: ready with the time of its last contact, needs attention with the reason, or no phone paired with a link to pairing. It SHALL be text, not colour alone.

#### Scenario: No phone paired
- **WHEN** the owner has no paired device
- **THEN** the status SHALL say that no phone is connected and link to the device settings

#### Scenario: Status in the rail
- **WHEN** the sidebar is the icon rail or the full sidebar
- **THEN** the sidebar SHALL show no companion status, and the status SHALL be read in Pengaturan · HP & perangkat
