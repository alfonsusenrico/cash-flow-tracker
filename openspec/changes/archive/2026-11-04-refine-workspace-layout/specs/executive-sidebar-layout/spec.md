## MODIFIED Requirements

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

### Requirement: Companion Status
Pengaturan · HP & perangkat SHALL show at its top whether a paired phone is ready, from the owner's devices and their latest health report: ready with the time of its last contact, needs attention with the reason, or no phone paired with a link to pairing. It SHALL be text, not colour alone.

#### Scenario: No phone paired
- **WHEN** the owner has no paired device
- **THEN** the status SHALL say that no phone is connected and link to the device settings

#### Scenario: Status in the rail
- **WHEN** the sidebar is the icon rail or the full sidebar
- **THEN** the sidebar SHALL show no companion status, and the status SHALL be read in Pengaturan · HP & perangkat
