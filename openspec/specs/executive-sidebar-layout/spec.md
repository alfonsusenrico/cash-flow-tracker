# executive-sidebar-layout Specification

## Purpose
Define persistent desktop navigation, responsive mobile navigation and a usable full-width financial workbench.

## Requirements

### Requirement: Persistent Left-Hand Executive Sidebar Navigation
From 768 px the web app SHALL show a persistent left sidebar: the brand row, the pages in three labelled groups in the order of `tactile-pulse-ui`, and a foot with Pengaturan and the user button. The current page SHALL have a neutral fill and a straight accent selection bar and expose `aria-current="page"`. Tinjau and Laporan SHALL carry muted counts that are also announced in words.

#### Scenario: Required behavior and constraints
- **WHEN** an owner navigates the desktop dashboard
- **THEN** the following required behavior and constraints hold:

  1. The 56 px brand row SHALL show the 24 px accent mark, "CashFlow" in Manrope and the collapse button. It SHALL NOT contain an avatar, a quick-capture button, a theme toggle or a balance-privacy toggle; search, "Sembunyikan saldo", theme, notifications and "+ Catat" SHALL reside in the workspace top bar.
  2. `nav` "Menu utama" SHALL list, under the uppercase 12 px labels HARIAN, RINGKASAN and SIMPANAN: `Beranda` `/`, `Tinjau` `/review`, `Transaksi` `/ledger`; `Analisis` `/insights`, `Laporan` `/laporan`; `Rekening & Kantong` `/accounts`, `Target & Tagihan` `/goals`, each with its icon.
  3. Nav items SHALL be 36 px high with a 6 px radius. The current item SHALL have the `--s2` fill, `--ink` text and a straight 2 × 20 px `--accent` bar 8 px left of its fill, and SHALL NOT use an accent fill, a coloured border or a coloured text.
  4. `Tinjau` SHALL show the review attention count and `Laporan` a 1 while the latest report is ready and still fresh by the rule the report-ready notice uses today. Both counts SHALL be 12 px `--muted` text at the item's right and SHALL also be announced in words in the link's accessible name.
  5. The foot, under a 1 px line, SHALL show `Pengaturan` (`/settings`) and the user button with a 32 px initials avatar, the owner's name and username, which opens the account menu. It SHALL NOT show the companion status or a `Bantuan` entry.

#### Scenario: Navigating via streamlined sidebar
- **WHEN** the user navigates between views using the left sidebar at 1440 px
- **THEN** the sidebar shows the three group labels and the seven links in order, the current link with `aria-current="page"`, the grey fill and the 2 × 20 px accent bar, the muted counts, Pengaturan and the user button, and no companion status, Bantuan, quick-capture button or theme toggle

#### Scenario: Logging out from user identity card
- **WHEN** the user chooses "Keluar" in the account menu opened from the sidebar's user button
- **THEN** the active session terminates and redirects to the login screen

#### Scenario: Tanya in the AI group
- **WHEN** the owner opens any page at 1440 px, or opens a bookmarked `/tanya`
- **THEN** the sidebar SHALL show no AI group and no Tanya link, and `/tanya` SHALL redirect to Beranda

#### Scenario: No coral pill
- **WHEN** the computed styles of the current nav item are read in both themes
- **THEN** its background SHALL be `--s2`, its text `--ink`, its border none, and the only accent-coloured element SHALL be the 2 × 20 px bar

### Requirement: Collapsible Desktop Sidebar and Responsive Mobile Drawer
The workspace SHALL adapt the sidebar to the viewport width:
1. From 1024 px the sidebar SHALL be 240 px wide with a collapse toggle to a 64 px icon rail; the choice SHALL persist in the browser.
2. From 768 to 1023 px the sidebar SHALL always be the icon rail, without the toggle.
3. Below 768 px the sidebar SHALL be hidden and the phone header and tab bar SHALL provide navigation; there SHALL be no slide-over drawer.

#### Scenario: Toggling sidebar collapse on desktop
- **WHEN** the user activates the collapse button at 1440 px
- **THEN** the sidebar becomes the 64 px icon rail: group labels become 1 px dividers, labels, counts and names hide, icons stay centred with the selection bar at the rail's left edge, every icon keeps an accessible name including its count in words and a tooltip on hover and keyboard focus, the expand button stays reachable in the brand row, and the choice is restored on the next visit

#### Scenario: Rail below 1024 px
- **WHEN** the owner opens any page at 1023 px and at 1024 px
- **THEN** at 1023 px the sidebar SHALL be the 64 px rail without the collapse button and the page side margin SHALL be 32 px, and at 1024 px the stored choice SHALL apply with a 48 px side margin

#### Scenario: Opening mobile drawer
- **WHEN** the user opens the app at 390 px
- **THEN** no hamburger button or slide-over drawer is shown, and the phone tab bar and its "Lainnya" sheet reach every page

### Requirement: Fluid Full-Width Workbench Layout
Signed-in pages SHALL render on the canvas without a floating window: the sidebar beside a main column whose page frame is at most 1200 px wide (1040 px on Transaksi and Pengaturan), aligned to the left edge of a centred 1200 px frame, with side margins of 48, 32 or 16 px by width and 64 px at the bottom. Details SHALL open as centered dialogs, or bottom sheets on the phone, never as a right-hand column.

#### Scenario: Viewing dashboard on wide monitor
- **WHEN** the dashboard is viewed at 1440 px or wider
- **THEN** there SHALL be no window corners or window shadow, every page title SHALL start at the same x, the frame SHALL be at most 1200 px wide, and no horizontal scrolling occurs at 1440, 1280, 1024, 768, 390 or 320 px

#### Scenario: Opening a detail keeps the main column
- **WHEN** the owner opens a transaction detail on Transaksi at 1440 px
- **THEN** the main column SHALL keep its width and layout behind a dimmed scrim, and the detail SHALL be a centered dialog

#### Scenario: Frame width per page
- **WHEN** Transaksi and Beranda are opened at 1920 px
- **THEN** both titles SHALL start at the same x, the Transaksi frame SHALL be at most 1040 px wide and the Beranda frame at most 1200 px

### Requirement: Workspace Top Bar
From 768 px every signed-in page SHALL start with a sticky 56 px top bar holding only global actions: search with the ⌘K hint, "Sembunyikan saldo", the theme menu, notifications with an unread count and "+ Catat" as the only primary button. The page title and page controls SHALL NOT be in the top bar. Below 768 px the phone header SHALL replace it.

#### Scenario: Search
- **WHEN** the owner types "warung" in the top-bar search and presses Enter
- **THEN** Transaksi SHALL open with `q=warung` applied and visible

#### Scenario: Search on Transaksi
- **WHEN** the owner presses ⌘K or Ctrl+K on Transaksi, and on Beranda
- **THEN** on Transaksi the page's own search field SHALL take focus, on Beranda the top-bar search SHALL take focus, and the top-bar search SHALL be visible on both

#### Scenario: Account menu
- **WHEN** the owner opens the account menu from the user button
- **THEN** it SHALL be a menu showing the name and username, "Profil" and "Keluar", operable with ↑/↓, with focus returned to the user button on Esc

#### Scenario: Notifications
- **GIVEN** a monthly report is ready and a plan is due tomorrow, neither seen in this browser
- **WHEN** the owner opens notifications
- **THEN** both SHALL be listed with their date and a link and marked unread, the bell SHALL announce "Notifikasi, 2 baru" before opening and show no count after it

#### Scenario: Hiding balances from the top bar
- **WHEN** the owner presses "Sembunyikan saldo" in the top bar
- **THEN** the button SHALL expose `aria-pressed="true"`, every amount the app masks today SHALL read "Rp ••••••", and Pengaturan › Tampilan SHALL show the same state

#### Scenario: Theme menu
- **WHEN** the owner opens "Tema" and moves with ↓ to "Gelap" and presses Enter
- **THEN** the menu SHALL list Terang, Gelap and Ikuti sistem as radio items with the current one checked, the theme SHALL switch at once, and focus SHALL return to "Tema"

### Requirement: Phone Tab Bar
Below 768 px the app SHALL show a fixed 64 px bottom tab bar plus the safe-area inset with Beranda, Tinjau (with its count), Catat (an accent block with its label, opening capture), Transaksi and Lainnya. The current tab SHALL show `--ink` and a 2 px accent bar at the bar's top edge. Lainnya SHALL open a sheet with the other pages, Notifikasi and Tema; "Keluar" lives in Pengaturan. The bar SHALL never hide focused content.

#### Scenario: Reaching every page on a phone
- **WHEN** the owner opens "Lainnya" at 390 px
- **THEN** the sheet SHALL list Analisis, Laporan, Rekening & Kantong, Target & Tagihan, Pengaturan, Notifikasi and Tema, without Tanya, Bantuan or Keluar, trap focus while open and return focus to "Lainnya" when closed

#### Scenario: Focus not obscured
- **WHEN** a keyboard user tabs to the last row of a long list at 390 px
- **THEN** the focused row SHALL scroll fully above the tab bar and below the phone header

#### Scenario: Active tab
- **WHEN** the owner is on `/ledger` at 390 px
- **THEN** the Transaksi tab SHALL carry `aria-current="page"` and the 2 px accent bar over the middle half of its top edge, and every tab label SHALL measure at least 4.5:1 against the bar in both themes

#### Scenario: Catat on the tab bar
- **WHEN** the owner taps Catat at 390 px
- **THEN** the capture dialog SHALL open, and the tab SHALL show a 44 × 32 px accent block with a plus and the visible word "Catat"

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
The workspace SHALL support ⌘K/Ctrl+K to focus search, N to open capture, ? to open "Bantuan & pintasan" and Esc to close the top-most popover, menu, sheet or dialog. Single-key shortcuts, including page keys, SHALL be ignored while typing in a field and SHALL be switchable off in Pengaturan › Tampilan; the choice SHALL persist in the browser. The help SHALL list the global keys and the keys of the open page.

#### Scenario: Typing in a field
- **WHEN** the owner types "n" into the search field
- **THEN** capture SHALL NOT open

#### Scenario: Turning single-key shortcuts off
- **WHEN** the owner turns off "Pintasan satu tombol" in Tampilan and later presses N or ? outside a field
- **THEN** nothing SHALL happen, while ⌘K/Ctrl+K and Esc keep working, and the setting SHALL survive a reload

#### Scenario: Page keys in the help
- **GIVEN** a page that registers its own keys, such as Tinjau
- **WHEN** the owner opens the help on that page and on Beranda
- **THEN** that page's keys SHALL be listed with the page's name only while it is open, and the help SHALL say that single-key shortcuts can be turned off in Pengaturan › Tampilan

### Requirement: Companion Status
Pengaturan · HP & perangkat SHALL show at its top whether a paired phone is ready, from the owner's devices and their latest health report: ready with the time of its last contact, needs attention with the reason, or no phone paired with a link to pairing. It SHALL be text, not colour alone.

#### Scenario: No phone paired
- **WHEN** the owner has no paired device
- **THEN** the status SHALL say that no phone is connected and link to the device settings

#### Scenario: Status in the rail
- **WHEN** the sidebar is the icon rail or the full sidebar
- **THEN** the sidebar SHALL show no companion status, and the status SHALL be read in Pengaturan · HP & perangkat

### Requirement: Page Title Block
Every signed-in page SHALL start its frame with a title block: the page title in Manrope 24/32, an optional one-line subtitle 4 px below, and at the right at most two visible page actions plus a "…" menu for the rest, on the title's bottom edge. The block SHALL have 48 px top and 32 px bottom padding from 768 px, and 24/24 px on the phone.

#### Scenario: First content position
- **WHEN** Transaksi is opened at 1440 × 900 without a test session
- **THEN** the title block SHALL end and the page's first content SHALL start at `y = 192`

#### Scenario: More actions
- **WHEN** Target & Tagihan is opened at 1440 px
- **THEN** "Rencana" and "Tambah target" SHALL be visible secondary buttons, "Tambah utang" SHALL be in the "…" menu labelled "Tindakan lain", and "+ Catat" SHALL be the only primary button on the screen

#### Scenario: Page-written title
- **WHEN** Beranda is opened
- **THEN** its title block SHALL show the date as title and the cycle line as subtitle, written by the page, and the heading SHALL be the page's only `h1`

### Requirement: Phone Header
Below 768 px every signed-in page SHALL start with a sticky 56 px header: the brand linking to Beranda, a search button that opens Transaksi with its search focused, "Sembunyikan saldo" and the bell with its count, each target at least 44 × 44 px.

#### Scenario: Searching from the phone header
- **WHEN** the owner taps the search button on Beranda at 390 px
- **THEN** Transaksi SHALL open with its search field focused, and the address SHALL no longer contain the focus parameter after it took effect

#### Scenario: Notifications on a phone
- **WHEN** the owner taps the bell at 390 px
- **THEN** the notifications SHALL open as a bottom sheet with focus inside it, and Esc or the close control SHALL return focus to the bell

### Requirement: Notifications Include Due Plans
The bell SHALL list the ready monthly report, every active expense plan and every unpaid credit bill that is overdue, due today or due within two days, plus one item for recent automatic plan links. Each item SHALL show its title, detail and date. An overdue or due-today plan, or the automatic-link item, SHALL open "Rencana jatuh tempo"; a credit bill SHALL open Catat's Perpindahan into its account; other items SHALL open their page.

#### Scenario: Due plan from the bell on Transaksi
- **GIVEN** a plan "Tagihan SPayLater" due today
- **WHEN** the owner opens Transaksi
- **THEN** no due-plan banner SHALL be shown, the bell SHALL list the plan, and activating it SHALL open "Rencana jatuh tempo" where the plan can be linked, skipped or recorded with the same confirmation as before

#### Scenario: No duplicate plan item
- **WHEN** a plan is both due today in the pending list and within the two-day window
- **THEN** the bell SHALL list it once

#### Scenario: Seen state unavailable
- **WHEN** browser storage throws
- **THEN** the bell SHALL list every item as unread and keep working without an error

#### Scenario: Due credit bill
- **GIVEN** a BCA credit card bill due tomorrow with Rp 1.250.000 left and no plan for it
- **WHEN** the owner opens the bell
- **THEN** it SHALL list "Tagihan BCA Kartu Kredit Rp 1.250.000 besok" with "Kredit · jatuh tempo {day}", and activating it SHALL open Perpindahan with the card as "Ke" and Rp 1.250.000, recording nothing until the owner saves

### Requirement: Bantuan Dialog
"Bantuan & pintasan" SHALL be a dialog with three short facts about how the app works, a table of keys and their functions, and one line saying that single-key shortcuts are turned off in Pengaturan › Tampilan. It SHALL contain no switch.

#### Scenario: Opening Bantuan
- **WHEN** the owner presses ? outside a field on Beranda
- **THEN** "Bantuan & pintasan" SHALL open as a 560 px dialog with focus inside, list ⌘K / Ctrl K, N, ? and Esc in a table with key caps, and Esc SHALL close it and return focus

### Requirement: Test-Session Notice
While a test session is open, every signed-in page SHALL show one neutral line between the top bar or phone header and the title block, naming the session's end time, saying that notifications are stored and not recorded, and offering "Lihat sesi" and "Akhiri sesi". It SHALL use no warning colour and SHALL scroll with the page.

#### Scenario: Session running
- **GIVEN** a session that ends at 18.30
- **WHEN** the owner opens Analisis at 1440 px
- **THEN** a line "Sesi uji berjalan sampai 18.30 · notifikasi disimpan, tidak dicatat" with "Lihat sesi" and "Akhiri sesi" SHALL sit above the title block, with a neutral fill and no coloured border

#### Scenario: Ending from the notice
- **WHEN** the owner chooses "Akhiri sesi" in the line
- **THEN** the session SHALL end and the line SHALL disappear on every page
