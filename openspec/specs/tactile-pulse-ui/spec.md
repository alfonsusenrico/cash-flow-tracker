# tactile-pulse-ui Specification

## Purpose
Define rapid daily transaction capture, spending awareness and shared financial navigation and interaction patterns.

## Requirements

### Requirement: 3-Screen Information Architecture
The web application SHALL organize user navigation into a dual-mode information architecture.

#### Scenario: Required behavior and constraints
- **WHEN** the application renders navigation for the current viewport
- **THEN** the following required behavior and constraints hold:

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

### Requirement: 3-Second Rapid Transaction Capture Pad
The application SHALL provide a prominent quick-capture interface accessible on the main screen and via keyboard shortcut (`N` key).

#### Scenario: Required behavior and constraints
- **WHEN** an owner opens and completes rapid transaction capture
- **THEN** it SHALL provide an organized category selector with icons, default to the last-used or primary spend account, support inline arithmetic, and refresh observed figures and activity without a full page reload

#### Scenario: Organized category selection in quick capture
- **WHEN** a user opens the quick-capture modal and selects a category
- **THEN** the category is selected with its icon and name

#### Scenario: Desktop keyboard shortcut capture
- **WHEN** a user presses `N` on any screen
- **THEN** quick capture opens with the amount input auto-focused

### Requirement: Anti-AI-Slop Visual Design Standard
The design system SHALL adhere to modern digital banking craftsmanship standards (Bank Jago, Bibit, SeaBank).

#### Scenario: Required behavior and constraints
- **WHEN** the application renders its financial interface
- **THEN** the following required behavior and constraints hold:

  The design system SHALL adhere to modern digital banking craftsmanship standards (Bank Jago, Bibit, SeaBank):
  1. **Typography:** Plus Jakarta Sans for interface text and Geist Mono for numeric code text, both self-hosted from the app's own origin as defined by `web-font-hosting`, paired with tabular figures (`font-variant-numeric: tabular-nums`) for currency and timestamps.
  2. **Seamless Surface Elevation:** Card surfaces SHALL be separated from the page by the tonal contrast between `--surface` and `--bg` and a 1px hairline `--border` (ink at 9% opacity in the light theme, white at 8% in the dark theme), never by heavy wireframe outlines. A raised surface SHALL use the `--shadow-xs`, `--shadow-sm` or `--shadow-md` token of `frontend/tokens.css`, where `--shadow-xs` is the 1px raise of plain cards, chips and buttons and stays lighter than `--shadow-sm` in both themes; deeper shadows are reserved for floating layers such as dialogs, menus, the phone navigation bar and the selection bar.
  3. **Accents:** Emerald green for Income/Cash In, coral/rose for Expense/Cash Out, sky blue for internal transfers ("Pindah Saldo") and lavender for investments, investment top-ups included. Every transfer element (a "Pindah Saldo" action, a movement row's icon, amount or badge, and a movement's detail amount) SHALL use sky blue, never amber or another blue; amber SHALL be reserved for warnings such as "Waspada". Sky blue SHALL be used only for transfer elements; information that is neither a transfer nor an investment, such as the "Pilar Kakeibo" card, an explanatory note or a "Ditautkan otomatis" label, SHALL use the neutral surface, text and muted tokens. Every investment element (an investment balance, an investment account's badge, card or allocation share, an instrument control, and a top-up's badge, row, amount or note) SHALL use lavender, never amber, sky, blue, indigo or purple. Spring Lime, emerald and coral keep their vivid colours for fills (navigation pills, chart peaks, buttons, chart marks); lime, emerald, coral, sky and lavender used as text or as a meaningful icon on a theme surface SHALL use a text ink of the same hue (darker than the fill in the light theme), measuring at least 4.5:1 for text and 3:1 for icons against the surface it sits on in the light and the dark theme. Text on the solid lavender fill SHALL use a contrast ink that measures at least 4.5:1 against that fill in both themes. Text on an emerald fill, the strong `bg-emerald-600` fill included, SHALL use an ink that measures at least 4.5:1 against that fill in both themes. Amber used as text SHALL use a warning text ink of the same hue, measuring at least 4.5:1 against the surface it sits on in both themes. An icon tile that repeats its button's label SHALL be hidden from assistive technology, and a tile icon that conveys meaning SHALL measure at least 3:1 against its tile.
  4. **Kakeibo pillars:** each of the five pillars ("Kebutuhan", "Keinginan", "Budaya", "Tak Terduga", "Tabungan") SHALL have its own hue, at least 25 degrees from the transfer sky, the investment lavender, the income emerald and the expense coral (fills and text inks) and at least 40 degrees from every other pillar, in the light and the dark theme. "Belum diklasifikasi" SHALL stay a neutral grey. A pillar color SHALL measure at least 4.5:1 as text against `--surface`, `--bg`, `--surface-raised` and its soft fill, and at least 3:1 as a chart mark against its track, in both themes. Pillar colors SHALL come only from the pillar tokens, and every pillar mark SHALL sit beside a label that names the pillar.
  5. **Prohibitions:** The UI SHALL NOT contain harsh box outlines, rainbow gradients, blurry purple glow drop-shadows, 0–100 health meters, or unformatted text blocks.

#### Scenario: Dark mode contrast validation
- **WHEN** dark mode is active
- **THEN** text satisfies WCAG AA contrast against surface colors and cards float seamlessly over dark backgrounds with subtle hairline separation

#### Scenario: Light theme lime and coral text contrast
- **WHEN** the light theme is active and a page shows lime text such as "Kelola →" or "Lihat Semua →", or coral text such as an expense amount, the "Uang Keluar" badge or "+ Tambah Tagihan"
- **THEN** the text measures at least 4.5:1 against the surface it sits on, it still reads as lime or coral, and the lime pill, chart peak and coral fills are unchanged

#### Scenario: Plain cards carry a subtle raise
- **WHEN** a card, chip or button styled with the `shadow-xs` utility renders in the light or the dark theme
- **THEN** its computed `box-shadow` SHALL be the `--shadow-xs` value of that theme (a 1px offset with at most 2px blur, lighter than `--shadow-sm`), and a surface that already has `--shadow-sm` or a deeper shadow SHALL keep it

#### Scenario: Emerald income text and buttons
- **WHEN** Beranda, Transaksi or Analisis shows emerald text such as an income amount or the "Uang Masuk" badge, or text on an emerald button such as "Tambah Kategori" or "Catat Transaksi", in the light or the dark theme at 390 or 1280 px
- **THEN** the text measures at least 4.5:1 against the surface or fill it sits on, income still reads as emerald, and the emerald fills are unchanged

#### Scenario: Amber text contrast
- **WHEN** a page shows `text-amber-400` to `text-amber-700` text, such as a "Waspada" badge, on `--surface`, `--bg`, `--surface-raised` or the warning soft fill, in the light or the dark theme
- **THEN** the text measures at least 4.5:1 against that background, it still reads as amber, and the amber fills are unchanged

#### Scenario: Text on the strong emerald fill
- **WHEN** Target & Tagihan shows "Target Baru", "Buat Target Pertama Anda" or "Simpan Progres" on the `bg-emerald-600` fill, in the light or the dark theme
- **THEN** the text measures at least 4.5:1 against the fill, and the fill is unchanged

#### Scenario: Decorative icon tiles
- **WHEN** the Beranda wallet card shows its "Pengeluaran", "Pemasukan" and "Pindah Saldo" buttons
- **THEN** each button's icon tile SHALL be hidden from assistive technology, the button SHALL be named by its text, and the emerald tile's icon and the sky "Pindah Saldo" tile's icon SHALL each measure at least 3:1 against the tile in both themes

#### Scenario: Sky blue transfers
- **WHEN** Beranda, Transaksi or the phone-width home view shows the "Pindah Saldo" text button or capsule, a movement row's icon, amount or "Pindah Saldo" badge, or a movement's detail amount, in the light or the dark theme at 390 or 1280 px
- **THEN** the element SHALL be sky blue and not amber or another blue, its text SHALL measure at least 4.5:1 and its icon at least 3:1 against the surface or sky tint it sits on, and amber SHALL remain on warnings only

#### Scenario: Lavender investments
- **WHEN** Beranda, the phone-width home view, Transaksi or Rekening shows an investment element, such as the "Investasi" wallet pill or balance, the investment wallet card, the "Invest" badge, the "Investasi" allocation share, a selected instrument button, or a "Top up Investasi" badge, amount or note, in the light or the dark theme at 390 or 1280 px
- **THEN** the element SHALL be lavender and not amber, sky, blue, indigo or purple, its text SHALL measure at least 4.5:1 against the surface, lavender soft fill or lavender tint it sits on, text on the solid lavender fill SHALL measure at least 4.5:1, and transfers SHALL stay sky, warnings amber, income emerald and expenses coral

#### Scenario: Sky only for transfers
- **WHEN** a page shows the "Pilar Kakeibo" card, the Target & Tagihan progress note, a "Ditautkan otomatis" label or the phone-width "Dipilih" selection label
- **THEN** the element SHALL use the neutral surface, text and muted tokens and not sky blue, and every sky blue element on the web SHALL be a transfer element

#### Scenario: Distinct Kakeibo pillar hues
- **WHEN** Analisis, Beranda or Laporan Bulanan shows the Kakeibo classification, the "Pilar Kakeibo" bar or the spending per pillar, in the light or the dark theme at 390 or 1280 px
- **THEN** the five pillars show five distinct hues, none of them sky, lavender, emerald or coral, "Belum diklasifikasi" is grey, each mark sits beside its pillar's name, pillar text measures at least 4.5:1 and pillar marks at least 3:1 against what they sit on

### Requirement: Quick Capture Linking to Goals and Obligations
The global transaction quick-capture interface (`N`) SHALL allow optional direct linking to savings goals and debt obligations.

#### Scenario: Required behavior and constraints
- **WHEN** an owner links a Quick Capture transaction to a goal or debt
- **THEN** the following required behavior and constraints hold:

  The global transaction quick-capture interface (`N`) SHALL allow optional direct linking to savings goals and debt obligations:
  1. When recording an expense or transfer, the user can select an active Goal (marking it as a milestone contribution) or an active Obligation (marking it as a debt repayment).
  2. Upon submission, the transaction SHALL record the corresponding `goal_id` or `obligation_id` and automatically increment the goal's `current_amount` or decrement the obligation's `remaining_amount` in the database.

#### Scenario: Contributing to a goal via quick capture
- **WHEN** the user presses `N`, enters `500000`, selects account "Main Bank", and chooses goal "Emergency Fund"
- **THEN** an expense of 500,000 IDR is recorded and linked to "Emergency Fund", and the goal's saved amount increases by 500,000 IDR immediately

### Requirement: Animated Headline Figures
Headline money figures on the Beranda balance card, the Beranda cash-flow matrix and the Rekening net-worth summary SHALL move from their previous value to a new value over about 380 ms with an ease-out curve. When the viewer requests reduced motion, the final value SHALL be shown at once.

#### Scenario: Balance changes
- **GIVEN** the Beranda balance card shows Rp 100.000
- **WHEN** the balance changes to Rp 150.000
- **THEN** the figure SHALL count from Rp 100.000 to Rp 150.000 within about 380 ms and end on the exact value

#### Scenario: Reduced motion
- **GIVEN** the operating system requests reduced motion
- **WHEN** a headline figure changes
- **THEN** the new value SHALL be shown immediately without intermediate values

### Requirement: Quick Capture Save Confirmation
After Quick Capture saves a transaction or movement, the submit button SHALL show a brief success state (`✓ Tersimpan!`, or `✓ Berhasil dipindahkan` for a movement) and the modal SHALL close about 450 ms later. If the record is saved but its receipt upload fails, the modal SHALL stay open with an error message instead.

#### Scenario: Transaction saved
- **WHEN** the owner submits an expense and the save succeeds
- **THEN** the submit button SHALL show `✓ Tersimpan!` and the modal SHALL close shortly afterwards

#### Scenario: Receipt upload fails after the save
- **WHEN** the transaction is saved but its receipt upload fails
- **THEN** the modal SHALL stay open and say that the transaction was saved but the receipt was not uploaded
