# tactile-pulse-ui Specification

## Purpose
Define rapid daily transaction capture, spending awareness and shared financial navigation and interaction patterns.

## Requirements

### Requirement: 3-Screen Information Architecture
The web application SHALL organize navigation into grouped workspaces shared by the desktop sidebar and the phone tab bar.

#### Scenario: Required behavior and constraints
- **WHEN** the application renders navigation for the current viewport
- **THEN** the following required behavior and constraints hold:

  The web application SHALL organize navigation into grouped workspaces:
  1. Groups and pages, in this order: Harian (`Beranda` `/`, `Tinjau` `/review`, `Transaksi` `/ledger`), AI (`Tanya`, once it exists), Uang (`Rekening & Kantong` `/accounts`, `Target & Tagihan` `/goals`) and Laporan (`Analisis` `/insights`, `Laporan` `/laporan`), with `Pengaturan` (`/settings`) and `Bantuan` apart from the groups.
  2. Above 760 px the groups SHALL appear in the sidebar of the workspace window (`executive-sidebar-layout`).
  3. At 760 px and below a bottom tab bar SHALL offer Beranda, Tinjau, Catat (opens capture), Transaksi and Lainnya; Lainnya SHALL reach every other page.

#### Scenario: Navigating between executive dashboard screens
- **WHEN** a user selects any page from the sidebar or the phone tab bar
- **THEN** the page switches without a full page reload, the active page is marked, and the page-specific period selection of Transaksi, Analisis and Laporan is kept while moving between them

#### Scenario: Mobile responsive dashboard navigation
- **WHEN** a user opens the app at 760 px or narrower
- **THEN** the navigation is the bottom tab bar with the Tinjau count badge and the Lainnya sheet
- **AND** the sidebar is hidden and no hamburger slide-over menu exists

### Requirement: 3-Second Rapid Transaction Capture Pad
The application SHALL provide a prominent capture entry: "+ Catat" in the workspace top bar, the Catat button of the phone tab bar, and the `N` key while single-key shortcuts are on.

#### Scenario: Required behavior and constraints
- **WHEN** an owner opens and completes rapid transaction capture
- **THEN** it SHALL provide an organized category selector with icons, default to the last-used or primary spend account, support inline arithmetic, and refresh observed figures and activity without a full page reload

#### Scenario: Organized category selection in quick capture
- **WHEN** a user opens the quick-capture modal and selects a category
- **THEN** the category is selected with its icon and name

#### Scenario: Desktop keyboard shortcut capture
- **WHEN** single-key shortcuts are on and a user presses `N` on any screen outside a text field
- **THEN** quick capture opens with the amount input auto-focused
- **AND** when single-key shortcuts are off, pressing `N` does nothing

### Requirement: Anti-AI-Slop Visual Design Standard
The design system SHALL follow the approved workspace design: calm warm neutrals, one lime action accent, semantic colours with fixed meanings and readable contrast in light and dark themes.

#### Scenario: Required behavior and constraints
- **WHEN** the application renders its financial interface
- **THEN** the following required behavior and constraints hold:

  The design system SHALL follow the approved workspace design:
  1. **Typography:** Geist for interface text and Geist Mono for code-like text, both self-hosted from the app's own origin as defined by `web-font-hosting`, with tabular figures (`font-variant-numeric: tabular-nums`) for money, percentages, counts, dates and times.
  2. **Surfaces and elevation:** pages sit on the canvas token inside the window token; cards use the surface token, the card radius and the card shadow token, never heavy outlines. Dividers inside cards use the hairline line token. Deeper shadows are reserved for floating layers: drawers, sheets, popovers, dialogs, menus, the toast and the phone tab bar. The `--shadow-xs` token stays the 1px raise of plain chips and buttons and stays lighter than `--shadow-sm` in both themes.
  3. **Accents:** lime is the active-navigation and positive-action fill; the dark ink fill is the structural primary action ("+ Catat"). Income is green, expense coral, internal transfers ("Pindah Saldo") sky blue, investments (balances, products, top-ups) lavender and warnings amber, each with a soft fill and a text ink of the same hue. Every transfer element SHALL be sky and only transfer elements SHALL be sky; every investment element SHALL be lavender, never amber, sky, blue, indigo or purple; amber SHALL mean a warning. Information that is neither, such as notes and "Ditautkan otomatis", SHALL use the neutral tokens. Text in these hues on a surface or soft fill SHALL measure at least 4.5:1 and meaningful icons at least 3:1 in both themes; text on a solid fill (lime, green, lavender, ink, destructive) SHALL use that fill's contrast ink at 4.5:1 or better. An icon tile that repeats its control's label SHALL be hidden from assistive technology, and a tile icon that conveys meaning SHALL measure at least 3:1 against its tile.
  4. **Kakeibo pillars:** each of the five pillars ("Kebutuhan", "Keinginan", "Budaya", "Tak Terduga", "Tabungan") SHALL have its own hue, at least 25 degrees from the transfer, investment, income and expense hues (fills and text inks) and at least 40 degrees from every other pillar, in the light and the dark theme. "Belum diklasifikasi" SHALL stay a neutral grey shown with a dashed marker. A pillar colour SHALL measure at least 4.5:1 as text against the surfaces and its soft fill, and at least 3:1 as a chart mark against its track, in both themes. Pillar colours SHALL come only from the pillar tokens, and every pillar mark SHALL sit beside a label that names the pillar.
  5. **Prohibitions:** The UI SHALL NOT contain harsh box outlines, rainbow gradients, blurry purple glow drop-shadows, 0–100 health meters, decorative empty KPI cards or unformatted text blocks.

#### Scenario: Dark mode contrast validation
- **WHEN** the dark theme is active
- **THEN** text satisfies WCAG AA contrast against the window, surface and soft fills, and cards stand out from the window through tonal contrast and their shadow token

#### Scenario: Light theme lime and coral text contrast
- **WHEN** the light theme is active and a page shows lime text such as "Kelola →" or "Lihat Semua →", or coral text such as an expense amount, the "Uang Keluar" badge or "+ Tambah Tagihan"
- **THEN** the text measures at least 4.5:1 against the surface it sits on, it still reads as lime or coral, and the lime navigation pill, chart peak and coral fills keep their fill colour

#### Scenario: Plain cards carry a subtle raise
- **WHEN** a card renders in the light or the dark theme
- **THEN** its computed `box-shadow` SHALL be the card shadow token of that theme, a chip or button styled with `shadow-xs` SHALL use `--shadow-xs` (a 1px offset with at most 2px blur, lighter than `--shadow-sm`), and floating layers SHALL keep their deeper shadow

#### Scenario: Emerald income text and buttons
- **WHEN** Beranda, Transaksi or Analisis shows income text such as an income amount or the "Uang Masuk" badge, or text on a green button such as "Tambah Kategori" or "Catat Transaksi", in the light or the dark theme at 390 or 1280 px
- **THEN** the text measures at least 4.5:1 against the surface or fill it sits on, income still reads as green, and the green fills keep their colour

#### Scenario: Amber text contrast
- **WHEN** a page shows amber text, such as a "Waspada" badge or a "Pilih" action, on the window, a surface, the raised surface or the warning soft fill, in the light or the dark theme
- **THEN** the text measures at least 4.5:1 against that background, it still reads as amber, and the amber fills keep their colour

#### Scenario: Text on the strong emerald fill
- **WHEN** Target & Tagihan shows "Target Baru", "Buat Target Pertama Anda" or "Simpan Progres" on the strong green fill, in the light or the dark theme
- **THEN** the text measures at least 4.5:1 against the fill, and the fill keeps its colour

#### Scenario: Decorative icon tiles
- **WHEN** the Beranda KPI strip and card headers show their icon tiles
- **THEN** each tile that repeats its label SHALL be hidden from assistive technology, the figure SHALL be named by its label, and a tile icon that conveys meaning SHALL measure at least 3:1 against the tile in both themes

#### Scenario: Sky blue transfers
- **WHEN** Beranda, Transaksi or the phone-width Beranda shows a "Pindah Saldo" action, a movement row's icon, amount or "Pindah saldo" pill, or a movement's detail amount, in the light or the dark theme at 390 or 1280 px
- **THEN** the element SHALL be sky blue and not amber or another blue, its text SHALL measure at least 4.5:1 and its icon at least 3:1 against the surface or sky soft fill it sits on, and amber SHALL remain on warnings only

#### Scenario: Lavender investments
- **WHEN** Beranda, the phone-width Beranda, Transaksi or Rekening shows an investment element, such as a "Top up Investasi" row, pill, amount or note, an investment balance, an investment account's badge, card or allocation share, or a selected instrument button, in the light or the dark theme at 390 or 1280 px
- **THEN** the element SHALL be lavender and not amber, sky, blue, indigo or purple, its text SHALL measure at least 4.5:1 against the surface or lavender soft fill it sits on, text on the solid lavender fill SHALL measure at least 4.5:1, and transfers SHALL stay sky, warnings amber, income green and expenses coral

#### Scenario: Sky only for transfers
- **WHEN** a page shows the "Pilar Kakeibo" card, the Target & Tagihan progress note, a "Ditautkan otomatis" label, an explanation note or the phone-width "Dipilih" selection label
- **THEN** the element SHALL use the neutral surface, text and muted tokens and not sky blue, and every sky blue element on the web SHALL be a transfer element

#### Scenario: Distinct Kakeibo pillar hues
- **WHEN** Analisis, Beranda or Laporan Bulanan shows the Kakeibo classification, the Kakeibo bar or the spending per pillar, in the light or the dark theme at 390 or 1280 px
- **THEN** the five pillars show five distinct hues, none of them the transfer, investment, income or expense hue, "Belum diklasifikasi" is grey with a dashed marker, each mark sits beside its pillar's name, pillar text measures at least 4.5:1 and pillar marks at least 3:1 against what they sit on

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
Headline money figures in the Beranda KPI strip and the Rekening net-worth summary SHALL move from their previous value to a new value over about 380 ms with an ease-out curve. When the viewer requests reduced motion, the final value SHALL be shown at once.

#### Scenario: Balance changes
- **GIVEN** the Beranda "Keluar siklus ini" figure shows Rp 100.000
- **WHEN** the figure changes to Rp 150.000
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
