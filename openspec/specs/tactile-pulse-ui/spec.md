# tactile-pulse-ui Specification

## Purpose
Define rapid daily transaction capture, spending awareness and shared financial navigation and interaction patterns.

## Requirements

### Requirement: 3-Screen Information Architecture
The web application SHALL offer one short page list shared by the desktop sidebar, the phone tab bar and the Lainnya sheet, in three groups: HARIAN, RINGKASAN and SIMPANAN.

#### Scenario: Required behavior and constraints
- **WHEN** the application renders navigation for the current viewport
- **THEN** the following required behavior and constraints hold:

  The web application SHALL offer one short page list:
  1. Pages, in this order: HARIAN `Beranda` `/`, `Tinjau` `/review`, `Transaksi` `/ledger`; RINGKASAN `Analisis` `/insights`, `Laporan` `/laporan`; SIMPANAN `Rekening & Kantong` `/accounts` and `Target & Tagihan` `/goals`, with `Pengaturan` (`/settings`) apart at the foot. There SHALL be no Tanya page and no separate Bantuan entry; help opens with `?`.
  2. From 768 px the list SHALL appear in the sidebar with its group labels (`executive-sidebar-layout`).
  3. Below 768 px a bottom tab bar SHALL offer Beranda, Tinjau, Catat (opens capture), Transaksi and Lainnya; Lainnya SHALL reach every other page in the same order.

#### Scenario: Navigating between executive dashboard screens
- **WHEN** a user selects any page from the sidebar or the phone tab bar
- **THEN** the page switches without a full page reload, the active page is marked, and the page-specific period selection of Transaksi, Analisis and Laporan is kept while moving between them

#### Scenario: Mobile responsive dashboard navigation
- **WHEN** a user opens the app below 768 px
- **THEN** the navigation is the bottom tab bar with the Tinjau count and the Lainnya sheet
- **AND** the sidebar is hidden and no hamburger slide-over menu exists

#### Scenario: Same order everywhere
- **WHEN** the sidebar at 1440 px and the Lainnya sheet at 390 px are read
- **THEN** the pages SHALL appear in the same relative order, with Analisis before Laporan and Rekening & Kantong before Target & Tagihan in both

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

### Requirement: Quick Capture Save Confirmation
After Quick Capture saves a transaction or movement, the submit button SHALL show a brief success state (`✓ Tersimpan!`, or `✓ Berhasil dipindahkan` for a movement) and the modal SHALL close about 450 ms later. If the record is saved but its receipt upload fails, the modal SHALL stay open with an error message instead.

#### Scenario: Transaction saved
- **WHEN** the owner submits an expense and the save succeeds
- **THEN** the submit button SHALL show `✓ Tersimpan!` and the modal SHALL close shortly afterwards

#### Scenario: Receipt upload fails after the save
- **WHEN** the transaction is saved but its receipt upload fails
- **THEN** the modal SHALL stay open and say that the transaction was saved but the receipt was not uploaded

### Requirement: Semantic Colour Meaning
Coral SHALL be the primary-action fill and the navigation and selection mark. Expense amounts SHALL carry "−", in the ink in the shared list row and in coral text elsewhere. Income SHALL be green, transfers ("Pindah Saldo") sky, investments lavender and warnings amber, as text inks or marks; badges SHALL carry the ink on the neutral raised fill. Sky SHALL mean a transfer. Other information SHALL be neutral. Text SHALL measure at least 4.5:1 and meaningful icons and marks 3:1 in both themes.

#### Scenario: Light theme coral text contrast
- **WHEN** the light theme is active and a page shows coral text such as "Buka Tinjau · 6", an expense amount on Transaksi or "+ Tambah Tagihan"
- **THEN** the text uses the coral text ink, measures at least 4.5:1 against the surface it sits on and still reads as coral

#### Scenario: Expense in the shared list row
- **WHEN** Beranda lists an expense of Rp 30.000 in either theme at 390 or 1440 px
- **THEN** the amount SHALL read "−Rp 30.000" in the ink, measure at least 4.5:1, and the row SHALL name its pillar in words

#### Scenario: Income text
- **WHEN** Beranda, Transaksi or Analisis shows an income amount or the "Uang Masuk" badge in the light or the dark theme at 390 or 1280 px
- **THEN** the text is green and measures at least 4.5:1 against the surface or badge fill it sits on, and no button uses a green fill

#### Scenario: Sky blue transfers
- **WHEN** Beranda, Transaksi or the phone-width Beranda shows a "Pindah Saldo" action, a movement row's icon or amount, or a movement's detail amount, in either theme at 390 or 1280 px
- **THEN** the element SHALL be sky blue, its text SHALL measure at least 4.5:1 and its icon at least 3:1, and amber SHALL remain on warnings only

#### Scenario: Lavender investments
- **WHEN** a page shows a "Top up Investasi" row, an investment amount, balance or badge, in either theme at 390 or 1280 px
- **THEN** the element SHALL be lavender and not amber or sky, and its text SHALL measure at least 4.5:1 against what it sits on

#### Scenario: Neutral information
- **WHEN** a page shows the Target & Tagihan progress note, a "Ditautkan otomatis" label, an explanation note or the phone-width "Dipilih" label
- **THEN** the element SHALL use the neutral text and muted tokens, not sky blue

#### Scenario: Text on the coral primary fill
- **WHEN** a page shows "Tambah target", "Simpan progres" or "+ Catat" on the coral fill, in the light or the dark theme
- **THEN** the text uses the fill's contrast ink and measures at least 4.5:1 against the fill

### Requirement: Kakeibo Pillar Marks
Each Kakeibo pillar SHALL have a mark colour from the pillar tokens: Kebutuhan violet, Keinginan rose, Budaya blue, Tak Terduga amber and Tabungan green. "Belum diklasifikasi" SHALL be neutral grey with a dashed marker. Pillar colours SHALL appear only as marks (dots, bars, rails), each at least 3:1 against what it sits on and each beside a label that names the pillar; pillar names and amounts SHALL use the neutral text tokens.

#### Scenario: Pillars on Analisis, Beranda and Laporan
- **WHEN** Analisis, Beranda or Laporan Bulanan shows the Kakeibo classification or the spending per pillar, in either theme at 390 or 1280 px
- **THEN** each pillar shows its colour only as a dot, bar or rail beside its name, the name and amount are in the neutral text tokens, and "Belum diklasifikasi" is grey with a dashed marker

#### Scenario: Pillar mark contrast
- **WHEN** `tokens.test.ts` runs
- **THEN** every pillar token SHALL measure at least 3:1 against the canvas, the surfaces and the raised fill in both themes, and the test SHALL fail on the first pair below it

### Requirement: No Decorative Effects
The interface SHALL NOT contain rainbow gradients, glow drop-shadows, 0–100 health meters, decorative empty KPI cards or unformatted text blocks. An icon tile that repeats its control's label SHALL be hidden from assistive technology, and a tile icon that conveys meaning SHALL measure at least 3:1 against its tile.

#### Scenario: Decorative icon tiles
- **WHEN** a page not yet redesigned, such as Transaksi, Analisis or Laporan, shows icon tiles in its KPI strip or card headers
- **THEN** each tile that repeats its label SHALL be hidden from assistive technology, the figure SHALL be named by its label, and a tile icon that conveys meaning SHALL measure at least 3:1 against the tile in both themes

#### Scenario: Beranda without tiles
- **WHEN** Beranda is shown in either theme
- **THEN** it SHALL show no icon tile or avatar, and the 16 px icons in its "Perlu tindakan" rows SHALL be hidden from assistive technology
