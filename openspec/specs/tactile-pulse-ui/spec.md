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
The application SHALL provide a prominent capture entry: "+ Catat" in the workspace top bar, the Catat button of the phone tab bar, and the `N` key while single-key shortcuts are on. The manual form SHALL offer Pengeluaran, Pemasukan and Perpindahan, a 56 px amount field, quick-add chips, and categories grouped by Kakeibo pillar.

#### Scenario: Required behavior and constraints
- **WHEN** an owner opens and completes rapid transaction capture
- **THEN** it SHALL provide a category selector grouped by pillar, default to the primary spend account, support inline arithmetic, and refresh observed figures and activity without a full page reload

#### Scenario: Organized category selection in quick capture
- **WHEN** a user opens the manual form and opens "Kategori"
- **THEN** the expense categories SHALL be listed under their pillar names, and the chosen category SHALL set the pillar unless the owner chose another pillar

#### Scenario: Desktop keyboard shortcut capture
- **WHEN** single-key shortcuts are on and a user presses `N` on any screen outside a text field
- **THEN** the capture dialog opens with the sentence field focused
- **AND** when single-key shortcuts are off, pressing `N` does nothing

#### Scenario: Amount focus
- **WHEN** the amount field receives focus, in either theme
- **THEN** its edge SHALL be 1 px `--field-focus` with a 3 px `--ring`, and no outline, coloured or 2 px border SHALL appear

### Requirement: Quick Capture Linking to Goals and Obligations
The global capture form SHALL allow optional direct linking of an expense to one savings goal or to one or more debt obligations, inside "Detail tambahan", each with a one-line hint. A goal and a debt SHALL be exclusive, and the debt choice SHALL not be offered for an expense on a credit account.

#### Scenario: Required behavior and constraints
- **WHEN** an owner links a capture expense to a goal or debt
- **THEN** the following required behavior and constraints hold:

  The global capture form SHALL allow optional direct linking to savings goals and debt obligations:
  1. When recording an expense, the user can select an active Goal (marking it as a milestone contribution) or one or more active Obligations (marking a debt repayment, split by amount).
  2. Upon submission, the transaction SHALL record the corresponding `goal_id`, `obligation_id` or `obligation_allocations`, and SHALL decrement each linked obligation's `remaining_amount` in the database. A goal link is a label: it SHALL NOT change the goal's progress by itself (a goal follows its own amount or its linked accounts' balance).

#### Scenario: Contributing to a goal via quick capture
- **WHEN** the user presses `N`, chooses "Isi manual", enters `500000`, selects account "Main Bank", opens "Detail tambahan" and chooses goal "Emergency Fund"
- **THEN** an expense of 500,000 IDR is recorded with the `goal_id` of "Emergency Fund", and the hint under the goal field says that the link does not add to the goal's progress

#### Scenario: Details open when needed
- **WHEN** the form holds a goal passed by its caller or an error inside "Detail tambahan"
- **THEN** "Detail tambahan" SHALL be open with that field visible

### Requirement: Quick Capture Save Confirmation
After Quick Capture saves a transaction or movement, the submit button SHALL show a brief success state (`✓ Tersimpan!`, or `✓ Berhasil dipindahkan` for a movement) and the modal SHALL close about 450 ms later. If the record is saved but its receipt upload fails, the modal SHALL stay open with an error message instead.

#### Scenario: Transaction saved
- **WHEN** the owner submits an expense and the save succeeds
- **THEN** the submit button SHALL show `✓ Tersimpan!` and the modal SHALL close shortly afterwards

#### Scenario: Receipt upload fails after the save
- **WHEN** the transaction is saved but its receipt upload fails
- **THEN** the modal SHALL stay open and say that the transaction was saved but the receipt was not uploaded

### Requirement: Semantic Colour Meaning
Coral SHALL be the primary-action fill and the navigation and selection mark. Expense amounts SHALL carry "−" and the expense red text ink everywhere, including list rows and the transaction dialog. Income SHALL be green, transfers sky, investments lavender and warnings amber, as text inks or marks; badges SHALL carry the ink on the neutral raised fill. Other information SHALL be neutral. Text SHALL measure at least 4.5:1 and meaningful icons and marks 3:1 in both themes.

#### Scenario: Light theme coral text contrast
- **WHEN** the light theme is active and a page shows coral text such as "Buka Tinjau · 6", "Tandai sudah dicek" or "+ Tambah Tagihan"
- **THEN** the text uses the coral text ink, measures at least 4.5:1 against the surface it sits on and still reads as coral

#### Scenario: Expense in the shared list row
- **WHEN** Beranda (Aktivitas terbaru or Perlu tindakan) or Transaksi lists an expense of Rp 30.000 in either theme at 390 or 1440 px, at rest, hovered or selected
- **THEN** the amount SHALL read "−Rp 30.000" in the expense red, measure at least 4.5:1 against the row's surface, and the row SHALL name its pillar in words

#### Scenario: Expense in the transaction dialog
- **WHEN** the owner opens an expense of Rp 120.000 from Transaksi, Beranda or Analisis in either theme
- **THEN** the dialog's amount SHALL read "−Rp 120.000" in the expense red and measure at least 4.5:1

#### Scenario: Expense ink token test
- **WHEN** `tokens.test.ts` runs
- **THEN** the expense text ink SHALL measure at least 4.5:1 against the canvas, every surface and the dialog in both themes, and the shared expense amount SHALL resolve to that ink

#### Scenario: Income text
- **WHEN** Beranda, Transaksi or Analisis shows an income amount or the "Uang Masuk" badge in the light or the dark theme at 390 or 1280 px
- **THEN** the text is green and measures at least 4.5:1 against the surface or badge fill it sits on, and no button uses a green fill

#### Scenario: Sky blue transfers
- **WHEN** Beranda, Transaksi or the phone-width Beranda shows a "Pindah Saldo" action, a movement row's amount, or a movement's detail amount, in either theme at 390 or 1280 px
- **THEN** the element SHALL be sky blue, its text SHALL measure at least 4.5:1 and its icon at least 3:1, and amber SHALL remain on warnings only

#### Scenario: Lavender investments
- **WHEN** a page shows a "Top up Investasi" row, an investment amount, balance or badge, in either theme at 390 or 1280 px
- **THEN** the element SHALL be lavender and not amber or sky, and its text SHALL measure at least 4.5:1 against what it sits on

#### Scenario: Neutral information
- **WHEN** a page shows the Target & Tagihan progress note, a "Ditautkan otomatis" label, an explanation note, a Transaksi day total or the phone-width "Dipilih" label
- **THEN** the element SHALL use the neutral text and muted tokens, not sky blue

#### Scenario: Text on the coral primary fill
- **WHEN** a page shows "Tambah target", "Simpan progres", "Terapkan" or "+ Catat" on the coral fill, in the light or the dark theme
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
- **WHEN** a page not yet redesigned, such as Target & Tagihan, shows icon tiles in its KPI strip or card headers
- **THEN** each tile that repeats its label SHALL be hidden from assistive technology, the figure SHALL be named by its label, and a tile icon that conveys meaning SHALL measure at least 3:1 against the tile in both themes

#### Scenario: Beranda without tiles
- **WHEN** Beranda is shown in either theme
- **THEN** it SHALL show no icon tile or avatar, and the 16 px icons in its "Perlu tindakan" rows SHALL be hidden from assistive technology

#### Scenario: Transaksi without tiles
- **WHEN** Transaksi is shown in either theme at 1440 or 390 px
- **THEN** it SHALL show no icon tile, avatar, KPI card or type pill, and each row SHALL carry its kind by the amount's sign and colour and the words in its category or pillar column

#### Scenario: Analisis without tiles
- **WHEN** any tab of Analisis is shown in either theme at 1440 or 390 px
- **THEN** it SHALL show no icon tile, KPI card or card shadow, and its sections SHALL be named by their headings alone

#### Scenario: Laporan without tiles
- **WHEN** Laporan is shown in either theme at 1440 or 390 px
- **THEN** it SHALL show no icon tile, KPI card or card shadow, its sections SHALL be named by their question and section headings alone, and the data-quality line SHALL use no tinted fill

### Requirement: Inline Arithmetic in the Amount
The capture amount field SHALL accept digits and the operators `+ − * /` typed on any keyboard, evaluate the expression without `eval`, show "= {amount}" as its hint only while an operator is present, and save the evaluated whole-rupiah amount. No on-screen keypad SHALL be shown.

#### Scenario: Evaluate inline arithmetic in transaction entry
- **WHEN** the owner types `25000+15000` in "Nominal (IDR)"
- **THEN** the hint SHALL read "= Rp 40.000" and saving SHALL send `amount` 40000

#### Scenario: Plain amount
- **WHEN** the owner types `35000`
- **THEN** the field SHALL show "35.000" and no "=" hint
