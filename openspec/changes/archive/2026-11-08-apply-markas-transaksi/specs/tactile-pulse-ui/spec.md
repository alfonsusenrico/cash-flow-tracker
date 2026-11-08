## MODIFIED Requirements

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

### Requirement: No Decorative Effects
The interface SHALL NOT contain rainbow gradients, glow drop-shadows, 0–100 health meters, decorative empty KPI cards or unformatted text blocks. An icon tile that repeats its control's label SHALL be hidden from assistive technology, and a tile icon that conveys meaning SHALL measure at least 3:1 against its tile.

#### Scenario: Decorative icon tiles
- **WHEN** a page not yet redesigned, such as Analisis or Laporan, shows icon tiles in its KPI strip or card headers
- **THEN** each tile that repeats its label SHALL be hidden from assistive technology, the figure SHALL be named by its label, and a tile icon that conveys meaning SHALL measure at least 3:1 against the tile in both themes

#### Scenario: Beranda without tiles
- **WHEN** Beranda is shown in either theme
- **THEN** it SHALL show no icon tile or avatar, and the 16 px icons in its "Perlu tindakan" rows SHALL be hidden from assistive technology

#### Scenario: Transaksi without tiles
- **WHEN** Transaksi is shown in either theme at 1440 or 390 px
- **THEN** it SHALL show no icon tile, avatar, KPI card or type pill, and each row SHALL carry its kind by the amount's sign and colour and the words in its category or pillar column
