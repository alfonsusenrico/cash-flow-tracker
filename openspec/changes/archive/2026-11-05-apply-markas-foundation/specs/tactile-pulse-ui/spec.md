## ADDED Requirements

### Requirement: Semantic Colour Meaning
Coral SHALL be the primary-action fill and the active-navigation mark and SHALL mark expenses. Income SHALL be green, transfers ("Pindah Saldo") sky, investments lavender and warnings amber, as text inks or marks; badges SHALL carry the ink on the neutral raised fill. Sky text and icons SHALL mean a transfer. Other information SHALL use the neutral tokens. Text SHALL measure at least 4.5:1 and meaningful icons and marks 3:1 in both themes.

#### Scenario: Light theme coral text contrast
- **WHEN** the light theme is active and a page shows coral text such as "Buka Tinjau", "Semua transaksi", an expense amount or "+ Tambah Tagihan"
- **THEN** the text uses the coral text ink, measures at least 4.5:1 against the surface it sits on and still reads as coral

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
- **WHEN** the Beranda KPI strip and card headers show their icon tiles
- **THEN** each tile that repeats its label SHALL be hidden from assistive technology, the figure SHALL be named by its label, and a tile icon that conveys meaning SHALL measure at least 3:1 against the tile in both themes

## REMOVED Requirements

### Requirement: Anti-AI-Slop Visual Design Standard
**Reason**: On 2026-11-05 the owner accepted option 6 "Markas+". Its fonts, warm surfaces, flat panels, 1 px neutral lines and pillar marks replace the pre-redesign standard (Plus Jakarta Sans, card shadows, `--shadow-xs` raises, soft fills, distinct pillar text hues).
**Migration**:
- Typography: `design-tokens` "Type Families and Scale" and `web-font-hosting`.
- Surfaces, radii, borders and elevation: `design-tokens` "Geometry, Borders and Elevation".
- Colour meaning: "Semantic Colour Meaning" in this capability.
- Pillars: "Kakeibo Pillar Marks".
- Prohibitions and icon tiles: "No Decorative Effects".

### Requirement: Animated Headline Figures
**Reason**: The accepted Markas+ design (option-6 SPEC §10) forbids counting numbers. A figure that changes shows its new value at once.
**Migration**: The Beranda headline figures render their final value immediately, and the counter hook is removed. The rule now lives in `design-tokens` "State and Layer Motion".
