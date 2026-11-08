## MODIFIED Requirements

### Requirement: No Decorative Effects
The interface SHALL NOT contain rainbow gradients, glow drop-shadows, 0–100 health meters, decorative empty KPI cards or unformatted text blocks. An icon tile that repeats its control's label SHALL be hidden from assistive technology, and a tile icon that conveys meaning SHALL measure at least 3:1 against its tile.

#### Scenario: Decorative icon tiles
- **WHEN** a page not yet redesigned, such as Laporan or Rekening, shows icon tiles in its KPI strip or card headers
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
