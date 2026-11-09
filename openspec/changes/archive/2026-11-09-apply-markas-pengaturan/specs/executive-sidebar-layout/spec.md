## MODIFIED Requirements

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
