# accounts-wallet-view Specification

## Purpose
The Rekening & Kantong page: Kartu and Grafik tabs, grouping per kegunaan or per rekening, the summary, rows of cards, capital gain everywhere, and the owner's saved order per grouping.

## Requirements

### Requirement: Wallet Page Layout
Rekening & Kantong SHALL open on "Kartu" grouped "Per kegunaan", and SHALL offer "Grafik" and "Per rekening". It SHALL show a summary (Kekayaan bersih, Saldo tersedia, Dana investasi, Investasi, untung/rugi, Utang), then one row of equal-size cards per use or per rekening, each row with its total. The grouping choice SHALL be remembered per browser.

#### Scenario: Per kegunaan
- **WHEN** the owner opens Rekening & Kantong
- **THEN** rows SHALL be Uang harian, Tabungan, Dana investasi, Investasi and Utang with their totals, the summary SHALL show a ring of uses, and the Investasi row SHALL show Emas, Reksa dana and Saham sub-totals

#### Scenario: Per rekening
- **WHEN** the owner chooses "Per rekening"
- **THEN** each rekening SHALL be one row with a bar of its composition by use and amounts, each card SHALL name its use, and the summary SHALL show composition bars per rekening

#### Scenario: Grafik
- **WHEN** the owner opens "Grafik"
- **THEN** a two-ring chart (uses inside, kantong and products outside) SHALL show beside the list of the chosen use, grouped by type for Investasi under Per kegunaan and by rekening under Per rekening

### Requirement: Capital Gain Everywhere
Every investment product SHALL show its capital gain in rupiah and percent on its card and in the Grafik list, and the page SHALL show totals per type, per rekening and overall. A total SHALL be summed only over products with a known cost basis and SHALL read "Modal belum diisi" when one is unknown.

#### Scenario: Gains on BCA
- **GIVEN** BCA holds Tabungan Emas +Rp 51.200 and two funds +Rp 18.400 and +Rp 47.400, all with known cost
- **WHEN** Rekening shows Per rekening
- **THEN** the BCA row SHALL show +Rp 117.000 with its percent, and each card SHALL show its own gain

#### Scenario: Unknown cost basis
- **WHEN** one Bibit fund has no cost basis
- **THEN** the Bibit row and the overall gain SHALL read "Modal belum diisi" and SHALL NOT show a partial sum

### Requirement: Saved Layout per Grouping
The owner SHALL be able to reorder rows and cards by dragging on desktop, by Alt + arrow keys, and by the "Ubah urutan" mode on phones. The order SHALL be saved per grouping through `PUT /api/accounts/layout`, SHALL survive a reload and other devices, and SHALL be scoped to the owner.

#### Scenario: Reorder and reload
- **WHEN** the owner moves Investasi to the top under Per kegunaan and Main Pocket before BCA ATM, then reloads on another device
- **THEN** the same order SHALL show under Per kegunaan, and Per rekening SHALL keep its own order

#### Scenario: Failed save
- **WHEN** the layout save fails
- **THEN** the previous order SHALL be restored and "Urutan gagal disimpan" SHALL be announced

#### Scenario: Unknown ids
- **WHEN** a layout PUT names an account of another owner or an unknown section
- **THEN** those ids SHALL be dropped, the owner's other accounts SHALL be appended in default order, and another owner's data SHALL never be stored or returned
