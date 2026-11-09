# accounts-wallet-view Specification

## Purpose
The Rekening & Kantong page: Kartu and Grafik tabs, grouping per kegunaan or per rekening, the summary, rows of cards, capital gain everywhere, and the owner's saved order per grouping.

## Requirements

### Requirement: Wallet Page Layout
Rekening & Kantong SHALL open on "Kartu" grouped "Per kegunaan", and SHALL offer "Grafik" and "Per rekening". Below the title block it SHALL show the hero, then the two controls, then one group per use or per rekening, each with a 2 px straight rail, an uppercase label, a muted count, its gain where it exists and its total, and its tiles in a wrapping grid. The grouping SHALL be remembered per browser and the view SHALL be kept in the URL.

#### Scenario: Per kegunaan
- **WHEN** the owner opens Rekening & Kantong
- **THEN** the groups SHALL be Uang harian, Tabungan, Dana investasi, Investasi and Utang in the saved order, each with its total, and the Investasi group SHALL show one line per kind (Reksa dana, Saham, Emas) with its count, gain and total before the product tiles

#### Scenario: Per rekening
- **WHEN** the owner chooses "Per rekening"
- **THEN** each top-level account SHALL be one group whose rail takes the account's colour, whose header names its pocket count and its per-use subtotals in words, and whose tiles are its pockets, products or credit account
- **AND** no stacked composition bar SHALL be drawn

#### Scenario: Grafik
- **WHEN** the owner chooses "Grafik" and reloads the page
- **THEN** the page SHALL open on Grafik with `?tampilan=grafik`, showing the ring of uses beside its legend and the chosen use's contents as bars, and the grouping control SHALL NOT be shown on Grafik

#### Scenario: Tiles at desktop width
- **WHEN** the page is shown at 1440 px
- **THEN** tiles SHALL be at least 112 px tall with a 1 px neutral edge, radius 12 and no coloured edge, four in a row, and the first content SHALL start at y = 192 px

### Requirement: Capital Gain Everywhere
Every investment product SHALL show its capital gain in rupiah and percent on its tile, in Grafik and in its detail, and the page SHALL show the gain per kind, per platform and for the Investasi group. A gain SHALL read "▲ +Rp 396.000 · +8,0%" or "▼ −Rp 164.000 · −10,7%", with a hidden "untung" or "rugi". A total SHALL be summed only over products with a known cost basis and SHALL read "Modal belum diisi" when one is unknown.

#### Scenario: Gains on BCA
- **GIVEN** BCA holds Tabungan Emas +Rp 51.200 and two funds +Rp 18.400 and +Rp 47.400, all with known cost
- **WHEN** Rekening shows Per rekening
- **THEN** the BCA group SHALL show +Rp 117.000 with its percent, and each tile SHALL show its own gain

#### Scenario: Unknown cost basis
- **WHEN** one Bibit fund has no cost basis
- **THEN** the Bibit group and the Investasi group SHALL read "Modal belum diisi" and SHALL NOT show a partial sum

#### Scenario: Colour is not the only signal
- **WHEN** a product shows a loss in either theme
- **THEN** its gain SHALL carry "▼", the "−" sign and the hidden word "rugi" besides its colour

### Requirement: Saved Layout per Grouping
The owner SHALL be able to reorder groups and tiles in "Atur urutan" mode: by dragging on desktop, by Alt + arrow keys, and by move buttons on phones. The order SHALL be saved per grouping through `PUT /api/accounts/layout`, SHALL survive a reload and other devices, and SHALL be scoped to the owner.

#### Scenario: Reorder and reload
- **WHEN** the owner moves Investasi to the top under Per kegunaan and Main Pocket before BCA ATM, then reloads on another device
- **THEN** the same order SHALL show under Per kegunaan, and Per rekening SHALL keep its own order

#### Scenario: Failed save
- **WHEN** the layout save fails
- **THEN** the previous order SHALL be restored and "Urutan gagal disimpan" SHALL be announced

#### Scenario: Unknown ids
- **WHEN** a layout PUT names an account of another owner or an unknown section
- **THEN** those ids SHALL be dropped, the owner's other accounts SHALL be appended in default order, and another owner's data SHALL never be stored or returned

### Requirement: Rekening Hero
The page SHALL lead with one hero: "Kekayaan bersih" from the server's `net_worth` in the page's only 44 px figure, the page's only "Rincian" control, the line "dari aset {total_assets}", and a split bar of the four asset uses with a legend of their shares. "Rincian" SHALL open a dialog listing the server's net worth lines and their total. No other summary figure SHALL be shown above the groups.

#### Scenario: Hero and Rincian
- **GIVEN** net worth Rp 45.828.280, assets Rp 58.971.280, one credit account owing Rp 893.000 and two obligations owing Rp 12.250.000
- **WHEN** the owner opens Rekening & Kantong and activates "Rincian"
- **THEN** the hero SHALL read Rp 45.828.280 and "dari aset Rp 58.971.280", and the dialog SHALL list Saldo tersedia, Dana investasi, Investasi, each obligation as "Tagihan · {name}" and the credit account as "Utang · {name}", with the total Rp 45.828.280

#### Scenario: Split bar shares
- **WHEN** the hero is shown
- **THEN** the split bar SHALL carry its shares in its accessible name, and its legend SHALL read each use with its rounded share of assets, leaving out a use at zero

#### Scenario: One Rincian
- **WHEN** the page is shown in either view
- **THEN** exactly one control named "Rincian" SHALL be on the page

### Requirement: Rekening Groups and Tiles
Each tile SHALL be one button that opens its detail and SHALL show a 24 px mark in the account's colour, the name, one meta line, the balance and, for a product, its gain. A tile SHALL carry at most one note line, in this order: "Melewati limit", "Perlu dicocokkan", "{n} pindah saldo belum lengkap". A parent and its pockets SHALL be counted once in every total.

#### Scenario: Pocket tile
- **WHEN** the Jago pocket "Makan & Jajan" with Rp 1.489.000 is shown under Per kegunaan
- **THEN** its tile SHALL read "Makan & Jajan", "Kantong di Jago" and Rp 1.489.000, and activating it SHALL open its detail

#### Scenario: Readable mark
- **WHEN** an account's chosen colour is light
- **THEN** its mark's initials SHALL be ink instead of white, so they reach 4.5:1

#### Scenario: Parent counted once
- **GIVEN** a parent with Rp 100.000 of its own and two pockets of Rp 400.000 and Rp 500.000
- **WHEN** the groups are shown
- **THEN** the group totals SHALL include Rp 1.000.000 once, with the own balance as a tile "saldo di luar kantong"

### Requirement: Utang Without Obligations
Rekening & Kantong SHALL NOT show obligation tiles. The Utang group SHALL list credit accounts only, and its total SHALL be what they owe (`credit_owed`). When obligations are active, one whole-row link "Tagihan di Target & Tagihan" SHALL follow with their count and `total_liabilities − credit_owed`, opening `/goals`. Kekayaan bersih SHALL stay the server's figure, still subtracting obligations.

#### Scenario: Credit and obligations
- **GIVEN** SPayLater owes Rp 893.000 and two obligations owe Rp 12.250.000
- **WHEN** the owner opens Per kegunaan
- **THEN** Utang SHALL show the SPayLater tile and the total −Rp 893.000, then "Tagihan di Target & Tagihan · 2 tagihan" with −Rp 12.250.000, which opens `/goals`

#### Scenario: Per rekening
- **WHEN** the owner opens Per rekening with active obligations
- **THEN** no "Utang lain" group SHALL be shown, and the Tagihan link row SHALL follow the last group

#### Scenario: No obligation
- **WHEN** no obligation is active
- **THEN** no Tagihan row SHALL be shown

### Requirement: Rekening Grafik
Grafik SHALL show one ring of the four asset uses, labelled in the centre with the total assets, beside a legend that lists each use with its amount and share and lets the owner choose one. The chosen use's contents SHALL follow as horizontal bars: for Investasi, "Investasi per jenis" and "Per produk" with their gains, otherwise one bar per kantong. No value SHALL be readable only from the ring.

#### Scenario: Choosing Tabungan
- **WHEN** the owner presses "Tabungan" in the legend
- **THEN** it SHALL be marked by the straight bar and `aria-pressed`, and the bars SHALL list each Tabungan pocket with its amount and share of assets

#### Scenario: Investasi bars
- **WHEN** Investasi is chosen
- **THEN** each kind and each product SHALL have one bar with its amount and gain, and a product's bar SHALL open its detail

#### Scenario: Grafik on a phone
- **WHEN** Grafik is shown at 390 px
- **THEN** each bar SHALL sit under its label and amount, and nothing SHALL overflow horizontally

### Requirement: Atur Urutan Mode
"Atur urutan" in the page's "…" menu SHALL turn on a reorder mode with one instruction bar and "Selesai". Only in this mode SHALL tiles be dragged, moved with Alt + arrow keys, or moved with move buttons on phones, and a tile SHALL NOT open its detail. Each move SHALL be announced, and focus SHALL stay on the moved element.

#### Scenario: Keyboard move
- **WHEN** the owner turns on Atur urutan, focuses "GoPay" and presses Alt + ArrowRight
- **THEN** GoPay SHALL move one place later, "GoPay dipindah ke posisi 4 dari 8." SHALL be announced, focus SHALL stay on GoPay and the order SHALL be saved

#### Scenario: Outside the mode
- **WHEN** the mode is off and the owner presses Alt + ArrowRight on a tile, or drags it
- **THEN** nothing SHALL move, and no drag hint SHALL be shown on the page

#### Scenario: Phone move buttons
- **WHEN** the mode is on at 390 px
- **THEN** each tile and group SHALL show 44 px move buttons, and the first item's "earlier" button SHALL be disabled

### Requirement: Account Detail Dialog
A tile SHALL open a centered 560 px dialog, a bottom sheet on phones, with the account's figure, a label and value list, its pockets or products, "Juga dilaporkan" and the last four transactions. Its footer SHALL show one primary action, one secondary action and the other supported actions under "Lainnya". It SHALL be openable from other pages and SHALL return focus to its opener.

#### Scenario: Product actions
- **WHEN** the owner opens a unit-tracked stock
- **THEN** "Beli" SHALL be primary, "Jual" secondary, and "Perbarui nilai", "Sesuaikan saldo", "Ubah kantong" and "Arsipkan" SHALL be under "Lainnya"

#### Scenario: Recent expense in red
- **WHEN** the detail lists an expense of Rp 30.000
- **THEN** it SHALL read "−Rp 30.000" in the expense colour

#### Scenario: Deep link
- **WHEN** the owner opens `/accounts?akun=<id>` for an archived or unknown account
- **THEN** no dialog SHALL open and the parameter SHALL be cleared

### Requirement: Rekening Loading, Failure and Empty States
While accounts load, the page SHALL show skeleton figures and tiles marked busy. When the accounts request fails, it SHALL show one error box with "Coba lagi" and no figures. When the net worth request fails, only the hero SHALL become an error box, and the groups SHALL still show. With no active account, it SHALL show "Belum ada rekening" with "Tambah rekening".

#### Scenario: Net worth fails
- **WHEN** `GET /api/dashboard/net-worth` fails and `GET /api/accounts` succeeds
- **THEN** the hero SHALL read "Kekayaan bersih belum dapat dihitung." with "Coba lagi", no Rp 0 SHALL be shown, and the groups SHALL show their totals

#### Scenario: No accounts
- **WHEN** the owner has no active account
- **THEN** the page SHALL show the empty state with "Tambah rekening" and no hero or controls

### Requirement: Rekening in Both Languages
Rekening & Kantong and its dialogs SHALL be shown in the interface language, from the `accounts` namespace and the shared `terms`, with no untranslated notice. Every figure, count, date and gain SHALL be identical in English and Indonesian for the same data and region. Account, pocket, product and obligation names SHALL be shown as stored.

#### Scenario: English account
- **WHEN** an English account opens `/accounts`
- **THEN** the title SHALL read "Accounts & Pockets", the groups "Everyday money", "Savings", "Investment cash", "Investments" and "Debt", and "This page is in Indonesian for now." SHALL NOT be shown

#### Scenario: Same figures
- **WHEN** the same data is shown to an English and an Indonesian account in region Indonesia
- **THEN** every amount, percent and count SHALL be the same string
