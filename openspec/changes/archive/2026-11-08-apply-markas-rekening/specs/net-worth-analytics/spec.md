## MODIFIED Requirements

### Requirement: Net Worth Endpoint and Display
`GET /api/dashboard/net-worth` SHALL return `liquid`, `investment`, `total_assets`, `reconciliation_pending`, `total_liabilities`, `net_worth` and `lines` (the cycle summary's `key`, `label`, `amount` lines, one per part and per active debt) besides its existing fields. The web SHALL show these server figures without computing them, and when the request fails it SHALL say the figure cannot be calculated instead of showing Rp 0.

#### Scenario: Net worth unavailable on the page
- **WHEN** `GET /api/dashboard/net-worth` fails
- **THEN** the hero of Rekening & Kantong SHALL show "Kekayaan bersih belum dapat dihitung." with a retry action and SHALL NOT show Rp 0 or a sum computed in the browser, and the account groups SHALL still be shown

### Requirement: Asset Composition on Rekening
Rekening & Kantong SHALL show the composition of assets in its hero: a split bar of Uang harian, Tabungan, Dana investasi and Investasi, each segment the use's total from the account list, with a legend of each use's share of `total_assets`. Debt SHALL NOT appear in it. The shares SHALL be in the bar's accessible name and in the legend, so the bar is never the only carrier of the values.

#### Scenario: Shares add up
- **GIVEN** Uang harian Rp 11.680.520, Tabungan Rp 16.300.000, Dana investasi Rp 771.060 and Investasi Rp 30.219.700, with Total aset Rp 58.971.280
- **WHEN** the owner opens Rekening & Kantong
- **THEN** the legend SHALL read 20%, 28%, 1% and 51%, and no debt SHALL be part of the bar

#### Scenario: Net worth unavailable
- **WHEN** the net worth request fails
- **THEN** the hero SHALL say that the figure cannot be calculated, SHALL show no split bar and no Rp 0

### Requirement: Investment Gain in the Account List
`GET /api/accounts` SHALL add `group_capital_gain` and `group_cost_basis` to a parent account whose children are investments, computed on the server with the per-product capital gain rule. Rekening SHALL show each product's value with its gain, and each platform with its group gain, as "▲ +Rp … · +x,y%" or "▼ −Rp … · −x,y%" with one decimal, never by colour alone.

#### Scenario: Gain and loss on one platform
- **GIVEN** a platform holding one stock with a loss of Rp 480.000 and one with a gain of Rp 192.000
- **WHEN** the owner opens Rekening & Kantong
- **THEN** the product tiles SHALL read "▼ −Rp 480.000 · −8,3%" and "▲ +Rp 192.000 · +15,2%", and the platform group SHALL read "▼ −Rp 288.000" with its percentage from `group_cost_basis`

#### Scenario: Missing cost basis
- **WHEN** one product of a platform has no recorded cost basis
- **THEN** that product SHALL say "Modal belum diisi", `group_capital_gain` SHALL be `null`, and the platform group SHALL say "Modal belum lengkap" instead of a gain
