## ADDED Requirements

### Requirement: Asset Composition on Rekening
Rekening & Kantong SHALL show "Komposisi aset": a segmented bar and a legend of every top-level account with its share of Total aset and its amount, from the server's figures, ordered by amount. Debt SHALL NOT appear in this card. Segments SHALL carry the account colours and each legend row SHALL name its account, so the bar is never the only carrier of the values.

#### Scenario: Shares add up
- **GIVEN** Total aset Rp 68.763.549 across eight top-level accounts
- **WHEN** the owner opens Rekening & Kantong
- **THEN** the eight legend amounts SHALL add up to Rp 68.763.549, and no debt line SHALL be shown in the card

#### Scenario: Net worth unavailable
- **WHEN** the net worth request fails
- **THEN** the card SHALL say that the composition cannot be shown and SHALL show no Rp 0

### Requirement: Investment Gain in the Account List
`GET /api/accounts` SHALL add `group_capital_gain` and `group_cost_basis` to a parent account whose children are investments, computed on the server with the per-product capital gain rule. Rekening SHALL show each product's value with its `capital_gain` and `capital_gain_pct`, and each platform with its group gain, marked by a sign and ▲ or ▼, never by colour alone.

#### Scenario: Gain and loss on one platform
- **GIVEN** a platform holding one stock with a loss of Rp 480.000 and one with a gain of Rp 192.000
- **WHEN** the owner opens Rekening & Kantong
- **THEN** the product rows SHALL read "▼ −Rp 480.000 (−8,28%)" and "▲ +Rp 192.000 (+15,24%)", and the platform head SHALL read "▼ −Rp 288.000" with its percentage from `group_cost_basis`

#### Scenario: Missing cost basis
- **WHEN** one product of a platform has no recorded cost basis
- **THEN** that product SHALL say "Modal belum dicatat", `group_capital_gain` SHALL be `null`, and the platform head SHALL say "Modal belum lengkap" instead of a gain
