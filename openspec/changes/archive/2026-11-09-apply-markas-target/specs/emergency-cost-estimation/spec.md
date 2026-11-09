## MODIFIED Requirements

### Requirement: Essential cost basis disclosure
Every response with the essential cost SHALL include its method, basis, the number of periods used, each period's kind, label, boundaries and amounts, including its unclassified share, for drill-down, the debt minimum, and `limited_history: true` when fewer than three periods were used. The interface SHALL show this basis in the Ketahanan Dana "Rincian" of Target & Tagihan and link each period to its ledger rows.

#### Scenario: Limited history
- **WHEN** only one completed period is available
- **THEN** the response SHALL carry `limited_history: true` and the interface SHALL label the cost as based on limited history

#### Scenario: Drill-down to a period
- **WHEN** the owner opens the periods behind the essential cost
- **THEN** each period SHALL link to the ledger filtered to its exact window, `type=expense` and `kakeibo=need`, and its unclassified share, when above zero, SHALL link to the same window with `kakeibo=unclassified`

#### Scenario: Basis in the Rincian
- **GIVEN** an essential cost from the median of three periods plus Rp 850.000 of minimum instalments
- **WHEN** the owner opens "Rincian" on the Ketahanan Dana hero
- **THEN** the "Biaya hidup pokok bulanan" line SHALL open the three periods, each a ledger link, and SHALL note "Termasuk cicilan minimum Rp 850.000"
