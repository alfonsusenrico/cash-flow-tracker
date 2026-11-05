## MODIFIED Requirements

### Requirement: Net Fresh Savings Calculation
The system SHALL classify savings from the economic direction of a durably linked movement.

#### Scenario: Required behavior and constraints
- **WHEN** savings are calculated from linked internal movements
- **THEN** the following required behavior and constraints hold:

  The system SHALL classify savings from the economic direction of a durably linked movement:
  1. A savings destination is an account with role `tabungan`, an account linked to a goal, or an investment product. Generic movements cannot target an investment position; an investment product is funded through a trade or an investment top-up.
  2. A movement from an operational liquid account into a savings destination SHALL increase net fresh savings ("Masuk ke tabungan").
  3. A movement from a savings source back into an operational liquid account SHALL reduce net fresh savings ("Diambil dari tabungan").
  4. A movement between operational liquid accounts, including a liquid investment-funding account such as an RDN, SHALL not affect savings.
  5. A trade between a funding account and an investment instrument position SHALL remain excluded from Kakeibo turnover.
  6. Manual, plan-recorded, captured and ingested movements SHALL use the same classification rule; automation SHALL NOT mark every transfer as saving.
  7. The classification SHALL use the accounts' current roles and goal links at read time, so a role change applies to earlier movements too.
  8. An expense paid from a savings account SHALL stay in its spending pillar and SHALL reduce net fresh savings ("Pengeluaran dari tabungan").
  9. Net fresh savings MAY be negative; Keluar SHALL equal spending plus net fresh savings, and Beranda SHALL read "Tabungan bertambah Rp X" for a positive value and "Tabungan berkurang Rp X" for a negative one.

#### Scenario: User transfers funds from Bank to Investment account
- **WHEN** a user tops up an amount-tracked investment product from an operational bank account
- **THEN** the top-up SHALL be recognized as monthly savings

#### Scenario: Moving money into a goal-linked pocket
- **WHEN** a user moves money from an operational bank account into a pocket linked to a savings goal
- **THEN** the amount increases net fresh savings

#### Scenario: Subtracting a savings withdrawal
- **WHEN** a user moves money from a goal-linked savings pocket back to an operational bank account
- **THEN** the amount reduces net fresh savings for the active period

#### Scenario: User transfers funds between checking or e-wallet accounts
- **WHEN** a user moves funds between operational checking accounts, pockets, e-wallets or a liquid RDN funding account
- **THEN** the movement does not affect the savings pillar

#### Scenario: Moving money into a Tabungan pocket without a goal
- **WHEN** a user moves Rp 1.000.000 from Main Pocket into a pocket with role `tabungan` and no goal
- **THEN** net fresh savings SHALL grow by Rp 1.000.000

#### Scenario: Paying from savings
- **GIVEN** Masuk Rp 10.000.000 and Rp 3.000.000 moved into Dana Darurat (role `tabungan`) in the cycle
- **WHEN** a Rp 1.000.000 repair is paid from Dana Darurat
- **THEN** Pengeluaran SHALL include Rp 1.000.000, net fresh savings SHALL be Rp 2.000.000, and Aman dipakai SHALL be Rp 7.000.000 before Terjadwal

#### Scenario: More taken out than saved
- **WHEN** Rp 300.000 goes into Tabungan and Rp 800.000 comes out in one cycle
- **THEN** net fresh savings SHALL be −Rp 500.000, Keluar SHALL equal spending minus Rp 500.000, and Beranda SHALL read "Tabungan berkurang Rp 500.000"

#### Scenario: Role applies to past movements
- **GIVEN** a movement last month into a pocket then set to role `tabungan`
- **WHEN** last month's Kakeibo is read
- **THEN** that movement SHALL count as savings
