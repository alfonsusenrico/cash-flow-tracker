## MODIFIED Requirements

### Requirement: System Category Identity
Each owner's investment, movement, balance-adjustment and credit-fee categories SHALL carry a stable system key (`investment`, `internal_movement`, `balance_adjustment`, `credit_fee`), at most one per owner, key and kind. Every rule that treats a category specially SHALL read the key, never the category's name. Seeded and automatically created system categories SHALL carry their key. Every other category SHALL carry none.

#### Scenario: Seeded system categories carry keys
- **WHEN** a new user registers
- **THEN** "Investasi" SHALL carry `investment`, both "Internal Movement" categories SHALL carry `internal_movement`, and no other seeded category SHALL carry a key

#### Scenario: Existing categories keep their treatment
- **GIVEN** an owner with "Investasi" (expense), "Internal Movement" (expense and income), an archived income "Internal Movement", a "Penyesuaian Saldo" expense, and an expense named "investasi" in lower case
- **WHEN** the system-key migration runs
- **THEN** the first four SHALL carry their keys, the lower-case "investasi" SHALL carry none, and every Beranda, ledger, analytics and report figure SHALL equal its value before the migration

#### Scenario: A later category with a system name is ordinary
- **GIVEN** an owner who renamed "Investasi" to "Investments" after the migration
- **WHEN** the owner creates a new expense category named "Investasi" and records Rp 100.000 in it
- **THEN** the Rp 100.000 SHALL count as spending, and an investment buy in "Investments" SHALL still not count as spending

#### Scenario: Credit fee category
- **GIVEN** an owner with an expense "Biaya kredit" holding Rp 10.000 and an income named "Biaya kredit"
- **WHEN** the credit-fee migration runs
- **THEN** only the expense SHALL carry `credit_fee`, its Rp 10.000 SHALL still count as spending, and every figure SHALL equal its value before the migration

#### Scenario: Credit fee category in English
- **WHEN** an English account without a credit-fee category books its first credit fee
- **THEN** the category SHALL be created as "Credit fee" with `credit_fee`, and a renamed credit-fee category SHALL still be found by its key
