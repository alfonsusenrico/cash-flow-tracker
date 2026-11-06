## ADDED Requirements

### Requirement: System Category Identity
Each owner's investment, movement and balance-adjustment categories SHALL carry a stable system key (`investment`, `internal_movement`, `balance_adjustment`), at most one per owner, key and kind. Every rule that treats a category specially SHALL read the key, never the category's name. Seeded and automatically created system categories SHALL carry their key. Every other category SHALL carry none.

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

### Requirement: Renaming a System Category Keeps Figures
Renaming a system category SHALL change only its displayed name. Spending, income, movement, investment, pairing, review and report results SHALL stay exactly as before the rename.

#### Scenario: Renamed investment category
- **GIVEN** an investment buy of Rp 1.000.000 in "Investasi" this cycle
- **WHEN** the owner renames "Investasi" to "Investments"
- **THEN** Beranda's Pengeluaran, the overview's total outflow, the monthly report and the ledger's "Investasi" type filter SHALL show the same amounts and rows as before

#### Scenario: Renamed movement category
- **GIVEN** a linked movement of Rp 500.000 and an unlinked "Internal Movement" income waiting for its partner
- **WHEN** the owner renames both movement categories to "Transfer"
- **THEN** the movement SHALL still count as neither spending nor income, and the unlinked leg SHALL still be offered for pairing in Tinjau

### Requirement: System Category Key in the API
`GET /api/categories` SHALL return each category's `system_key` (a key or `null`), and transaction rows SHALL return `category_system_key`. Clients SHALL NOT be able to set or change a key through `POST` or `PATCH /api/categories`.

#### Scenario: Key is returned
- **WHEN** the owner lists categories
- **THEN** "Investasi" SHALL be returned with `system_key` `investment` and "Makanan & Minuman" with `system_key` `null`

#### Scenario: Key cannot be set by a client
- **WHEN** a client sends `system_key: "investment"` when creating or updating "Hobi"
- **THEN** "Hobi" SHALL keep `system_key` `null`, and its spending SHALL still count as spending
