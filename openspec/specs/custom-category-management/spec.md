# custom-category-management Specification

## Purpose
Allow users to manage transaction categories and apply category budget variance filters.

## Requirements

### Requirement: Custom Category Creation & Management
The system SHALL provide full category lifecycle management allowing users to create, update, archive, and query custom categories.

#### Scenario: Required behavior and constraints
- **WHEN** an owner manages custom transaction categories
- **THEN** the following required behavior and constraints hold:

  The system SHALL provide full category lifecycle management allowing users to create, update, archive, and query custom categories:
  1. Category forms SHALL expose `name`, `kind` (`income` or `expense`), Kakeibo pillar where applicable, `is_primary`, optional color, optional icon, and optional monthly budget.
  2. Income categories SHALL not present expense-only budget or Kakeibo controls unless the underlying contract supports them.
  3. Existing values SHALL load when editing and every persisted attribute SHALL remain editable where compatible.
  4. Color and icon choices SHALL expose names and selected states without relying on appearance alone.
  5. The system SHALL expose `GET /api/categories`, `POST /api/categories`, `PATCH /api/categories/{id}`, and soft-delete/archive behavior.
  6. Transaction forms SHALL filter category choices by the selected transaction type.

#### Scenario: Creating an optional expense category
- **WHEN** a user creates "Hobi" as an expense, selects `want`, marks it optional, and sets a 500,000 IDR budget
- **THEN** all selected attributes are persisted and the category appears only in expense selections

#### Scenario: Creating an income category
- **WHEN** a user creates an income category named "Bonus"
- **THEN** it appears in income transaction forms and not in expense-only budget tables

#### Scenario: Creating a custom expense category
- **WHEN** a user creates "Hobi" as an optional expense category with a 500,000 IDR monthly budget
- **THEN** the category is persisted with its selected classification and appears in expense category choices

### Requirement: Category Budget Variance Filtering
The `/insights` category budget matrix SHALL strictly filter and display categories of kind `expense`:
1. Income categories (e.g. "Gaji", "Pendapatan Lain") SHALL NOT be displayed in the Category Spending Variance or Budget table.
2. Users SHALL be able to click directly on any category's budget cell to edit or set its `monthly_budget` ceiling via an inline modal.
3. While no expense category has a budget, the table SHALL show no budget figure or column and be titled "Pengeluaran per Kategori".

#### Scenario: Viewing the category budget matrix
- **WHEN** the user views the Category Budget table on the `/insights` page
- **THEN** only expense categories are shown, each with their `Pokok` or `Opsional` badge, actual spending, and budget ceiling

#### Scenario: No budget set
- **WHEN** no expense category has a monthly budget
- **THEN** the table SHALL list categories with their spending only, without "Tanpa Batas" chips, budget columns or "Atur batas" buttons

### Requirement: Snack and Café Default Category
Every user SHALL have the expense category "Jajan & Nongkrong" with the Kakeibo pillar `want`, separate from daily meals. The migration SHALL create it only where the user has no expense category of that name, SHALL leave an existing one unchanged, and SHALL NOT move any existing transaction into it.

#### Scenario: Existing user without the category
- **GIVEN** a user whose food spending is all in "Makanan & Minuman"
- **WHEN** the migration runs
- **THEN** the user SHALL have the expense category "Jajan & Nongkrong" with pillar `want`
- **AND** every existing transaction SHALL keep its category.

#### Scenario: Existing category is kept
- **GIVEN** a user who already has an expense category "Jajan & Nongkrong" with pillar `need`
- **WHEN** the migration runs
- **THEN** the category SHALL keep pillar `need`.

#### Scenario: New user defaults
- **WHEN** a new user registers
- **THEN** the default categories SHALL include "Jajan & Nongkrong" with pillar `want`.

### Requirement: New User Movement Categories
At registration, the seeded expense and income "Internal Movement" categories SHALL be excluded from budget and SHALL carry no Kakeibo pillar, so that movement legs never count as spending, income or a pillar from the first movement on.

#### Scenario: Seeded movement categories
- **WHEN** a new user registers
- **THEN** both "Internal Movement" categories SHALL have `is_excluded_from_budget = true` and no pillar
- **AND** no other seeded category SHALL be excluded from budget.

### Requirement: System Category Icons Render
The web icon set SHALL draw every icon that seeded or system categories use (`coffee`, `users`, `receipt`, `arrows-right-left` among them) instead of the generic tag fallback, and the category icon picker SHALL offer `coffee` ("Jajan") and `users` ("Patungan"). Stored category icons SHALL NOT be rewritten.

#### Scenario: Snack category icon
- **WHEN** the category "Jajan & Nongkrong" with icon `coffee` is shown
- **THEN** a coffee cup icon SHALL be drawn, not the tag fallback.

#### Scenario: Unknown icon
- **WHEN** a category carries an icon name the web does not know
- **THEN** the tag fallback SHALL be drawn.

### Requirement: Starter Category Pillars
At registration, the seeded expense categories SHALL carry default Kakeibo pillars: "Makanan & Minuman", "Belanja Harian", "Transportasi", "Tagihan & Utilitas" and "Kesehatan" `need`; "Jajan & Nongkrong", "Belanja" and "Hiburan" `want`. "Investasi", both "Internal Movement" categories and the income categories SHALL carry none. Seeding SHALL apply only to a user without categories and SHALL NOT change any existing user's categories or transactions.

#### Scenario: New user starter pillars
- **WHEN** a new user registers
- **THEN** the seeded categories SHALL carry exactly these pillars, and an expense in "Transportasi" SHALL count as `need` without any classification step

#### Scenario: Existing user unchanged
- **GIVEN** an existing user whose "Makanan & Minuman" category has no pillar
- **WHEN** the starter defaults are applied again
- **THEN** the category SHALL keep no pillar and no category SHALL be added

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
