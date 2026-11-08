## MODIFIED Requirements

### Requirement: Starter Category Pillars
At registration, the seeded expense categories SHALL carry default Kakeibo pillars: "Makanan & Minuman", "Belanja Harian", "Transportasi", "Tagihan & Utilitas" and "Kesehatan" `need`; "Jajan & Nongkrong", "Belanja" and "Hiburan" `want`. "Investasi", both "Internal Movement" categories and the income categories SHALL carry none. An English account's starter categories SHALL carry the same pillars under their English names. Seeding SHALL apply only to a user without categories.

#### Scenario: New user starter pillars
- **WHEN** a new user registers
- **THEN** the seeded categories SHALL carry exactly these pillars, and an expense in "Transportasi" SHALL count as `need` without any classification step

#### Scenario: English starter pillars
- **WHEN** a new English account registers
- **THEN** "Food & drinks", "Groceries", "Transport", "Bills & utilities" and "Health" SHALL carry `need`, "Snacks & going out", "Shopping" and "Entertainment" `want`, and "Investments", both "Transfer" categories and the income categories none

#### Scenario: Existing user unchanged
- **GIVEN** an existing user whose "Makanan & Minuman" category has no pillar
- **WHEN** the starter defaults are applied again
- **THEN** the category SHALL keep no pillar and no category SHALL be added

### Requirement: Snack and Café Default Category
Every user SHALL have an expense category for snacks and cafés with the Kakeibo pillar `want`, separate from daily meals: "Jajan & Nongkrong" for Indonesian accounts and "Snacks & going out" for accounts created in English. The migration SHALL create it only where the user has no expense category of that name, SHALL leave an existing one unchanged, and SHALL NOT move any existing transaction into it.

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
- **THEN** the default categories SHALL include "Jajan & Nongkrong" with pillar `want`, or "Snacks & going out" with pillar `want` for an English account.

## ADDED Requirements

### Requirement: Starter Names Follow the Account Language
At registration, starter categories and accounts SHALL be named in the account's language, with the same icons, colours, kinds, pillars, flags and system keys in every language. Categories the system creates later (movement, balance adjustment) SHALL be named in the account's language at creation. Existing names SHALL never be renamed.

#### Scenario: English starter data
- **WHEN** a new English account registers
- **THEN** it SHALL have the accounts "Cash" and "Main account", "Investments" with key `investment`, and two "Transfer" categories with key `internal_movement` excluded from budget

#### Scenario: Switching language renames nothing
- **GIVEN** an Indonesian account with its starter categories
- **WHEN** the owner switches the interface to English
- **THEN** every category and account SHALL keep its Indonesian name

#### Scenario: Bank hints still resolve
- **WHEN** a myBCA notification with the category "Shopping" is processed for an English account
- **THEN** it SHALL be offered or filed under that account's "Shopping" category, as "Belanja" is for an Indonesian account
