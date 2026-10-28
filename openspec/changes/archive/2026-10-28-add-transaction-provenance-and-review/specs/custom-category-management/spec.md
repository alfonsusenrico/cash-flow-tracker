## ADDED Requirements

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
