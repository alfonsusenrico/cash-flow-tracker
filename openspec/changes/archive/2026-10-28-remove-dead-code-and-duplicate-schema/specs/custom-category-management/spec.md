## ADDED Requirements

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
