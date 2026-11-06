## MODIFIED Requirements

### Requirement: Penyesuaian Saldo System Category
The audit transaction SHALL use the user's balance-adjustment system category (key `balance_adjustment`, seeded name "Penyesuaian Saldo") of the matching kind (income or expense), whatever its current name. That category SHALL be excluded from budget, Kakeibo, spending and income reports, and SHALL carry no Kakeibo pillar. The system SHALL create the category when no category of that kind carries the key.

#### Scenario: Missing adjustment category is created
- **GIVEN** a user without an income "Penyesuaian Saldo" category
- **WHEN** the user reconciles an account upwards
- **THEN** the system SHALL create the income category "Penyesuaian Saldo" with key `balance_adjustment`, excluded from budget and without a pillar, and file the adjustment under it.

#### Scenario: Renamed adjustment category is reused
- **GIVEN** the user renamed the expense adjustment category to "Koreksi saldo"
- **WHEN** the user reconciles an account downwards
- **THEN** the adjustment SHALL be filed under "Koreksi saldo", and no new "Penyesuaian Saldo" category SHALL be created.
