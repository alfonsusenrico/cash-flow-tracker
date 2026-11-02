## MODIFIED Requirements

### Requirement: Primary Expense Category Flagging
The system SHALL keep the `is_primary` flag on expense categories for compatibility and SHALL never use it to derive a pillar: not when a category is created, not when a category is updated, and not when a category or transaction is read or recorded. A category update that sends no `kakeibo_type` SHALL leave the stored pillar unchanged. Living expenses and runway SHALL be based on the effective Kakeibo pillar `need` as defined by `emergency-cost-estimation`, not on `is_primary`.

#### Scenario: Categorizing primary expenses
- **WHEN** an expense transaction is recorded in a category where `is_primary = true` and whose Kakeibo pillar is `want`
- **THEN** the transaction amount is not included in the essential cost, because its effective pillar is `want`

#### Scenario: Category without a stored pillar
- **WHEN** an expense is recorded in a primary category whose stored pillar is empty
- **THEN** the transaction SHALL be stored without a pillar, the category list SHALL return `kakeibo_type: null`, and reports SHALL count the expense as "Belum diklasifikasi"

#### Scenario: Primary category created without a pillar
- **WHEN** an expense category is created with `is_primary = true` and no `kakeibo_type`
- **THEN** it SHALL be stored without a pillar, not with `need`

#### Scenario: Primary flag changed without a pillar
- **GIVEN** expense categories with no pillar, with `want` and with `need`
- **WHEN** each is updated through `PATCH /api/categories/{id}` with only `is_primary`, true or false
- **THEN** each SHALL keep its stored pillar (none, `want`, `need`), the response SHALL report 0 corrections, and no feedback row SHALL be written
