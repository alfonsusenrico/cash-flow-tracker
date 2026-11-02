## MODIFIED Requirements

### Requirement: Primary Expense Category Flagging
The system SHALL keep the `is_primary` flag on expense categories for compatibility and SHALL use it only to derive the stored Kakeibo pillar of a category created or updated without one (`need` when primary, `want` otherwise). It SHALL NOT derive a pillar when a category or transaction is read, and SHALL NOT copy a derived pillar onto a transaction recorded in a category without a pillar; such an expense stays unclassified. Living expenses and runway SHALL be based on the effective Kakeibo pillar `need` as defined by `emergency-cost-estimation`, not on `is_primary`.

#### Scenario: Categorizing primary expenses
- **WHEN** an expense transaction is recorded in a category where `is_primary = true` and whose Kakeibo pillar is `want`
- **THEN** the transaction amount is not included in the essential cost, because its effective pillar is `want`

#### Scenario: Category without a stored pillar
- **WHEN** an expense is recorded in a primary category whose stored pillar is empty
- **THEN** the transaction SHALL be stored without a pillar, the category list SHALL return `kakeibo_type: null`, and reports SHALL count the expense as "Belum diklasifikasi"
