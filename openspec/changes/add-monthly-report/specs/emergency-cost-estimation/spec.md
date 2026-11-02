## MODIFIED Requirements

### Requirement: Essential spending scope
Per-period essential spending SHALL include only expenses whose effective Kakeibo pillar is `need` and, until they are classified, unclassified expenses (no transaction or category pillar), reduced by their proportional share of applied reimbursement offsets. It SHALL exclude internal movement legs, categories excluded from budget and the "Internal Movement" and "Investasi" categories, expenses on investment accounts, and expenses linked to a debt. Expenses whose effective pillar is `want`, `culture`, `unexpected` or `saving` SHALL be excluded. Each period SHALL report its unclassified share separately as `unclassified_amount`.

#### Scenario: Reimbursement in a mixed category
- **GIVEN** a period with Rp 1.000.000 of `need` groceries, Rp 100.000 of `need` and Rp 300.000 of `want` spending in one category, and a Rp 200.000 reimbursement offsetting that category
- **WHEN** the period's essential spending is computed
- **THEN** gross need SHALL be Rp 1.100.000, the reimbursement share Rp 50.000 and essential spending Rp 1.050.000

#### Scenario: Debt counted once
- **GIVEN** an active debt with a Rp 500.000 minimum and, in each period, Rp 2.000.000 of `need` spending plus a Rp 500.000 `need` expense linked to that debt
- **WHEN** the essential cost is computed
- **THEN** each period SHALL be Rp 2.000.000 and the essential cost Rp 2.500.000

#### Scenario: Movements, investments and excluded categories
- **WHEN** a period contains a movement leg, an expense on an investment account and an expense in a category excluded from budget
- **THEN** none of them SHALL count as essential spending

#### Scenario: Unclassified spending counts and is shown
- **GIVEN** a completed period with Rp 2.000.000 of `need` and Rp 300.000 of unclassified spending
- **WHEN** the period's essential spending is computed
- **THEN** it SHALL be Rp 2.300.000 with `unclassified_amount` Rp 300.000

### Requirement: Essential cost fallback and unknown state
When no completed period is available, the system SHALL use the sum of monthly budgets of active, reportable `need` categories and categories without a pillar plus debt minimums, labelled as a budget basis. Without such budgets the essential cost SHALL be unknown (`null`); the system SHALL NOT substitute 0 or a sentinel.

#### Scenario: Budget fallback
- **GIVEN** no completed period, a `need` category budget of Rp 1.500.000 and a `want` category budget of Rp 9.000.000
- **WHEN** the essential cost is computed
- **THEN** it SHALL be Rp 1.500.000 with basis `budget`

#### Scenario: Nothing known
- **GIVEN** no completed period and no `need` budget
- **WHEN** the essential cost is computed
- **THEN** its amount SHALL be `null` with basis `unknown`

### Requirement: Essential cost basis disclosure
Every response with the essential cost SHALL include its method, basis, the number of periods used, each period's kind, label, boundaries and amounts, including its unclassified share, for drill-down, the debt minimum, and `limited_history: true` when fewer than three periods were used. The interface SHALL show this basis and link each period to its ledger rows.

#### Scenario: Limited history
- **WHEN** only one completed period is available
- **THEN** the response SHALL carry `limited_history: true` and the interface SHALL label the cost as based on limited history

#### Scenario: Drill-down to a period
- **WHEN** the owner opens the periods behind the essential cost
- **THEN** each period SHALL link to the ledger filtered to its exact window, `type=expense` and `kakeibo=need`, and its unclassified share, when above zero, SHALL link to the same window with `kakeibo=unclassified`
