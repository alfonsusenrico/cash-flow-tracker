# emergency-cost-estimation Specification

## Purpose
Estimate the owner's essential monthly cost from observed completed periods and express the emergency reserve as months of that cost, with a visible basis and an explicit unknown state instead of invented values.

## Requirements

### Requirement: Essential monthly cost from completed periods
The system SHALL compute the essential monthly cost as the median of per-period essential spending over up to the last six completed periods, plus the active debt minimums once. A period is a closed confirmed salary cycle, or a complete calendar month in the application timezone before the first confirmed salary or when none is confirmed. The current period and periods that began before the owner's first recorded transaction SHALL NOT be used.

#### Scenario: One month with a large unexpected cost
- **GIVEN** four completed months each with Rp 2.000.000 of `need` spending, and one of them also with Rp 30.000.000 of `unexpected` spending
- **WHEN** the essential cost is computed
- **THEN** it SHALL be Rp 2.000.000

#### Scenario: A large one-off need expense
- **GIVEN** five completed months with `need` spending of Rp 2.000.000, Rp 2.100.000, Rp 24.900.000, Rp 2.000.000 and Rp 2.000.000
- **WHEN** the essential cost is computed
- **THEN** it SHALL be the median Rp 2.000.000

#### Scenario: Even number of periods
- **WHEN** the two periods used are Rp 1.000.001 and Rp 1.000.002
- **THEN** the median SHALL be Rp 1.000.002, the mean of the middle pair rounded half up to whole rupiah

#### Scenario: Confirmed salary cycles
- **GIVEN** confirmed salary receipts that close two cycles and earlier complete calendar months of history
- **WHEN** the essential cost is computed
- **THEN** the two closed cycles SHALL be used first, bounded by the receipt instants, followed by complete calendar months that ended before the first confirmed receipt, up to six periods

#### Scenario: Month before tracking began
- **WHEN** the owner's first recorded transaction is on the 5th of a month
- **THEN** that month SHALL NOT be used as a period

### Requirement: Essential spending scope
Per-period essential spending SHALL include only expenses whose effective Kakeibo pillar is `need`, reduced by their proportional share of applied reimbursement offsets. It SHALL exclude internal movement legs, categories excluded from budget and the "Internal Movement" and "Investasi" categories, expenses on investment accounts, and expenses linked to a debt. Expenses whose effective pillar is `want`, `culture`, `unexpected` or `saving` SHALL be excluded.

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

### Requirement: Essential cost fallback and unknown state
When no completed period is available, the system SHALL use the sum of monthly budgets of active, reportable `need` categories plus debt minimums, labelled as a budget basis. Without such budgets the essential cost SHALL be unknown (`null`); the system SHALL NOT substitute 0 or a sentinel.

#### Scenario: Budget fallback
- **GIVEN** no completed period, a `need` category budget of Rp 1.500.000 and a `want` category budget of Rp 9.000.000
- **WHEN** the essential cost is computed
- **THEN** it SHALL be Rp 1.500.000 with basis `budget`

#### Scenario: Nothing known
- **GIVEN** no completed period and no `need` budget
- **WHEN** the essential cost is computed
- **THEN** its amount SHALL be `null` with basis `unknown`

### Requirement: Essential cost basis disclosure
Every response with the essential cost SHALL include its method, basis, the number of periods used, each period's kind, label, boundaries and amounts for drill-down, the debt minimum, and `limited_history: true` when fewer than three periods were used. The interface SHALL show this basis and link each period to its ledger rows.

#### Scenario: Limited history
- **WHEN** only one completed period is available
- **THEN** the response SHALL carry `limited_history: true` and the interface SHALL label the cost as based on limited history

#### Scenario: Drill-down to a period
- **WHEN** the owner opens the periods behind the essential cost
- **THEN** each period SHALL link to the ledger filtered to its exact window, `type=expense` and `kakeibo=need`

### Requirement: Emergency coverage without invented values
Ketahanan Dana SHALL be the emergency balance divided by the essential monthly cost, with days at 30.4 days per month. Coverage months and days SHALL be `null`, with status `unknown`, whenever the essential cost is unknown or zero; the system SHALL NOT return 999 days, 99.9 months or a healthy status in that case.

#### Scenario: Unknown cost
- **WHEN** the essential cost is unknown
- **THEN** `coverage_months`, `runway_months` and `runway_days` SHALL be `null`, the status `unknown`, and the interface SHALL show that coverage cannot be calculated yet

#### Scenario: Known cost
- **WHEN** the emergency balance is Rp 4.000.000 and the essential cost Rp 2.000.000 with a 6-month target
- **THEN** coverage SHALL be 2.0 months, 61 days and status `critical`

### Requirement: Emergency balance basis
The emergency balance SHALL be the designated emergency goals, de-duplicating linked parent and child pockets. When no goal is designated, it SHALL be all liquid cash, excluding investment accounts, reported with `balance_basis: "all_liquid"`, and the interface SHALL state "Belum ada dana darurat khusus — dihitung dari seluruh saldo kas". Every coverage consumer SHALL use the same fallback and the owner's target multiplier.

#### Scenario: No designated reserve
- **GIVEN** no emergency goal, Rp 19.000.000 of liquid cash and an investment account
- **WHEN** coverage is requested from the net-worth or overview endpoint
- **THEN** the balance SHALL be Rp 19.000.000 with `balance_basis: "all_liquid"`

#### Scenario: Designated reserve
- **WHEN** a goal flagged as emergency holds Rp 4.000.000
- **THEN** the balance SHALL be Rp 4.000.000 with `balance_basis: "emergency_goals"`
