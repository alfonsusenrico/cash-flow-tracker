## MODIFIED Requirements

### Requirement: Primary Expense Category Flagging
The system SHALL keep the `is_primary` flag on expense categories for compatibility and SHALL use it only to derive a default Kakeibo pillar for a category without one (`need` when primary, `want` otherwise). Living expenses and runway SHALL be based on the effective Kakeibo pillar `need` as defined by `emergency-cost-estimation`, not on `is_primary`.

#### Scenario: Categorizing primary expenses
- **WHEN** an expense transaction is recorded in a category where `is_primary = true` and whose Kakeibo pillar is `want`
- **THEN** the transaction amount is not included in the essential cost, because its effective pillar is `want`

### Requirement: Real-World Ketahanan Dana Calculation
The system SHALL calculate "Ketahanan Dana" (Emergency Fund Coverage) as the ratio between the emergency fund balance and the essential monthly cost defined by `emergency-cost-estimation`, and SHALL report the balance basis and the cost basis.

#### Scenario: Required behavior and constraints
- **WHEN** the system calculates emergency fund coverage
- **THEN** the following required behavior and constraints hold:

  The system SHALL calculate "Ketahanan Dana" (Emergency Fund Coverage) as the ratio between the emergency fund balance and the essential monthly cost:
  $$\text{Ketahanan Dana} = \frac{\text{Emergency Fund Balance}}{\text{Essential Monthly Cost}}$$
  Where `Essential Monthly Cost` is the median `need` spending of up to six completed periods plus active debt minimums, with fallback to the sum of `need` category monthly budgets when no completed period exists, and unknown otherwise. The trailing 30-day primary-category sum SHALL NOT be used. When the essential cost is unknown or zero, coverage SHALL be `null` with status `unknown`.

#### Scenario: Sufficient emergency fund coverage
- **WHEN** a user has an emergency fund balance of 30,000,000 IDR and an essential monthly cost of 5,000,000 IDR
- **THEN** the system reports Ketahanan Dana as 6.0 months (6.0x monthly essential cost) with status `healthy` (Aman)

#### Scenario: Zero emergency fund balance
- **WHEN** a user has 0 IDR in flagged emergency fund accounts and a known essential cost
- **THEN** the system reports Ketahanan Dana as 0 months with status `zero`

#### Scenario: Essential cost unknown
- **WHEN** no completed period and no `need` budget exist
- **THEN** the system reports Ketahanan Dana as `null` with status `unknown` and does not report 999 days or a healthy status

### Requirement: Time-Series & Multi-Angle Dashboard Aggregations
The system SHALL expose aggregation endpoints for executive analytics with explicit reporting periods and shared financial semantics.

#### Scenario: Required behavior and constraints
- **WHEN** the dashboard requests financial analytics
- **THEN** overview SHALL provide observed period income, expense, cash flow, eligible liquid balance, emergency coverage when known with its balance and cost basis, time-series points, account breakdown and categories
- **AND** analytics SHALL provide descriptive spending series, category variance and a spending density matrix without an invented daily allowance or linear forecast
- **AND** net worth SHALL provide assets and liabilities and the emergency coverage of `emergency-cost-estimation`, using liquid cash as the no-reserve fallback and the owner's target multiplier

#### Scenario: Requesting executive overview time-series
- **WHEN** an authenticated user requests an available reporting period
- **THEN** the system returns cumulative cash-flow points and their period context

#### Scenario: Requesting spending cadence and heatmap
- **WHEN** an authenticated user requests analytics for an available reporting period
- **THEN** the system returns descriptive spending series and heatmap data without representing an unknown end as known
