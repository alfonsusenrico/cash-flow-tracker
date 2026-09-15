## MODIFIED Requirements

### Requirement: Minimal 5-Table Persistence Baseline
The system SHALL persist core personal finance state across a clean, normalized relational schema consisting of: `users`, `accounts`, `categories`, `transactions`, `api_keys`, `goals`, and `obligations`. It SHALL NOT require legacy allocation engines, strategy rules, or background auto-funding schedulers. Transactions SHALL support optional foreign key links to `goal_id` and `obligation_id` to record savings allocations and debt repayments directly in the ledger.

#### Scenario: Clean relational schema initialization
- **WHEN** the application starts against a fresh database
- **THEN** it initializes the 7 core tables with proper foreign key cascades and indexes

## ADDED Requirements

### Requirement: Time-Series & Multi-Angle Dashboard Aggregations
The system SHALL expose aggregation endpoints for executive visual analytics:
1. `GET /api/dashboard/overview`: Returns key metrics (total liquid balance, period inflow, period outflow, savings rate, runway days), cumulative time-series points (date, cumulative inflow, cumulative outflow, net), account breakdown, and top categories.
2. `GET /api/dashboard/analytics`: Returns daily and weekly spending cadence with budget reference benchmarks, category spending variance against limits, and a day-of-week by hour/time spending density matrix.
3. `GET /api/dashboard/net-worth`: Returns total assets (liquid accounts), total liabilities (active obligations), net worth balance, and liquid runway based on the 30-day average daily burn rate.

#### Scenario: Requesting executive overview time-series
- **WHEN** an authenticated user requests `GET /api/dashboard/overview?timeframe=cycle`
- **THEN** the system returns day-by-day cumulative cash flow points ready for direct SVG/Recharts rendering along with KPI metrics

#### Scenario: Requesting spending cadence and heatmap
- **WHEN** an authenticated user requests `GET /api/dashboard/analytics?timeframe=cycle`
- **THEN** the system returns the day-by-day burn cadence bars and 7-day spending heatmap matrix
