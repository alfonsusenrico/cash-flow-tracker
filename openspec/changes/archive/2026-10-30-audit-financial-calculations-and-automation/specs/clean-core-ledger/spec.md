## REMOVED Requirements

### Requirement: Payday Clamping in Short Months
**Reason**: Confirmed salary receipts replace a fixed calendar payday.
**Migration**: Retain the old setting as historical data without inventing confirmed salary events.

## MODIFIED Requirements

### Requirement: Payday Cycle Window & Safe-to-Spend Allowance
The system SHALL use confirmed salary receipts to define the active cycle and SHALL expose observed spending and budget information through the pulse contract without predicting a future payday or daily safe-to-spend value.

#### Scenario: Required behavior and constraints
- **WHEN** the system calculates the active salary cycle
- **THEN** pulse SHALL provide today living expenses, observed cycle spending and configured budget remaining where available
- **AND** it SHALL disclose an unknown cycle when no salary receipt has been confirmed
- **AND** it SHALL NOT calculate a daily allowance from a guessed end date
- **AND** active liquid balance SHALL avoid duplicate parent and child accounts

#### Scenario: Pulse total liquid balance ignores child duplicates
- **WHEN** a user has a parent account with 10,000,000 IDR and child pockets totaling 10,000,000 IDR
- **THEN** pulse returns liquid balance of 10,000,000 IDR rather than 20,000,000 IDR

#### Scenario: Checking daily pulse with late-month payday
- **WHEN** a user previously configured payday day 29 but has no confirmed salary receipt
- **THEN** pulse SHALL disclose that the cycle is unknown rather than inventing a boundary

#### Scenario: Culture and unexpected spending reduce the allowance
- **GIVEN** today's reportable culture spending of Rp 50.000 and unexpected spending of Rp 80.000
- **WHEN** the user requests pulse
- **THEN** today living spending SHALL include Rp 130.000

#### Scenario: Saving-pillar expense is not living spending
- **GIVEN** an expense of Rp 200.000 whose effective pillar is `saving`
- **WHEN** the user requests pulse
- **THEN** today and cycle living spending SHALL NOT include it

### Requirement: Time-Series & Multi-Angle Dashboard Aggregations
The system SHALL expose aggregation endpoints for executive analytics with explicit reporting periods and shared financial semantics.

#### Scenario: Required behavior and constraints
- **WHEN** the dashboard requests financial analytics
- **THEN** overview SHALL provide observed period income, expense, cash flow, eligible liquid balance, emergency coverage when known, time-series points, account breakdown and categories
- **AND** analytics SHALL provide descriptive spending series, category variance and a spending density matrix without an invented daily allowance or linear forecast
- **AND** net worth SHALL provide assets and liabilities and retain existing emergency coverage behavior for the deferred review

#### Scenario: Requesting executive overview time-series
- **WHEN** an authenticated user requests an available reporting period
- **THEN** the system returns cumulative cash-flow points and their period context

#### Scenario: Requesting spending cadence and heatmap
- **WHEN** an authenticated user requests analytics for an available reporting period
- **THEN** the system returns descriptive spending series and heatmap data without representing an unknown end as known

### Requirement: User Profile and Account Settings Management
The system SHALL expose user identity and settings endpoints, preserve existing compatibility as specified by migration, and configure salary source and user preferences without requiring a fixed payday.

#### Scenario: Salary source setup
- **WHEN** an owner confirms a salary source
- **THEN** the source SHALL be saved without creating a ledger transaction or fixed-day boundary

#### Scenario: Updating payday to day 29
- **WHEN** an older client attempts to update a fixed payday setting
- **THEN** the server SHALL provide explicit compatibility or deprecation behavior and SHALL NOT treat the value as a confirmed salary receipt

#### Scenario: Resetting automation API key
- **WHEN** an authenticated user requests an API-key reset
- **THEN** the system generates a new Bearer API token, invalidates prior keys, and returns the new key to the authenticated requester
