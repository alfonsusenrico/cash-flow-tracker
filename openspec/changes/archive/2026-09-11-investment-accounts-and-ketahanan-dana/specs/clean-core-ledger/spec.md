# clean-core-ledger Specification Delta

## ADDED Requirements

### Requirement: Investment Account Classification
The system SHALL support creating, updating, and querying accounts classified as `investment` alongside `bank`, `wallet`, and `cash`. Investment accounts SHALL support holding liquid and portfolio funds, accepting debits and credits, participating in transfers, and having child pockets (e.g. Bibit master account with RDPU, SBN, or Saham pockets).

#### Scenario: Creating an investment account
- **WHEN** an authenticated user creates an account with `type = "investment"` and name `"Bibit"`
- **THEN** the account is persisted with type `investment` and returned in the accounts list

#### Scenario: Creating a pocket under an investment account
- **WHEN** an authenticated user creates an account with `parent_id` referencing an investment account
- **THEN** the pocket inherits or sets type `investment` and its balance is aggregated into the parent investment account

### Requirement: Emergency Fund Goal Flagging
The system SHALL support flagging one or more goals as an emergency fund via `is_emergency: boolean`. The system SHALL compute the total emergency fund balance as the aggregated balance of all active goals flagged with `is_emergency = true` and their linked accounts and pockets.

#### Scenario: Flagging a goal as emergency fund
- **WHEN** an authenticated user sets `is_emergency = true` on a goal titled `"Dana Darurat"`
- **THEN** the goal is recorded with `is_emergency = true`, and its accumulated balance is designated as the user's emergency fund balance

### Requirement: Primary Expense Category Flagging
The system SHALL support designating expense categories as primary/essential (`is_primary = true`) or secondary/discretionary (`is_primary = false`). When calculating living expenses and runway, the system SHALL isolate expenses associated with primary categories.

#### Scenario: Categorizing primary expenses
- **WHEN** an expense transaction is recorded in a category where `is_primary = true`
- **THEN** the transaction amount is included in the user's primary living expense baseline

### Requirement: Real-World Ketahanan Dana Calculation
The system SHALL calculate "Ketahanan Dana" (Emergency Fund Coverage) as the ratio between the flagged emergency fund balance and the monthly primary living expenses:
$$\text{Ketahanan Dana} = \frac{\text{Emergency Fund Balance}}{\text{Monthly Primary Expense}}$$
Where `Monthly Primary Expense` is computed from the trailing 30-day primary category expenses plus active monthly debt/obligation payments (with fallback to the sum of primary category monthly budgets if no expenses have been recorded).

#### Scenario: Sufficient emergency fund coverage
- **WHEN** a user has an emergency fund balance of 30,000,000 IDR and a monthly primary expense of 5,000,000 IDR
- **THEN** the system reports Ketahanan Dana as 6.0 months (6.0x monthly living cost) with status `healthy` (Aman)

#### Scenario: Zero emergency fund balance
- **WHEN** a user has 0 IDR in flagged emergency fund accounts
- **THEN** the system reports Ketahanan Dana as 0 months with status `zero`
