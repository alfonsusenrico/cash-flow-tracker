## MODIFIED Requirements

### Requirement: Payday Cycle Window & Safe-to-Spend Allowance
The system SHALL calculate the active spending cycle from the user's configured payday day, across the entire calendar range of 1 to 31, and SHALL expose it through `GET /api/pulse`.

#### Scenario: Required behavior and constraints
- **WHEN** the system calculates the active payday cycle and daily allowance
- **THEN** `GET /api/pulse` SHALL return:
  1. `today_spent`: total living expenses recorded today.
  2. `safe_to_spend_today`: `(total_cycle_budget - cycle_spent_so_far) / max(1, remaining_cycle_days)`.
  3. `cycle_remaining`: total remaining spending budget for the current cycle.
  4. `cycle_pace`: progress percentage of the cycle days elapsed versus budget percentage consumed.
  5. `total_liquid_balance`: the sum of all active top-level account balances (`parent_id IS NULL`), reflecting market valuations accurately and avoiding cartesian join multiplication.

#### Scenario: Checking daily pulse with late-month payday
- **WHEN** a user with a configured payday of 29 requests `GET /api/pulse` in September
- **THEN** the system calculates the cycle starting from August 29 through September 28, adjusting safe daily spend accordingly

#### Scenario: Pulse total liquid balance ignores child duplicates
- **WHEN** a user has a parent account with 10,000,000 IDR and child pockets totaling 10,000,000 IDR
- **THEN** `GET /api/pulse` returns `total_liquid_balance` of 10,000,000 IDR rather than 20,000,000 IDR

#### Scenario: Culture and unexpected spending reduce the allowance
- **GIVEN** today's expenses of Rp 50.000 with effective pillar `culture` and Rp 80.000 with effective pillar `unexpected`, both in reportable categories
- **WHEN** the user requests `GET /api/pulse`
- **THEN** `today_spent` SHALL include Rp 130.000.

#### Scenario: Saving-pillar expense is not living spending
- **GIVEN** an expense of Rp 200.000 whose effective pillar is `saving`
- **WHEN** the user requests `GET /api/pulse`
- **THEN** `today_spent` and the cycle spending SHALL NOT include it.

## ADDED Requirements

### Requirement: Payday Clamping in Short Months
For months with fewer days than the configured payday day (e.g. February, April, June, September, November), the cycle calculation SHALL dynamically clamp the cycle start date to the maximum days in that month (`calendar.monthrange(year, month)[1]`).

#### Scenario: Payday 31 in a 30-day month
- **GIVEN** a user with a configured payday of 31
- **WHEN** the cycle that starts in April is calculated
- **THEN** it SHALL start on 30 April.

### Requirement: Living Spending Definition
Living spending for `today_spent` and the cycle SHALL cover every expense whose category is not excluded from budget and whose effective Kakeibo pillar (see `kakeibo-decoupling`) is `need`, `want`, `culture` or `unexpected`. Expenses whose effective pillar is `saving` SHALL NOT count as living spending.

#### Scenario: Budget-excluded category is not living spending
- **GIVEN** today's expense of Rp 40.000 with effective pillar `need` in a category excluded from budget
- **WHEN** the user requests `GET /api/pulse`
- **THEN** `today_spent` and the cycle spending SHALL NOT include it.
