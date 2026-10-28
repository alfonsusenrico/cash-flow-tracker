## MODIFIED Requirements

### Requirement: Payday Cycle Window & Safe-to-Spend Allowance
The system SHALL calculate the active spending cycle based on the user's configured payday day across the entire calendar range of 1 to 31. For months with fewer days than the configured payday day (e.g. February, April, June, September, November), the cycle calculation SHALL dynamically clamp the cycle start date to the maximum days in that month (`calendar.monthrange(year, month)[1]`). It SHALL expose `GET /api/pulse`.

Living spending for `today_spent` and the cycle covers every expense that meets both conditions:
- its category is not excluded from budget;
- its effective Kakeibo pillar (see `kakeibo-decoupling`) is `need`, `want`, `culture` or `unexpected`.

Expenses whose effective pillar is `saving` SHALL NOT count as living spending.

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

### Requirement: Unified Authentication for Web and Automation
The system SHALL authenticate requests using either browser session cookies (`ledger_session`) or Bearer API tokens (`Authorization: Bearer <key>`) through a single unified auth dependency. Bearer tokens serve automation clients such as the Android companion. No chat-bot client is deployed or supported.

#### Scenario: Browser session access
- **WHEN** a browser makes a request with a valid `ledger_session` cookie
- **THEN** the system resolves the authenticated user without requiring an API key

#### Scenario: Telegram bot API key access
- **WHEN** an automated client (now only the Android companion, since the Telegram bot is retired) makes a request with a valid Bearer token
- **THEN** the system resolves the corresponding user and allows full transaction and account operations

#### Scenario: Invalid API key is rejected
- **WHEN** a client makes a request with an unknown or revoked Bearer token
- **THEN** the system SHALL respond with 401
- **AND** it SHALL NOT resolve any user.
