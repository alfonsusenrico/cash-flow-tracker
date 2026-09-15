## MODIFIED Requirements

### Requirement: Payday Cycle Window & Safe-to-Spend Allowance
The system SHALL calculate the active spending cycle based on the user's configured payday day across the entire calendar range of 1 to 31. For months with fewer days than the configured payday day (e.g. February, April, June, September, November), the cycle calculation SHALL dynamically clamp the cycle start date to the maximum days in that month (`calendar.monthrange(year, month)[1]`). It SHALL expose `GET /api/pulse` returning:
1. `today_spent`: Total expenses recorded today.
2. `safe_to_spend_today`: `(total_cycle_budget - cycle_spent_so_far) / max(1, remaining_cycle_days)`.
3. `cycle_remaining`: Total remaining spending budget for the current cycle.
4. `cycle_pace`: Progress percentage of the cycle days elapsed versus budget percentage consumed.

#### Scenario: Checking daily pulse with late-month payday
- **WHEN** a user with a configured payday of 29 requests `GET /api/pulse` in September
- **THEN** the system calculates the cycle starting from August 29 through September 28, adjusting safe daily spend accordingly

#### Scenario: Checking daily pulse on track
- **WHEN** a user requests `GET /api/pulse`
- **THEN** the system returns the daily safe-to-spend allowance, today's spending, cycle pace, and recent transaction feed in a single response

### Requirement: Account Management & Real Liquid Balances
The system SHALL support creating, updating via both `PUT` and `PATCH`, archiving, and querying liquid cash accounts (`cash`, `bank`, `wallet`). Each account's current balance SHALL equal its initial balance plus all debits (income/inbound transfers) minus all credits (expenses/outbound transfers).

#### Scenario: Updating an account with PATCH or PUT
- **WHEN** an authenticated user calls `PATCH /api/accounts/{id}` or `PUT /api/accounts/{id}` with updated account fields
- **THEN** the system updates the account and returns the updated account object with its recalculated balance

#### Scenario: Querying account balances
- **WHEN** an authenticated user calls `GET /api/accounts`
- **THEN** the system returns all active accounts with their real-time calculated liquid balances

#### Scenario: Creating a new account
- **WHEN** an authenticated user posts a valid name, account type, and optional starting balance to `POST /api/accounts`
- **THEN** the system creates the account and records an opening balance entry if non-zero

### Requirement: Single-Call Transaction Recording
The system SHALL expose `POST /api/transactions` to record cash inflow (`income`) or cash outflow (`expense`), and expose both `PUT /api/transactions/{id}` and `PATCH /api/transactions/{id}` to update transactions. The recording operation SHALL require `account_id`, `amount`, and `type`, and accept optional `category_id`, `notes`, `date`, and `receipt_path`.

#### Scenario: Updating a transaction via PUT or PATCH
- **WHEN** a user submits an update to an existing transaction via `PUT /api/transactions/{id}` or `PATCH /api/transactions/{id}`
- **THEN** the system modifies the transaction, reverses previous side-effects on goals/obligations, applies new amounts, and returns a successful response

#### Scenario: Recording an expense
- **WHEN** a user records an expense of 45,000 IDR on an account
- **THEN** the transaction is persisted, the account balance decreases by 45,000, and the category spending increases

#### Scenario: Recording an income
- **WHEN** a user records income of 5,000,000 IDR into an account
- **THEN** the transaction is persisted and the account balance increases by 5,000,000

## ADDED Requirements

### Requirement: User Profile and Account Settings Management
The system SHALL expose endpoints for querying user identity and updating settings under both `/api/*` and `/api/auth/*` (`/me`, `/settings`, `/payday`, `/api-key`, `/api-key/reset`). The settings update endpoint SHALL accept `payday_day` between 1 and 31.

#### Scenario: Updating payday to day 29
- **WHEN** an authenticated user sends `PUT /api/settings` or `PUT /api/auth/settings` with `{"payday_day": 29}`
- **THEN** the system updates the user's `payday_day` to 29 and returns the updated user record

#### Scenario: Resetting automation API key
- **WHEN** an authenticated user sends `POST /api/api-key/reset` or `POST /api/auth/api-key/reset`
- **THEN** the system generates a new Bearer API token, invalidates prior keys, and returns the plaintext key
