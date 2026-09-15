# clean-core-ledger Specification

## Purpose
TBD - created by archiving change refactor-clean-cashflow-core. Update Purpose after archive.
## Requirements
### Requirement: Minimal 5-Table Persistence Baseline
The system SHALL persist core personal finance state across a clean, normalized relational schema consisting of: `users`, `accounts`, `categories`, `transactions`, `api_keys`, `goals`, and `obligations`. It SHALL NOT require legacy allocation engines, strategy rules, or background auto-funding schedulers. Transactions SHALL support optional foreign key links to `goal_id` and `obligation_id` to record savings allocations and debt repayments directly in the ledger.

#### Scenario: Clean relational schema initialization
- **WHEN** the application starts against a fresh database
- **THEN** it initializes the 7 core tables with proper foreign key cascades and indexes

### Requirement: Account Management & Real Liquid Balances
The system SHALL support creating, updating via both `PUT` and `PATCH`, archiving, and querying liquid cash and investment accounts (`cash`, `bank`, `wallet`, `investment`):
1. Each account's current balance SHALL equal its initial balance plus all debits (income/inbound transfers) minus all credits (expenses/outbound transfers), or its market valuation when instrument tracking is enabled.
2. Investment valuation updates (via market data or manual updates) SHALL update market valuation parameters (`last_price`, `units`, `initial_balance`) directly and SHALL NOT insert income, expense, or transfer transactions into the ledger.
3. Account entities SHALL support a customizable tag color (`color`, hex string default `#3b82f6`) and display position index (`display_order`, integer default `0`).
4. `GET /api/accounts` SHALL order top-level accounts by `display_order ASC, created_at ASC, name ASC`.
5. The system SHALL expose `POST /api/accounts/reorder` accepting `account_ids: list[UUID]` to persist updated card order.
6. In the user interface, account cards SHALL prominently display the account name as a large, tactile tag badge using the account's configured color with high-contrast text, support draggable reordering arranged in natural visual reading order (left-to-right, top-to-bottom) without printing numerical index numbers, and adapt to light mode with a rich, soft slate canvas tone and crisp structural contrast.

#### Scenario: Updating investment valuation without ledger transactions
- **WHEN** a user updates the market value of an investment account from 10,000,000 IDR to 11,500,000 IDR
- **THEN** the account's balance updates to 11,500,000 IDR without creating any income or expense transactions in the transaction history

#### Scenario: Creating an account with custom tag color
- **WHEN** an authenticated user creates an account with name "BCA Prioritas", type "bank", and color "#059669"
- **THEN** the system persists the account with the chosen color and renders the account name tag in emerald with high-contrast text

#### Scenario: Reordering accounts via drag-and-drop in visual order
- **WHEN** an authenticated user drags an account card to a new position in the grid
- **THEN** the UI rearranges the cards in natural visual sequence (left-to-right, top-to-bottom) and sends the updated ID order to `POST /api/accounts/reorder`

#### Scenario: Viewing accounts in rich light mode
- **WHEN** a user switches the application to light mode
- **THEN** the canvas displays a refined soft slate background (`#F4F5F8`), account cards render with crisp white surfaces and visible borders, and cards avoid unstyled dark inversions

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

### Requirement: Account-to-Account Transfers
The system SHALL record transfers between two owned accounts as an atomic movement. Transfers SHALL NOT be counted as income or expense in cashflow totals or category budgets.

#### Scenario: Moving funds between accounts
- **WHEN** a user moves 500,000 IDR from Bank Account A to E-Wallet Account B
- **THEN** Account A balance decreases by 500,000, Account B balance increases by 500,000, and net monthly spending remains unchanged

### Requirement: Payday Cycle Window & Safe-to-Spend Allowance
The system SHALL calculate the active spending cycle based on the user's configured payday day across the entire calendar range of 1 to 31. For months with fewer days than the configured payday day (e.g. February, April, June, September, November), the cycle calculation SHALL dynamically clamp the cycle start date to the maximum days in that month (`calendar.monthrange(year, month)[1]`). It SHALL expose `GET /api/pulse` returning:
1. `today_spent`: Total expenses recorded today.
2. `safe_to_spend_today`: `(total_cycle_budget - cycle_spent_so_far) / max(1, remaining_cycle_days)`.
3. `cycle_remaining`: Total remaining spending budget for the current cycle.
4. `cycle_pace`: Progress percentage of the cycle days elapsed versus budget percentage consumed.
5. `total_liquid_balance`: The sum of all active top-level account balances (`parent_id IS NULL`), accurately reflecting market valuations and avoiding cartesian join multiplication.

#### Scenario: Checking daily pulse with late-month payday
- **WHEN** a user with a configured payday of 29 requests `GET /api/pulse` in September
- **THEN** the system calculates the cycle starting from August 29 through September 28, adjusting safe daily spend accordingly

#### Scenario: Pulse total liquid balance ignores child duplicates
- **WHEN** a user has a parent account with 10,000,000 IDR and child pockets totaling 10,000,000 IDR
- **THEN** `GET /api/pulse` returns `total_liquid_balance` of 10,000,000 IDR rather than 20,000,000 IDR

### Requirement: Unified Authentication for Web and Automation
The system SHALL authenticate requests using either browser session cookies (`ledger_session`) or Bearer API tokens (`Authorization: Bearer <key>`) through a single unified auth dependency.

#### Scenario: Browser session access
- **WHEN** a browser makes a request with a valid `ledger_session` cookie
- **THEN** the system resolves the authenticated user without requiring an API key

#### Scenario: Telegram bot API key access
- **WHEN** an automated client makes a request with a valid Bearer token
- **THEN** the system resolves the corresponding user and allows full transaction and account operations

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

### Requirement: User Profile and Account Settings Management
The system SHALL expose endpoints for querying user identity and updating settings under both `/api/*` and `/api/auth/*` (`/me`, `/settings`, `/payday`, `/api-key`, `/api-key/reset`). The settings update endpoint SHALL accept `payday_day` between 1 and 31.

#### Scenario: Updating payday to day 29
- **WHEN** an authenticated user sends `PUT /api/settings` or `PUT /api/auth/settings` with `{"payday_day": 29}`
- **THEN** the system updates the user's `payday_day` to 29 and returns the updated user record

#### Scenario: Resetting automation API key
- **WHEN** an authenticated user sends `POST /api/api-key/reset` or `POST /api/auth/api-key/reset`
- **THEN** the system generates a new Bearer API token, invalidates prior keys, and returns the plaintext key

### Requirement: Hierarchical Accounts and Child Pockets
The system SHALL support organizing accounts into a clean two-level hierarchy (Parent Account -> Child Pockets / Sub-accounts) using a self-referential `parent_id`:
1. An account with `parent_id IS NULL` that has no children SHALL act as a standalone account (e.g. Cash, GoPay).
2. An account with `parent_id IS NULL` that has children SHALL act as a Master/Parent account (e.g. Bank Jago, BCA, Bibit).
3. An account with `parent_id IS NOT NULL` SHALL act as a child pocket belonging to that parent account.
4. Child pockets SHALL NOT have sub-pockets (enforcing a strict two-level depth).
5. Deleting or archiving a parent account SHALL cascade cleanly to its child pockets.

#### Scenario: Creating a child pocket under a bank account
- **WHEN** an authenticated user creates an account "Kantong Liburan" with `parent_id` pointing to "Bank Jago"
- **THEN** the system persists "Kantong Liburan" as a child pocket of "Bank Jago"

#### Scenario: Enforcing two-level hierarchy limit
- **WHEN** a user attempts to create a child account whose parent is already a child pocket
- **THEN** the system rejects the creation with a 400 Bad Request error

### Requirement: Parent Balance Aggregation and Grouped Representation
The system SHALL compute the balance of any master account dynamically as the sum of all its active child pockets' balances plus any direct balance of the master account:
1. `GET /api/accounts` SHALL return accounts with `parent_id`, `is_parent`, `children: [...]`, and aggregated total balance for master accounts.
2. Transactions recorded against a child pocket SHALL directly debit or credit that specific pocket and automatically reflect in the master account's aggregated balance.
3. Account selectors across Quick Capture, Ledger, and Transfers SHALL present pockets grouped under their respective master accounts using `<optgroup>`.
4. The Accounts page SHALL render master accounts with expandable/collapsible pocket lists and clear visual hierarchy.

#### Scenario: Querying accounts with pockets
- **WHEN** a user with Bank Jago having "Kantong Utama" (Rp 5.000.000) and "Kantong Darurat" (Rp 15.000.000) calls `GET /api/accounts`
- **THEN** Bank Jago reports a total aggregated balance of Rp 20.000.000 with both child pockets nested under it

#### Scenario: Recording expense from a specific pocket
- **WHEN** a user records an expense of Rp 50.000 from "Kantong Utama"
- **THEN** "Kantong Utama" balance decreases by Rp 50.000 and the parent "Bank Jago" aggregated balance decreases by Rp 50.000

### Requirement: Inter-Pocket Transfers
The system SHALL support atomic transfers between pockets belonging to the same parent account or across different parent accounts using the standard transfer mechanism without affecting cashflow spending metrics.

#### Scenario: Transferring money between pockets of the same bank
- **WHEN** a user transfers Rp 1.000.000 from "Kantong Utama" to "Kantong Darurat"
- **THEN** "Kantong Utama" decreases by Rp 1.000.000, "Kantong Darurat" increases by Rp 1.000.000, and Bank Jago total balance remains unchanged

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

### Requirement: Investment Instrument Classification
The system SHALL support classifying investment accounts and pockets by instrument type (`stock`, `mutual_fund`, `gold`, `crypto`, `deposit`, `other`). Standalone accounts or parent accounts classified as `investment`, as well as child pockets under an investment account, SHALL accept an optional `instrument_type` and optional `instrument_symbol`.

#### Scenario: Creating a stock pocket under an investment account
- **WHEN** an authenticated user creates an account or pocket with `type = "investment"`, `instrument_type = "stock"`, and `instrument_symbol = "BBCA.JK"`
- **THEN** the system persists the account with its instrument classification and symbol

#### Scenario: Defaulting instrument type for non-investment accounts
- **WHEN** an account is created with type `bank`, `wallet`, or `cash` without instrument attributes
- **THEN** `instrument_type` and `instrument_symbol` default to `NULL`

### Requirement: Dynamic Ticker Search API
The system SHALL expose `GET /api/accounts/instruments/search?q={query}` to provide lazy-fetch, debounced symbol lookups. The endpoint SHALL query public market data (Yahoo Finance) with no required API key and return matching assets with `symbol`, `name`, `type`, and `exchange`.

#### Scenario: Searching for Indonesian bank stock
- **WHEN** an authenticated user sends `GET /api/accounts/instruments/search?q=BBC`
- **THEN** the response includes `BBCA.JK` with name `"PT Bank Central Asia Tbk"` and exchange `"JKT"`

#### Scenario: Searching for precious metals
- **WHEN** an authenticated user sends `GET /api/accounts/instruments/search?q=Gold`
- **THEN** the response includes `GC=F` with name `"Gold"`

### Requirement: Unit & Cost Tracking with Unified Capital Gain Computation
The system SHALL store units/quantity (`units`, numeric with up to 6 decimal places), average buy price (`avg_buy_price`, bigint IDR), and cached market price (`last_price`, bigint IDR) for investment holdings. The system SHALL calculate and expose `capital_gain` and `capital_gain_pct` on account and pocket objects:
1. For ticker-tracked holdings with units and average buy price:
   $$\text{capital\_gain} = (\text{units} \times \text{last\_price}) - (\text{units} \times \text{avg\_buy\_price})$$
   $$\text{capital\_gain\_pct} = \frac{\text{last\_price} - \text{avg\_buy\_price}}{\text{avg\_buy\_price}} \times 100$$
2. For manual untracked holdings where units or market tickers are absent:
   $$\text{capital\_gain} = \text{current\_balance} - \text{initial\_cost}$$
3. The field SHALL be labeled "Capital Gain" regardless of whether the value is positive (gain) or negative (loss).

#### Scenario: Positive capital gain on shares
- **WHEN** a user holds 1,000 units of `BBCA.JK` bought at 6,000 IDR each and the current price is 6,325 IDR
- **THEN** the system computes `capital_gain` as +325,000 IDR (+5.42%) and updates the current liquid balance to 6,325,000 IDR

#### Scenario: Negative capital gain (capital loss)
- **WHEN** a user holds 10 units bought at 100,000 IDR each and the current price drops to 90,000 IDR
- **THEN** the system computes `capital_gain` as -100,000 IDR (-10.0%) and updates the current liquid balance to 900,000 IDR

### Requirement: Automated and On-Demand Market Price Synchronization
The system SHALL expose `POST /api/accounts/sync-prices` to trigger an immediate price update for all active investment accounts having a configured `instrument_symbol`.
The system SHALL also run a daily scheduled background task to fetch closing market prices, update `last_price` and `last_price_at`, and adjust account balances accordingly.

#### Scenario: Manual on-demand price synchronization
- **WHEN** an authenticated user sends `POST /api/accounts/sync-prices`
- **THEN** the system queries current quotes for all distinct tracked symbols, updates `last_price` and `last_price_at` on matching accounts, and returns the count of updated accounts

#### Scenario: Graceful handling of offline or unquoted tickers
- **WHEN** a ticker lookup fails or market data is temporarily unreachable
- **THEN** the system retains the existing `last_price` without corrupting account balances and logs a warning

### Requirement: Manual Valuation Adjustment for Untracked Assets
For investment accounts or pockets without dynamic tickers (e.g. Indonesian Money Market Funds / RDPU or time deposits), the system SHALL allow recording updated portfolio valuations. The update SHALL recalculate Capital Gain against the net invested cost or initial balance without requiring automated ticker symbols.

#### Scenario: Updating valuation on a mutual fund pocket
- **WHEN** an authenticated user updates the balance of "Bibit RDPU" from 10,000,000 IDR to 10,450,000 IDR with initial cost 10,000,000 IDR
- **THEN** the system updates the pocket balance to 10,450,000 IDR and reports `capital_gain` as +450,000 IDR (+4.5%)

