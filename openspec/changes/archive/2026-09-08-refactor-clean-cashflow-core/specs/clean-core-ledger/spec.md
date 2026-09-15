## ADDED Requirements

### Requirement: Minimal 5-Table Persistence Baseline
The system SHALL persist all personal finance state within exactly 5 core relational tables: `users`, `accounts`, `categories`, `transactions`, and `api_keys`. It SHALL NOT require or query legacy tables such as `buckets`, `allocation_plans`, `strategy_rules`, `financial_goals`, `obligations`, or `assets`.

#### Scenario: Clean database initialization
- **WHEN** the application starts against a fresh database
- **THEN** it initializes only the 5 core tables and rejects queries against deprecated legacy tables

### Requirement: Account Management & Real Liquid Balances
The system SHALL support creating, updating, archiving, and querying liquid cash accounts (`cash`, `bank`, `wallet`). Each account's current balance SHALL equal its initial balance plus all debits (income/inbound transfers) minus all credits (expenses/outbound transfers).

#### Scenario: Querying account balances
- **WHEN** an authenticated user calls `GET /api/accounts`
- **THEN** the system returns all active accounts with their real-time calculated liquid balances

#### Scenario: Creating a new account
- **WHEN** an authenticated user posts a valid name, account type, and optional starting balance to `POST /api/accounts`
- **THEN** the system creates the account and records an opening balance entry if non-zero

### Requirement: Single-Call Transaction Recording
The system SHALL expose `POST /api/transactions` to record cash inflow (`income`) or cash outflow (`expense`). The operation SHALL require `account_id`, `amount`, and `type`, and accept optional `category_id`, `notes`, `date`, and `receipt_path`.

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
The system SHALL calculate the active spending cycle based on the user's configured payday day (e.g. 25th of month). It SHALL expose `GET /api/pulse` returning:
1. `today_spent`: Total expenses recorded today.
2. `safe_to_spend_today`: `(total_cycle_budget - cycle_spent_so_far) / max(1, remaining_cycle_days)`.
3. `cycle_remaining`: Total remaining spending budget for the current cycle.
4. `cycle_pace`: Progress percentage of the cycle days elapsed versus budget percentage consumed.

#### Scenario: Checking daily pulse on track
- **WHEN** a user requests `GET /api/pulse`
- **THEN** the system returns the daily safe-to-spend allowance, today's spending, cycle pace, and recent transaction feed in a single response

### Requirement: Unified Authentication for Web and Automation
The system SHALL authenticate requests using either browser session cookies (`ledger_session`) or Bearer API tokens (`Authorization: Bearer <key>`) through a single unified auth dependency.

#### Scenario: Browser session access
- **WHEN** a browser makes a request with a valid `ledger_session` cookie
- **THEN** the system resolves the authenticated user without requiring an API key

#### Scenario: Telegram bot API key access
- **WHEN** an automated client makes a request with a valid Bearer token
- **THEN** the system resolves the corresponding user and allows full transaction and account operations
