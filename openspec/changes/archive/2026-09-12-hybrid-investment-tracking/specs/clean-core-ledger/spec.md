## ADDED Requirements

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
