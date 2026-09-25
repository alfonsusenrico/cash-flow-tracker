# Proposal

## Why

The current ledger passes its automated suite but still permits several paths that can create money, lose sale proceeds, split bilateral movements, overdraw liquid accounts, or execute automation only when a browser happens to load. These defects undermine the balances and planning metrics that the product is meant to make trustworthy.

## What Changes

- Replace synthetic goal-deposit income with explicit goal progress behavior: account-backed goals derive progress only from linked account balances, while standalone goals accept progress adjustments without ledger transactions.
- Persist a durable bilateral movement identifier and provide atomic create, update, and delete operations for both halves of a movement.
- Settle investment buys and sales between the funding account and instrument position atomically, validate trade quantities, and reject overselling.
- Restrict investment-position balance changes to the investment trade flow; generic movements, recurring transfers, and payroll allocations SHALL use liquid accounts only.
- Prevent manual expenses, movements, payroll allocations, and recurring executions from making liquid accounts negative; settled notification ingestion remains recordable and flags the account for reconciliation.
- Run automatic recurring execution from a server-owned scheduler, make each scheduled occurrence idempotent, validate every referenced entity on create and update, and expose failed executions without advancing their schedule.
- Make payroll and recurring transfers obey the same movement and Kakeibo classification rules as manual movements.
- Make notification ingestion concurrency-safe so duplicate deliveries cannot create duplicate ledger entries.
- Harden receipt storage with ownership checks before writing, bounded uploads, content validation, safe storage names, and replacement cleanup.
- Apply the existing rate limiter to authentication endpoints and constrain persisted settings and financial relationships to supported, user-owned values.
- Correct liquid-balance reporting and close cross-field validation gaps for accounts, goals, and obligations.
- **BREAKING**: writes that would overdraw a liquid source account or sell unavailable investment units will return a validation/conflict error instead of being persisted.

## Capabilities

### New Capabilities

- `balance-integrity-and-settlement`: Defines non-negative liquid-balance enforcement, atomic settlement boundaries, reconciliation exceptions, and insufficient-funds errors.
- `secure-receipt-management`: Defines authenticated receipt upload, validation, storage, replacement, and cleanup behavior.
- `authentication-abuse-protection`: Defines rate limiting for login and registration attempts without weakening valid session or API-key flows.
- `versioned-schema-migration-compatibility`: Defines clean-install migration behavior and guarded repair for the known V14 checksum correction.

### Modified Capabilities

- `transaction-ledger-management`: Replaces heuristic transfer pairing with durable linkage and atomic bilateral mutation contracts.
- `financial-goals-tracker`: Separates standalone progress adjustments from account-backed progress and prohibits synthetic goal cash flow.
- `investment-trade-recording`: Requires correct funding-account settlement and validates positive, available trade units.
- `automated-transactions`: Makes auto-posting server-owned, occurrence-idempotent, ownership-safe, and balance-safe.
- `payroll-allocation-flow`: Applies atomic movement linkage and source-balance validation to payroll batches.
- `notification-ingestion`: Makes event-to-ledger creation concurrency-safe and records reconciliation-needed state when a settled event exceeds the tracked balance.
- `idempotent-public-transactions`: Tightens target ownership, deterministic idempotency, and name-resolution behavior.
- `kakeibo-reconciliation`: Applies consistent net-savings classification to all manual and automated movements.
- `account-reconciliation`: Defines how notification-created negative discrepancies are surfaced and cleared.
- `finance-dashboard-cockpit`: Corrects liquid-versus-total asset reporting from the Pulse API.
- `obligations-debt-tracker`: Enforces coherent total, remaining, and minimum-payment values.

## Impact

- Backend routers: transactions, movements, accounts, goals, obligations, recurring, dashboard/pulse, ingest, auth, and receipt handling.
- PostgreSQL migrations: durable movement linkage, recurring occurrence identity/status, reconciliation-needed account state, clean-schema V14 compatibility, and a guarded one-time V14 checksum repair in the approved release path.
- Frontend consumers: goal actions, ledger movement mutations, trade settlement errors, recurring/payroll status, and reconciliation warnings.
- Tests: database-backed regression coverage for atomicity, concurrency, ownership, insufficient funds, overselling, scheduling, and storage validation.
- No production access or manual schema changes; migrations remain idempotent and release-pipeline driven.
