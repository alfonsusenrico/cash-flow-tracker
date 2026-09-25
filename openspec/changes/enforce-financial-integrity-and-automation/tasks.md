# Tasks

## 1. Safe Implementation Baseline

- [x] 1.1 Create a topic branch from the intended target revision, record the approved scope in `PROJECT_STATE.md`, and verify `git status --short`, current branch, and baseline backend/frontend checks match the handoff before code changes.
- [ ] 1.2 Add failing database-backed regression tests for synthetic goal deposits, incorrect sale settlement, heuristic transfer pairing, insufficient funds, overselling, recurring ownership, duplicate notification delivery, and unsafe receipt upload; verify each test fails for its intended pre-change reason.

## 2. Schema and Canonical Settlement Foundation

- [x] 2.1 Make Flyway V14 compatible with clean and historical transaction schemas, add idempotent startup migrations for movement linkage/roles, account reconciliation state, and recurring occurrence records, and add a guarded V14-only checksum repair to the approved release path; verify a fresh Flyway install reaches V18, repeated migration is a no-op, and an applied-V14 schema repairs only that checksum while exactly matched pending versions remain eligible and unrelated history anomalies fail closed.
- [x] 2.2 Implement canonical account locking, derived-balance calculation, structured insufficient-funds errors, and shared atomic mutation helpers; verify unit and concurrent database tests cover ordered locks, exact-balance success, insufficient-balance rejection, and rollback.
- [x] 2.3 Implement canonical bilateral movement creation, retrieval, update, and deletion with immutable server-generated linkage; verify API tests prove exactly two linked roles and all-or-nothing edit/delete behavior.
- [x] 2.4 Update transaction list/detail contracts to expose movement identity and remove heuristic pairing assumptions; verify legacy unlinked transactions remain independent in API fixtures.

## 3. Manual Financial Operations

- [x] 3.1 Route manual internal movements through the canonical settlement service and enforce active owned distinct liquid accounts plus non-negative source balances; reject investment positions on either endpoint while preserving trade settlement as the only position-changing path; verify movement, investment-trade, and idempotency tests pass.
- [x] 3.2 Rework investment buys and sales as atomic funding-account/position settlements with positive finite inputs and oversell rejection; verify buy WAC, sell proceeds, exact-unit liquidation, oversell, and rollback tests pass.
- [x] 3.3 Replace goal-linked transaction mutation with standalone progress adjustment and linked-account-derived progress, rejecting invalid account links; verify tests prove adjustments create no transactions and linking accounts creates no financial writes.
- [x] 3.4 Enforce final-state validation for obligations, supported settings currencies, transaction targets/categories, and account default pockets; verify focused API tests cover valid clearing and invalid foreign, archived, or incoherent references.

## 4. Automation and Classification

- [x] 4.1 Harden recurring create/update validation, explicit optional-field clearing, schedule semantics, category compatibility, active-state execution, same-user references, and liquid-only generic transfer endpoints; verify recurring API tests cover each transaction and schedule type, instrument-position rejection, and cross-user rejection.
- [x] 4.2 Implement occurrence-identified recurring execution and a server-owned bounded scheduler with locking and retry-safe uniqueness; verify tests show browser-independent execution, one commit under concurrency, failure without date advancement, and bounded catch-up.
- [x] 4.3 Route recurring transfers and payroll batches through canonical movements, aggregate source requirements before payroll writes, reject investment positions on generic transfer endpoints, and preserve batch atomicity; verify insufficient-funds, instrument-position rejection, and mid-batch failure tests leave no partial records or advanced rules.
- [x] 4.4 Centralize Kakeibo movement classification by economic source/destination role and apply it to manual, recurring, payroll, trade, and ingestion paths; verify operational transfers are excluded while fresh savings and withdrawals produce correct net savings.

## 5. Ingestion, Receipts, and Authentication

- [x] 5.1 Claim and lock notification events before provisioning or ledger effects, persist the logical result, and make replay/concurrent delivery return that result; verify a concurrent integration test creates one event and one financial operation.
- [x] 5.2 Implement the settled-notification negative-balance exception and reconciliation-required account state; verify ingestion records the event once, flags the account, excludes the discrepancy from spendable cash, and reconciliation clears the flag atomically.
- [x] 5.3 Connect the active receipt endpoints to ownership-first validation, configured size/pixel limits, byte-detected supported types, safe generated paths, replacement cleanup, and transaction-delete cleanup; verify receipt tests cover success and every storage/database failure boundary.
- [x] 5.4 Apply configured client and normalized-user rate limits to login and client limits to registration with generic HTTP 429 responses and successful-login reset behavior; verify `backend/tests/test_auth_rate_limits.py` covers threshold, retry window, generic responses, and normalized-user reset behavior.

## 6. Reporting and Compatibility

- [x] 6.1 Correct Pulse and dashboard liquid, investment, aggregate, and reconciliation-pending calculations; verify fixtures with mixed account types return separately labeled exact totals.
- [x] 6.2 Update frontend API types and affected consumers for movement identity, structured financial errors, recurring occurrence status, goal adjustment, and reconciliation warnings without implementing the separate form redesign; verify frontend type-check, Vitest, and production build pass.
- [x] 6.3 Remove or quarantine obsolete backend balance/receipt paths only after confirming no active caller remains; verify `rg` finds one canonical implementation per responsibility and the backend suite remains green.

## 7. Full Verification and Handoff

- [x] 7.1 Run the complete backend suite, frontend type-check, lint, production build, `git diff --check`, strict OpenSpec validation, and local Docker migration/API smoke checks; record exact pass/fail evidence in `PROJECT_STATE.md`.
- [ ] 7.2 Review the final diff for financial atomicity, tenant isolation, secrets, migration rollback safety, unrelated changes, and requirement coverage; verify every task and scenario has evidence before presenting the change for user acceptance.
