# Design

## Context

See `proposal.md` for motivation. The active FastAPI routers write directly through PostgreSQL cursors, account balances are derived from initial balance plus ledger rows, bilateral movements are currently represented by unrelated expense and income rows, and startup schema evolution is managed in `backend/app/db/init_db.py`. Existing helpers for rate limiting and receipt preparation are present but disconnected from active routes. The design must preserve current IDs and historical ledger data, remain safe with multiple API workers, and avoid production-only operations.

## Goals / Non-Goals

**Goals:**

- Make every balance- or position-changing operation use one canonical transaction boundary.
- Make concurrency correctness enforceable by database identity and constraints rather than timing assumptions.
- Preserve readable API errors and existing historical records during migration.
- Reuse the current PostgreSQL, FastAPI, receipt, and rate-limit infrastructure without adding a new financial framework.

**Non-Goals:**

- Introduce full double-entry accounting, multi-currency settlement, overdraft products, or credit-card accounts.
- Infer links between legacy transfer rows whose relationship is not provable.
- Repair historical user balances automatically or connect to production during implementation.
- Redesign the forms covered by the companion UI change.

## Decisions

### 1. Canonical ledger mutation service

Move balance-affecting operations behind a small backend service layer that receives an existing database connection and never commits independently. It will own source resolution, ordered account-row locking, balance calculation, insufficient-funds validation, bilateral movement creation, and effect reversal. Transactions, movements, trades, recurring execution, payroll, reconciliation, and ingestion will call this service.

Ordered locking by account UUID prevents deadlocks when concurrent operations touch the same accounts. Derived balances are recalculated after locks are acquired. This is preferred over cached mutable balance columns, which would introduce another synchronization source of truth.

### 2. Durable bilateral movement identity

Add nullable `movement_id UUID` and `movement_role` (`outbound` or `inbound`) to transactions, with a unique constraint on `(movement_id, movement_role)` when non-null. Canonical movement creation generates one ID and inserts exactly one row for each role. Server endpoints update and delete by movement ID while validating ownership.

Legacy rows remain `movement_id = NULL`. The frontend must show them independently rather than guess pairs. A heuristic backfill was rejected because equal amounts and nearby timestamps cannot prove identity.

### 3. Non-negative policy at locked source accounts

Manual expenses, movements, buys, recurring executions, and payroll batches validate the resolved effective liquid account after locking. Insufficient funds return HTTP 409 with a stable error code plus required and available amounts. No partial effects are committed.

Notification ingestion is the only negative-balance exception because it records a settled external fact. Accounts gain `reconciliation_required`, `reconciliation_reason`, and `reconciliation_event_id` fields. Negative amounts are excluded from spendable-liquid calculations until reconciliation clears the state. Credit products remain obligations rather than negative accounts.

### 4. Goal progress has one source of truth per goal

For a goal with linked accounts, query-time non-redundant linked balances are authoritative; linking accounts performs no financial write. For an unlinked goal, a dedicated progress-adjustment endpoint locks the goal and changes `current_amount` within valid bounds without creating a transaction. Transaction create/update/delete stops mutating goal progress. Existing transaction `goal_id` values remain historical metadata.

This avoids a separate contribution table because the user chose current progress rather than a standalone contribution history. If audit history becomes required later, it can be introduced as its own capability.

### 5. Trades settle through canonical movements

Buy and sell requests validate account ownership, account roles, positive finite quantity and price, and position availability while holding locks. A buy creates a funding-to-position movement and updates units and weighted average price. A sell creates a position-to-funding movement and decreases units; average cost remains unchanged. Cash and units commit together.

Investment positions are trade-only balance destinations/sources. Generic movements, recurring transfer rules, and payroll allocations SHALL reject accounts whose account type or instrument metadata identifies an investment position. The trade endpoint is the only path allowed to create a movement involving a position, and it must carry the units and price required to update the position consistently. Investment funding accounts remain ordinary liquid accounts and may be used as movement sources or destinations. Recurring investment trades are not supported by this change; they require a future dedicated trade-rule contract.

The existing single income/expense representation was rejected because it cannot represent both cash destination and position mutation correctly.

### 6. Recurring occurrence records and server scheduler

Add `recurring_executions` keyed uniquely by `(recurring_rule_id, scheduled_for)` with status, error code, timestamps, and resulting movement/transaction references. A lifespan-owned periodic scheduler selects due rules with row locking and `SKIP LOCKED`; the unique occurrence key makes multiple workers and retries safe. The browser may refresh status but no longer initiates auto-posting.

Success advances `next_due_date`; failure records an actionable occurrence state without financial writes or schedule advancement. Manual execution uses the same occurrence service and rejects inactive rules. Catch-up is bounded per scheduler pass so long downtime cannot monopolize a worker.

### 7. Notification event claim precedes effects

Ingestion first inserts or claims `(user_id, payload_hash)`, locks that row, and checks its existing result before provisioning pockets, mutating positions, or creating ledger records. Only the claimant applies effects and attaches the resulting transaction or movement identity. Replays return the stored result.

### 8. Active receipt endpoint adopts the hardened receipt pipeline

The route verifies transaction ownership before reading and storing the complete payload, applies configured size and decoded-pixel limits, detects type from bytes, normalizes images or compresses PDFs, and stores generated paths. Database metadata and file replacement are coordinated so a failed database change removes the new file and a successful replacement removes the old file afterward. Transaction deletion performs best-effort owned-file cleanup with logged failures.

### 9. Boundary validation and abuse protection

Recurring updates validate all final references and use field-presence information to distinguish omission from clearing. Goal links reject the entire request on an invalid account. Obligation validation operates on the final merged state. Settings use an explicit supported-currency enum. Public name resolution rejects ambiguity instead of provisioning from typos.

Login and registration routes apply the existing rate limiter using normalized client and username keys. Responses remain generic. Successful authentication clears only the user-scoped failure key, not broader client protection.

### 10. Clean-install migration compatibility and guarded checksum correction

The Flyway V14 transfer conversion currently assumes `transactions.idempotency_key` exists, while a clean V1–V13 schema does not create it until V15. V14 will idempotently add the nullable column before converting legacy transfers; V15 remains responsible for enforcing the intended width. Existing schemas that already have the column are unchanged, and the transfer conversion retains its current idempotency behavior.

Changing V14 intentionally changes its checksum for databases that already applied it. The approved release script will run a preflight before normal migration: inspect Flyway history, validate, and permit repair only for the applied V14 checksum mismatch with no failed, missing, deleted, or out-of-order history. Flyway may also report not-yet-applied versions as validation issues; the preflight accepts each such entry only when its version and description exactly match a `Pending` versioned entry in the inspected history. It then runs Flyway repair, requires the repair result to align V14 only, validates again with no remaining unexpected issue beyond those same verified pending versions, and only then proceeds with normal migrations. If any precondition or postcondition differs, deployment stops before migration and service replacement. The routine is not part of API startup and does not run when V14 needs no repair.

## Risks / Trade-offs

- **[Legacy movement rows remain unconsolidated]** → Preserve truth and provide explicit unpaired presentation instead of risky inferred linkage.
- **[Derived balance checks can be expensive]** → Lock only affected accounts, reuse indexed transaction aggregation, and measure query plans before considering cached balances.
- **[Scheduler runs in multiple API processes]** → Use row locks plus a unique occurrence constraint; correctness does not depend on one process.
- **[Filesystem and database cannot share a transaction]** → Use verify-first writes, deterministic cleanup order, and regression tests for each failure boundary.
- **[Notification exception can display a negative tracked balance]** → Mark it as reconciliation-required and exclude it from spendable balance rather than hiding the settled event.
- **[Removing transaction-driven goal mutation changes legacy expectations]** → Retain historical `goal_id` display and document the new standalone-versus-linked progress model in the UI change.
- **[Flyway repair can alter more than one history entry]** → Run it only after exact V14 mismatch and clean-history preconditions, ignoring only versioned migrations independently verified as pending; require its JSON result to align only V14 and revalidate before the release continues.
- **[V14 correction changes checksums on databases that already applied it]** → Repair only the verified V14 checksum record once through the approved release script; never run repair unconditionally or against history with any other anomaly.

## Migration Plan

1. Make V14 compatible with clean and historical schemas, then have the approved release script perform a guarded one-time checksum repair only for databases with the expected V14 mismatch and no unrelated anomaly. Verify that any not-yet-applied validation entries exactly match pending Flyway history. Validate both clean history and the pre-existing V14 path.
2. Add nullable movement linkage, reconciliation state, and recurring execution structures through an idempotent startup migration with indexes and constraints.
3. Leave legacy transfers unlinked; do not attempt heuristic backfill.
4. Deploy canonical mutation and validation services, then route manual movements and trades through them.
5. Route recurring, payroll, and ingestion paths through the same services and enable the server scheduler only after occurrence uniqueness is active.
6. Switch goal progress semantics and receipt/auth boundary behavior after their regression tests pass.
7. Verify backend tests, migration idempotency, concurrency cases, frontend contract checks, and local Docker smoke behavior before release.

Rollback disables the scheduler first, then reverts application routing while leaving additive nullable columns and occurrence history in place. Schema removal is deferred until a later verified cleanup because dropping financial history is not a safe rollback step.
