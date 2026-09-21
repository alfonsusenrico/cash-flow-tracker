# Design: Auto-Archive Paid-Off Debts and Obligations

## Context

See `proposal.md` for motivation.
Currently, debts and obligations track `remaining_amount` and `is_archived` independently. When payments are recorded via `POST /api/transactions` or recurring rules, `remaining_amount` is decremented down to 0, but `is_archived` remains `false`.
Furthermore, queries in `obligations.py` and `dashboard.py` check only `is_archived = false`, which allows 0-balance debts to continue showing up on dashboard cards, the `/goals` active debts view, and in monthly commitment tallies.

## Goals / Non-Goals

**Goals:**
- Guarantee that whenever an obligation's `remaining_amount` reaches 0 (via transaction payment, recurring execution, or direct update), its `is_archived` flag is updated to `true`.
- Guarantee that if a payoff transaction is deleted or updated such that `remaining_amount > 0`, the obligation is automatically reactivated (`is_archived = false`).
- Apply defense-in-depth filtering across `obligations.py` and `dashboard.py` so that active debt listings strictly exclude rows where `remaining_amount <= 0` or `is_archived = true`.
- Run an idempotent startup data migration in `init_db.py` to auto-archive all legacy or existing debts that have zero balance.

**Non-Goals:**
- Creating a separate debt archive table or soft-delete audit log.
- Changing frontend component UI hierarchies or modifying API response contracts.
- Altering investment asset tracking or non-debt features.

## Decisions

### Decision 1: Atomic SQL Amortization & Archival State Transition
- **Rationale:** Debt payments can be made concurrently or via background tasks. Updating `remaining_amount` and `is_archived` in the same SQL statement ensures ACID atomicity without race conditions:
  ```sql
  UPDATE obligations
  SET remaining_amount = GREATEST(0, remaining_amount - %s),
      is_archived = CASE WHEN (remaining_amount - %s) <= 0 THEN true ELSE is_archived END,
      updated_at = NOW()
  WHERE id = %s AND user_id = %s
  ```
- **Alternatives Considered:**
  - *Read-modify-write in Python*: Prone to race conditions and requires additional round trips.
  - *PostgreSQL Trigger*: While clean in DB, hidden business logic triggers bypass application unit test mocks and reduce code transparency.

### Decision 2: Automatic Reactivation on Payment Reversal
- **Rationale:** When a user deletes a mistake payment or updates its amount, the obligation's `remaining_amount` increases. If it was previously archived because of that payment, it must automatically un-archive (`is_archived = false`) so the user can see their debt again:
  ```sql
  UPDATE obligations
  SET remaining_amount = LEAST(total_amount, remaining_amount + %s),
      is_archived = CASE WHEN (remaining_amount + %s) > 0 THEN false ELSE is_archived END,
      updated_at = NOW()
  WHERE id = %s AND user_id = %s
  ```
- **Alternatives Considered:**
  - *Leave archived and require manual unarchive*: Bad user experience; deleting the payment would leave the debt hidden while restoring the balance.

### Decision 3: Defense-in-Depth Query Filtering
- **Rationale:** Even with write-path consistency, queries calculating active debt liabilities (`GET /api/obligations?include_archived=false`, `monthly_commitments`, `obligations_glance`, `net_worth`) should explicitly include `AND is_archived = false AND remaining_amount > 0`. This guarantees 0-balance debts never pollute active metrics even during transient edge cases.
- **Alternatives Considered:**
  - *Relying solely on `is_archived = false`*: Vulnerable if any legacy row or external modification didn't set `is_archived`.

### Decision 4: Idempotent Startup Migration
- **Rationale:** In `backend/app/db/init_db.py`, append an idempotent statement executed on backend boot:
  ```sql
  UPDATE obligations
  SET is_archived = true, updated_at = NOW()
  WHERE remaining_amount <= 0 AND is_archived = false;
  ```
  This immediately cleans up the user's existing paid debts in production and dev without manual database intervention.

## Risks / Trade-offs

- **Risk: User intentionally edits remaining_amount to 0 on an active card** → Mitigation: `update_obligation` checks `if remaining_amount == 0: is_archived = True`. If user later increases remaining amount, auto-set `is_archived = False`.
- **Risk: Partial payments overpaying debt** → Mitigation: `GREATEST(0, remaining_amount - payment)` clamps at 0 and sets `is_archived = true`.

## Migration Plan

1. The SQL migration runs automatically in `init_db.py` on container startup.
2. Zero downtime; backwards-compatible with all existing endpoints and frontend components.
