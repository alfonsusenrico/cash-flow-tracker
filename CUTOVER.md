# Cut-over Checklist & Rollback Notes

## Pre-flight (run before any migration)

- [ ] Take a full Postgres dump: `pg_dump $DATABASE_URL > backup-$(date +%Y%m%d-%H%M%S).sql`
- [ ] Confirm Flyway baseline version matches current highest applied migration (V9)
- [ ] Run smoke tests against the current production DB to establish a baseline
- [ ] Notify any API consumers of the maintenance window (if any)

---

## Phase 0 Migrations

### V10 — Drop `accounts.opening_balance`

**What it does:** Drops the unused column. Balance is always derived from transactions.

**Rollback:**
```sql
ALTER TABLE accounts ADD COLUMN opening_balance BIGINT NOT NULL DEFAULT 0;
```
No data loss — the column was always 0 for new accounts; initial balance was recorded as a transaction.

**Risk:** Low. No application code reads this column after the backend refactor.

---

### V11 — Add `user_id UUID` to all owner tables

**What it does:** Adds `user_id` column, backfills from `users.user_id`, adds FK constraints and indexes.
The `username` columns are kept — this is a dual-write window.

**Rollback:**
```sql
ALTER TABLE accounts         DROP CONSTRAINT IF EXISTS fk_accounts_user_id;
ALTER TABLE budgets          DROP CONSTRAINT IF EXISTS fk_budgets_user_id;
ALTER TABLE payday_overrides DROP CONSTRAINT IF EXISTS fk_payday_overrides_user_id;
ALTER TABLE transaction_audit DROP CONSTRAINT IF EXISTS fk_tx_audit_user_id;
ALTER TABLE transaction_receipts DROP CONSTRAINT IF EXISTS fk_receipts_user_id;
ALTER TABLE internal_loans   DROP CONSTRAINT IF EXISTS fk_loans_user_id;
ALTER TABLE api_keys         DROP CONSTRAINT IF EXISTS fk_api_keys_user_id;

ALTER TABLE accounts         DROP COLUMN IF EXISTS user_id;
ALTER TABLE budgets          DROP COLUMN IF EXISTS user_id;
ALTER TABLE payday_overrides DROP COLUMN IF EXISTS user_id;
ALTER TABLE transaction_audit DROP COLUMN IF EXISTS user_id;
ALTER TABLE transaction_receipts DROP COLUMN IF EXISTS user_id;
ALTER TABLE internal_loans   DROP COLUMN IF EXISTS user_id;
ALTER TABLE api_keys         DROP COLUMN IF EXISTS user_id;

ALTER TABLE users DROP COLUMN IF EXISTS user_id;
```

**Risk:** Medium. The backfill is safe (one user per username). The FK constraints will fail if any row has a NULL user_id after backfill — check with:
```sql
SELECT COUNT(*) FROM accounts WHERE user_id IS NULL;
```

---

### V12 — Add `direction` generated column + transaction extensions

**What it does:** Adds `direction` (generated, stored), `notes`, `currency`, `original_amount`, `fx_rate` to `transactions`.

**Rollback:**
```sql
ALTER TABLE transactions DROP COLUMN IF EXISTS direction;
ALTER TABLE transactions DROP COLUMN IF EXISTS notes;
ALTER TABLE transactions DROP COLUMN IF EXISTS currency;
ALTER TABLE transactions DROP COLUMN IF EXISTS original_amount;
ALTER TABLE transactions DROP COLUMN IF EXISTS fx_rate;
```

**Risk:** Low. All new columns are nullable or have defaults. Existing rows are unaffected.

---

## Phase 1 Migrations

### V13 — `categories` table + default seed

**Rollback:**
```sql
DROP TABLE IF EXISTS categories CASCADE;
```
Cascades to `transactions.category_id` (V14). Safe to drop if V14 has not been applied yet.

**Risk:** Low. Additive only.

---

### V14 — `transactions.category_id`

**Rollback:**
```sql
ALTER TABLE transactions DROP COLUMN IF EXISTS category_id;
```

**Risk:** Low. Nullable column, no existing data affected.

---

### V15 — `monthly_periods` + backfill

**Rollback:**
```sql
DROP TABLE IF EXISTS monthly_periods CASCADE;
```

**Risk:** Low. Additive only. The backfill uses `INSERT ... ON CONFLICT DO NOTHING` so it is idempotent.

---

## Application Cut-over Steps

### Step 1: Deploy backend with new code (no migration yet)
- The new `services/ledger/` package is a drop-in replacement — all imports via `__init__.py` are backward-compatible.
- The new resource routers (`/categories`, `/periods`) are additive.
- The old `routers/web.py` and `routers/public.py` are unchanged.
- **Verify:** `GET /health` returns 200. Existing endpoints still work.

### Step 2: Run migrations V10–V15
```bash
docker compose run --rm migrate
```
- Flyway applies V10 → V15 in order.
- **Verify after each migration:**
  - V10: `SELECT column_name FROM information_schema.columns WHERE table_name='accounts' AND column_name='opening_balance';` → 0 rows
  - V11: `SELECT COUNT(*) FROM accounts WHERE user_id IS NULL;` → 0
  - V12: `SELECT direction FROM transactions LIMIT 1;` → 'in' or 'out'
  - V13: `SELECT COUNT(*) FROM categories;` → > 0 (seeded defaults)
  - V14: `SELECT column_name FROM information_schema.columns WHERE table_name='transactions' AND column_name='category_id';` → 1 row
  - V15: `SELECT COUNT(*) FROM monthly_periods;` → > 0 (backfilled)

### Step 3: Deploy frontend
```bash
docker compose up -d --build frontend
```
The new Next.js app is at `http://localhost:8090/`.

**Verify:** Login works, dashboard loads, categories page shows seeded categories, periods page shows backfilled periods.

### Step 4: Validate (run smoke tests)
```bash
cd backend
TEST_DATABASE_URL=postgresql://ledger:ledgerpass@localhost:5432/ledger \
  python -m pytest tests/test_smoke.py -v
```

### Step 5: Cut-over traffic
The new frontend is the only frontend. Open `http://localhost:8090/` to use it.

---

## Rollback: Full application rollback

If a critical issue is found after migration:

1. Restore the database from the pre-flight dump:
   ```bash
   psql $DATABASE_URL < backup-YYYYMMDD-HHMMSS.sql
   ```
2. Deploy the previous backend image (git revert or previous Docker tag).
3. Remove the `frontend` service from compose and restore the old nginx config.

The old `routers/web.py` and `routers/public.py` are still present in the repo — they were not deleted, only supplemented.

---

## Known Risks

| Risk | Mitigation |
|---|---|
| V11 backfill fails on large user table | Run `ANALYZE users;` before migration; backfill is a single UPDATE per table |
| `direction` generated column breaks existing queries that SELECT * | All existing queries name columns explicitly; no SELECT * in the codebase |
| Next.js build fails in Docker | Run `npm run build` locally first; check node version (requires Node 22) |
| Legacy SPA session cookie conflicts with new frontend | Both use the same `ledger_session` cookie on the same domain — no conflict |
| categories seed inserts duplicate names | `ON CONFLICT DO NOTHING` makes it idempotent |
