## Environment

Local only, 2026-10-28. Disposable `postgres:16` container on `127.0.0.1:55508`, migrated by
`flyway/flyway:10`. Backend venv of the owner checkout (no dependency change). Synthetic data only.

## Schema evidence

- Baseline (phase 7 base, V1→V31, with startup block): 904 backend tests passed.
- Flyway V1→V31 versus the same database after `init_db_schema()`: `pg_dump --schema-only` differs only by
  `accounts.default_funding_account_id` (+ FK `accounts_default_funding_account_id_fkey`) and
  `uq_tx_user_idempotency`.
- Fresh Flyway V1→V32 versus the old V31 + startup dump: differs only by the removed
  `merchant_category_rules` table, its primary key, unique constraint, index and two foreign keys.

## Backend

Freshly recreated database, Flyway "Successfully applied 32 migrations … now at version v32", then
`PYTHONPATH=backend TEST_DATABASE_URL=… SKIP_TEST_MIGRATIONS=1 pytest backend/tests -q -rs`:
**901 passed, 0 skipped, exit 0**.

Count reconciliation: 904 − 6 (`test_category_rules.py`) − 1 (mirror-equality test in
`test_notification_schema.py`) + 4 (`test_schema_ownership.py`) = 901.

## Smoke boot

Separate database `ledger_smoke`, Flyway V1→V32 only; `backend/app/db` contains only `__init__.py`,
`init_db.py`, `pool.py`. `uvicorn app.main:app --host 127.0.0.1 --port 58508` with development settings
and AI disabled:

- `GET /api/health` → 200
- `POST /api/auth/register` (fictional user) → 200
- `GET /api/categories` with the session → 200 (14 categories); `GET /api/accounts` → 200
- database: both "Internal Movement" rows `is_excluded_from_budget = t`, `kakeibo_type` NULL
- server log: request lines and clean shutdown only, no warning or traceback. Server stopped afterwards.

## Frontend

`npm ci` exit 0; `npm test` **44 files, 210 tests passed** (baseline 205 + 5 in `Icon.test.tsx`), exit 0;
`npm run lint` exit 0; `npm run type-check` exit 0; `npm run build` exit 0 ("Compiled successfully").
No browser check: the visible change is four icon glyphs and two picker options, covered by unit tests.

## Repository checks

- `git diff --check` and `git diff HEAD --check`: exit 0.
- `openspec validate remove-dead-code-and-duplicate-schema --strict`: valid, exit 0.
- Legacy archive: ten `openspec archive <name> --yes` runs, each exit 0; `openspec validate --all` then
  reported only this change (no deltas yet), which was expected at that point.
- After `openspec archive remove-dead-code-and-duplicate-schema --yes` (exit 0, "+ 66, ~ 19") and writing the
  purpose of the new `schema-migrations` spec: `openspec validate --all` **58 passed, 0 failed, exit 0**;
  `openspec validate --all --strict` **58 passed, 0 failed, exit 0**.
- Split check: every split main spec kept all of its previous scenarios (scenario count before + new
  scenarios of the added requirements); `transaction-provenance` changed only its backfill scenario line.
