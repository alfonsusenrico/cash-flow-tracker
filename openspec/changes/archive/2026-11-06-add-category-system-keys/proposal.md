## Why

The owner is taking the product global with English as the default language, and on 2026-11-06 accepted every recommended default of the language research (`cash-flow-tracker-prototypes/research/2026-11-06-language-system.md`) with "defaults ok". Default 4 of that research reads: "Add `categories.system_key` in this change and seed by language afterwards." This change is that prerequisite, split out so the money work can be reviewed on its own.

Money logic finds the system categories by their display name. Measured on `feat/markas-beranda` at `d2805cb`:
- `'Investasi'` and `'Internal Movement'` are referenced by name about 55 times in 23 backend modules (`pulse.py` 8, `transactions.py` 6 including the ledger type filters and the investment-trade default, `dashboard.py`, `review.py`, `report_figures.reportable_category_sql`, `reimbursements.NON_REPORTABLE_CATEGORY_NAMES` and its six importers in habits, habit list, capture resolution, capture suggestions, history suggestions and purpose questions, `movement_linkage`, `movement_pairing`, `merchant_memory`, `balance_check`, `topup_learning`, `waiting_rows`, `observed_insights`, `notification_application`, `notification_resolution`), plus five parser `category_hint` values resolved by name, and in 6 frontend places (`activity.tsx`, `ledgerMovements.ts` ×2, `provenance.ts`, `BudgetPanel.tsx`, `MovementReviewCards.tsx`).
- `"Penyesuaian Saldo"` is created and found by name (`accounts.ensure_adjustment_category`).

Two consequences:
- Today a rename through `PATCH /api/categories/{id}` (allowed, unprotected) turns investment buys into spending and movement legs into income in Beranda, reports and the ledger filters.
- The language change cannot seed English category names for English accounts without breaking spending, report and movement figures.

## Expected Outcome

- Renaming "Investasi" to "Investments", or "Internal Movement" to "Transfer", leaves every figure on Beranda, Transaksi, Analisis, Laporan and Rekening unchanged.
- The owner's figures are identical before and after the migration (same seed, same minute).
- No visible copy changes. The API gains one read-only field per category and per transaction row.

## Plan approval

Approved by the owner on 2026-11-06 with "ok to all": the three-change language plan with every recommended default. For this change that means it is implemented and merged first, on its own branch from `main`, before `add-language-system` and the other language change. The owner also decided on 2026-11-06 that this change updates the project's pull-request rule: after acceptance, when the owner says "open", the agent creates the pull request (`gh pr create`) and never approves or merges it.

## What Changes

- **V52 migration:** `categories.system_key` (nullable, `investment` · `internal_movement` · `balance_adjustment`), backfilled from today's exact names, with a partial unique index per owner, key and kind.
- **One owner of system-category identity** in the backend (`app/services/system_categories.py`): every name comparison above moves to the key, including the seeds, the movement and adjustment "ensure" helpers, the investment-trade default category and the parser's `category_hint` resolution.
- **API (additive):** `GET /api/categories` returns `system_key`; ledger and drawer transaction rows return `category_system_key`. The web's 6 name checks switch to it.
- **Guard test:** fails when backend or frontend code compares a category to a system name outside the seeds, migrations and notification input patterns.
- **Removed:** the unused constant `REIMBURSEMENT_CATEGORY_NAME`. Reimbursements are already identified by `offsets_spending`.
- **Behaviour change (edge):** a category the owner creates after the migration with a system name ("Investasi") is an ordinary category. Every category that existed before keeps exactly its current treatment.

## Non-goals

- No English seed names, no language or region setting, no copy change (all in `add-language-system`).
- No reimbursement or salary key: reimbursements use `offsets_spending`, salary receipts are found from evidence (`salary_receipts.py`), not from the "Gaji" name. "Gaji" appears only in the myBCA hint aliases, which `add-language-system` extends for English seed names.
- No synonym resolution for `category_name` in the public API. `idempotent-public-transactions` forbids synonym matches, and no known client posts system names.
- No protection against renaming, archiving or deleting system categories beyond today's rules.
- No change to the Android companion. It reads `/api/categories` and ignores unknown fields.
- Local verification only, on disposable PostgreSQL with synthetic data. Release and production stay with the owner through the approved GitHub workflow after acceptance.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `custom-category-management`: system categories are identified by a stable key, the key is returned by the API, and renaming keeps every figure.
- `internal-movement-api`: movement legs use, and reports exclude, the movement system category by key instead of by the name "Internal Movement".
- `account-reconciliation`: the adjustment category is found and created by key.
- `report-periods-and-drilldown`: the categories that reports exclude are named by key in "Complete Category Breakdown".

Project capabilities do not change. The change exercises the declared `database` (versioned migration, disposable-database tests), `money` (figure parity, no double counting), `api-consumers` (additive fields only) and `multi-user` (per-owner backfill and uniqueness) standards.

## Impact

- **Database (V52, `db/migrations/V52__category_system_keys.sql`):**
  - `ALTER TABLE categories ADD COLUMN system_key VARCHAR(32) NULL` with `CHECK (system_key IN ('investment','internal_movement','balance_adjustment'))`.
  - Backfill: `investment` where `name = 'Investasi'`, `internal_movement` where `name = 'Internal Movement'` (any kind, archived rows included, as today's SQL ignores kind and archival), `balance_adjustment` where `name = 'Penyesuaian Saldo'`. Exact, case-sensitive, like today's comparisons.
  - `CREATE UNIQUE INDEX uq_category_user_system_key_kind ON categories (user_id, system_key, kind) WHERE system_key IS NOT NULL`. The existing `UNIQUE (user_id, name, kind)` guarantees the backfill cannot violate it.
  - Additive and online. No row is deleted or renamed.
- **Backend:** the modules listed in Why, `db/init_db.py` (seeds carry the key), `routers/categories.py`, `routers/transactions.py` row payloads, and the new `services/system_categories.py`. New tests: migration backfill, rename-keeps-figures across pulse, overview, cycle summary, reports and ledger filters, and the guard.
- **Frontend:** `lib/api.ts` types and the 6 checks. No visible change.
- **API contract:** additive `system_key` and `category_system_key`. No field is removed or renamed.
- **Compatibility and rollback:** old code ignores the column, so rolling back is a redeploy of the previous release; the column stays harmless. Rollback risk: a rename made while the new release ran would again affect figures under the old code, so the rollback note tells the owner to rename system categories back first. Dropping the column, if ever wanted, is a later migration.
