## Context

Three kinds of category have system meaning:
- **Investment** ("Investasi", seeded as an expense): investment buys and top-ups are not spending. It carries no `is_excluded_from_budget` flag, so the name is the only thing that keeps it out of spending.
- **Movement** ("Internal Movement", seeded once per kind): movement legs are neither spending nor income. They carry `is_excluded_from_budget = true`, and the name is also checked directly (pairing, review, merchant memory, notification resolution, ledger filters).
- **Balance adjustment** ("Penyesuaian Saldo", created on demand per kind by `ensure_adjustment_category`): excluded from budget by flag. The name is used to find or create it.

Reimbursement ("Patungan & Ganti Uang") is identified by `offsets_spending`. `REIMBURSEMENT_CATEGORY_NAME` is defined but never read. Salary receipts are found from evidence and reportable income, not from "Gaji".

Today's name checks are exact and case-sensitive in SQL (`c.name NOT IN ('Internal Movement', 'Investasi')`) and exact in Python (`row["name"] == "Internal Movement"`). Most ignore the category's kind and archival.

The language change (`add-language-system`) needs English seed names for English accounts. Under today's code an English "Investments" category would count as spending.

## Goals / Non-Goals

**Goals:**
- One stable identity per system category that does not depend on the display name.
- Every existing figure identical before and after the migration.
- Renaming a system category changes nothing but its name.

**Non-Goals:**
- Seeding by language, copy, or any visible change.
- Protecting system categories from archiving or deletion.
- Reworking the movement or investment rules themselves.

## Decisions

### D1. A `system_key` column on `categories`

`categories.system_key VARCHAR(32) NULL`, one of `investment`, `internal_movement`, `balance_adjustment`, enforced by a CHECK constraint. Ordinary categories have `NULL`.

Rejected:
- One boolean per meaning (`is_investment`, `is_movement`): more columns for the same fact, and nothing stops a row from being both.
- A separate `system_categories` table: a join on every report query for a one-to-one fact.
- Fixed category UUIDs: categories are per owner, so the UUIDs differ per owner anyway.

### D2. The key set is exactly the three names logic reads

`investment`, `internal_movement` and `balance_adjustment`. The research suggested `reimbursement` and `salary` too.

Rejected:
- `reimbursement`: `offsets_spending` already identifies it, and no logic reads its name. The unused constant is removed.
- `salary`: salary receipts are found from evidence, and "Gaji" appears only in the myBCA hint aliases (`notification_application.BANK_CATEGORY_HINT_ALIASES`), a classification aid. `add-language-system` adds the English seed names to those aliases.

### D3. Backfill reproduces today's predicate exactly

The migration sets:
- `internal_movement` on every category named exactly `Internal Movement`;
- `investment` on every category named exactly `Investasi`;
- `balance_adjustment` on every category named exactly `Penyesuaian Saldo`.

It covers every kind and archived rows, because today's checks ignore both. So for every existing row, "name is a system name" equals "has the system key" at migration time. That equivalence is what makes every figure identical, and the migration test asserts it per owner.

A category renamed before the migration (for example "Investasi" renamed to "Invest") gets no key. That is already today's behaviour: the rename already changed its figures.

Rejected:
- Case-insensitive or trimmed matching: it would change figures for a row named "investasi", which today counts as spending.

### D4. Uniqueness per owner, key and kind

`CREATE UNIQUE INDEX uq_category_user_system_key_kind ON categories (user_id, system_key, kind) WHERE system_key IS NOT NULL`.

The existing `UNIQUE (user_id, name, kind)` guarantees the backfill cannot produce duplicates, because each key comes from exactly one name. The migration still checks, and fails with the offending owner count, instead of guessing.

### D5. One module owns system-category identity

`app/services/system_categories.py` holds:
- the key constants;
- `NON_REPORTABLE_KEYS = ("internal_movement", "investment")`;
- SQL fragments such as `is_system_sql("c", "investment")` and `non_reportable_sql("c")`;
- `ensure_system_category(cur, user_id, key, kind)`.

Every caller in the proposal's list uses these, replacing `NON_REPORTABLE_CATEGORY_NAMES`, `MOVEMENT_CATEGORY(_NAME)` and `ADJUSTMENT_CATEGORY_NAME`. `report_figures.reportable_category_sql` keeps its signature and result columns.

`ensure_system_category` finds the owner's category by key and kind. When none exists it inserts one with the seed name. When a user category of that kind already holds the seed name (created by the owner after the migration), it adopts that row by setting its key. This matches today's `ensure_adjustment_category`, which already updates a same-name row on conflict.

Rejected:
- Inserting with a suffixed name ("Internal Movement 2"): it creates visible clutter for a case the owner did not ask for.

### D6. A guard keeps names out of logic

A backend test scans `backend/app` for the strings `'Investasi'`, `"Investasi"`, `'Internal Movement'`, `"Internal Movement"` and `Penyesuaian Saldo`. It allows them only in:
- the seed list;
- `system_categories.py` seed names;
- notification parser input patterns and their `category_hint` values;
- comments and docstrings.

A matching Vitest guard scans `frontend/src` (tests and `/komponen` excluded). Both fail with the file and line.

### D7. Parser hints resolve by key

The parser emits `category_hint="Internal Movement"` (five places) and `"Investasi"` (one place). `hinted_categories` and the movement-role code in `notification_application`/`notification_resolution` resolve these two hints to the keyed category of the matching kind, whatever it is named. Other hints (myBCA's English taxonomy) keep resolving by name through the alias table.

### D8. The API exposes the key and the web reads it

The API changes are additive:
- `GET /api/categories` returns `system_key` (string or `null`);
- transaction rows from `GET /api/transactions` and the drawer endpoint return `category_system_key`;
- the review and waiting rows keep their existing boolean `movement_category`, now computed from the key.

The web replaces its 6 name checks:
- `activity.tsx`: the movement kind;
- `ledgerMovements.ts`: movement candidates, ×2;
- `provenance.ts`: non-reportable categories;
- `BudgetPanel.tsx`: categories that can hold a budget;
- `MovementReviewCards.tsx`: choices that exclude the movement category.

Rejected:
- Keeping web name checks alongside the key: two sources of truth, and the web would break as soon as an English account exists.

### D9. Public `category_name` stays exact

`POST /api/transactions` keeps matching `category_name` exactly against the owner's names (`idempotent-public-transactions` forbids synonyms). After a rename, a client must send the new name or the id, as today.

Rejected:
- Accepting "Internal Movement" and "Investasi" as permanent aliases: it contradicts the no-synonym rule, and no known client posts them.

### D10. Verification proves identity, not just passing tests

Four kinds of evidence:
- **Migration test:** a copy of a synthetic database at V51 (the parity seed plus edge rows: an income "Investasi", an archived "Internal Movement", a row named "investasi" in lower case, a pre-renamed "Invest") is migrated to V52. Every owner's key assignment must equal the name predicate, and the lower-case and renamed rows must have no key.
- **Figure parity:** `scripts/verify/markas/parity.mjs`, plus API snapshots of `/dashboard/cycle-summary`, `/dashboard/overview`, `/dashboard/analytics`, `/reports` and the ledger type counts. Taken before (V51 code) and after (V52 code) on the same seed within the same minutes, with zero differences.
- **Rename test:** after renaming "Investasi" to "Investments" and both "Internal Movement" categories to "Transfer", the same snapshots are unchanged.
- **Backend suite:** the full suite on freshly migrated disposable PostgreSQL (V1 to V52).

## Data, API and failure behaviour

- The migration fails, and changes nothing, if the uniqueness check finds a duplicate. Under the current constraint that cannot happen.
- A request in flight during the deploy reads either the old code (names) or the new code (keys). Both give the same figures while no rename has happened.
- `system_key` is read-only through the API. `POST` and `PATCH /api/categories` ignore it, and a test proves that a client cannot set it.

## Migration and rollback

- Forward: Flyway V52 during the normal release. It is additive, and the backfill is one `UPDATE` per key.
- Rollback: redeploy the previous release. Old code ignores the column. Before rolling back, rename any renamed system category back to its seed name, or its figures change under the old code. The release note states this.
- No down migration. The column can stay. Dropping it would be a later, separate migration.

## Risks / Trade-offs

- [A category the owner creates later with a system name is ordinary] → Documented, and visible only to a user who does it on purpose. The seeds and "ensure" adopt such a row only when no keyed row of that kind exists (D5).
- [Missed name check] → The guard (D6) and the rename test (D10) catch a missed site in logic or a figure.
- [Large mechanical diff in money code] → No rule changes, only the predicate source. The per-module tests and the parity snapshots prove it.

## Open Questions

None. The owner's default 4 decided the approach.
