## Approval

Owner authorized phases 2–8 on 2026-10-28 without per-phase review; coordinator-reviewed.

## Why

Phase 8 of `research/notes/2026-10-28-automation-quality-plan.md` closes the automation work. The audit
(`research/notes/2026-10-28-automation-quality-audit.md`, section E) and phases 1–7 left:

- dead backend modules for an old schema (`services/ledger/*`, `metrics.py`, `projection.py`,
  `categories.py`, `models/public.py`) and a test-only `services/category_rules.py` that hardcodes a name;
- schema DDL in two places: Flyway, and an `init_db_schema()` block plus eleven SQL mirrors run at every
  API start. The startup block also deleted rows by note text on every boot and silently swallowed errors;
- the empty `merchant_category_rules` table, still read into the model context, which merchant memory
  replaced in phase 3;
- small follow-ups: new users' "Internal Movement" categories are not flagged as excluded until the first
  movement, and the seeded `coffee` and `users` icons render as the generic tag;
- sixteen open OpenSpec changes, many already implemented, and eighteen over-long main requirements that
  make `openspec validate --all --strict` fail;
- stale facts in `AGENTS.md` (an `android/` folder, the Telegram bot, the mirror convention).

## Expected Outcome

- The API starts on a database migrated by Flyway alone and creates no schema; a fresh V1→V32 database has
  exactly the schema the old startup block produced, minus the retired table.
- A deployment cannot lose owner data: V32 drops `merchant_category_rules` only when it is empty and
  otherwise stops the release with a clear error.
- A new user's movement categories never count as spending from the first day, and every seeded category
  shows its own icon.
- Implemented OpenSpec changes are archived, the rest are listed with the reason they stay open, and the
  strict validation of the main specs passes.

## What Changes

- Backend: delete the dead modules and their only tests; drop the startup schema call from `main.py`, the
  `init_db_schema()` function and the eleven `backend/app/db/*.sql` mirrors; keep `seed_user_defaults`.
  `notification_context` no longer reads merchant category rules and the deterministic interpretation no
  longer matches them; the prompt no longer mentions rules (`notification-interpretation-10`).
- Database: `V32__adopt_startup_schema_and_retire_merchant_rules.sql` adds the two schema elements only the
  startup block created (`accounts.default_funding_account_id`, unique `uq_tx_user_idempotency`), archives
  paid-off debts once, and drops `merchant_category_rules` when empty or raises.
- Registration seeds both "Internal Movement" categories with `is_excluded_from_budget = true` and no pillar.
- Web: `coffee`, `users`, `receipt` and `arrows-right-left` icon paths; the icon picker offers "Jajan" and
  "Patungan".
- Tests: mirror-equality tests become repeatability tests of the versioned migrations; new
  `test_schema_ownership.py`; `Icon.test.tsx`.
- OpenSpec: ten implemented changes archived (separate commit); five new main specs get a real purpose;
  this change splits the eighteen over-long requirements (spec-only, same meaning).
- Docs: `AGENTS.md` facts, the deployment diagram's migration range, fixture script prerequisites.

## Non-goals

- Dropping the retired bot tables, the provenance column defaults, or the legacy `backend/migrations`
  files (owner decisions, listed in design).
- Archiving changes whose delta text conflicts with the current code, or fixing the code gaps they reveal.
- Changing the category list API, which presents a missing expense pillar as `need`.
- Production authority. Verification is local: disposable PostgreSQL migrated V1→V32, full suites, a
  loopback uvicorn smoke run. Deployment happens through the GitHub workflow after the owner merges.

## Capabilities

### New Capabilities

- `schema-migrations`: Flyway owns the schema, the startup-only schema adopted by V32, and the guarded
  retirement of merchant category rules.

### Modified Capabilities

- `custom-category-management`: seeded movement categories are excluded; system category icons render.
- `transaction-provenance`: the backfill scenario no longer refers to a startup mirror.
- Spec-only splits (no behaviour change): `account-reconciliation`, `clean-core-ledger`,
  `history-suggestions`, `kakeibo-decoupling`, `kakeibo-reconciliation`, `merchant-memory`,
  `notification-filtering`, `reimbursement-offsets`, `report-periods-and-drilldown`, `review-queue`.

The project capability list is unchanged. Standards: migration safety with rollback and no silent data
loss, removal proven by import evidence, regression tests for the new seed and migration guard.

## Impact

- **Migrations:** new V32; no applied migration changes. Startup DDL removed.
- **Backend:** `main.py`, `db/init_db.py`, `services/notification_context.py`,
  `services/notification_application.py`, `services/notification_interpretation.py` (prompt version),
  prompt text; deleted modules listed above; tests; `evaluation/notification_cases.py`; fixture scripts.
- **API:** unchanged contracts. Model context loses the always-empty `rules` list.
- **Data:** V32 archives debts with remaining ≤ 0 (the startup block did this on every boot) and drops an
  empty table. The per-boot `DELETE … 'Penyesuaian Nilai Investasi%'` stops; V9 already ran it once.
- **Web:** icon set and picker only.
