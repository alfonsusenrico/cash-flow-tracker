## Context

Production runs the Compose `migrate` service (Flyway 10, `db/migrations`) before the API:
`scripts/deploy_remote_release.sh` lines 116–123 run the V14 preflight and
`docker compose … run --rm migrate`, and only then (lines 126–128) `up --detach --wait`; in
`docker-compose.yml` the `api` service `depends_on` (line 88) `migrate` with `condition:
service_completed_successfully` (lines 93–94); `.github/workflows/deploy.yml` line 129 calls that script. The API nevertheless ran `init_db_schema()` at startup:
an inline block of `CREATE TABLE IF NOT EXISTS`/`ALTER` statements, a `DELETE` of
"Penyesuaian Nilai Investasi%" rows, an `UPDATE` archiving paid debts, and eleven SQL files that are
byte-identical copies of V20, V22–V31. Errors were printed and ignored.

Evidence gathered before any removal (fresh PostgreSQL 16):

- Schema dump after Flyway V1→V31 versus the same database after `init_db_schema()`: the only differences
  are `accounts.default_funding_account_id` (+ FK) and the unique index `uq_tx_user_idempotency`. Both are
  used: the column by accounts, movements and notification context; the index backs idempotent writes.
- After this change, a fresh V1→V32 dump equals the old V31 + startup dump except for the dropped
  `merchant_category_rules` table, its index and constraints.
- Import evidence for removed modules: no module, test, script or evaluation imports
  `app.services.ledger*`, `app.services.metrics`, `app.services.projection`, `app.services.categories`,
  `app.models*`; `app.services.category_rules` is imported only by `tests/test_category_rules.py`; no dynamic
  import exists in `backend/app` or `scripts`.

## Goals / Non-Goals

**Goals:** one schema owner; no data change without a versioned migration; remove dead code; close the
phase 3 and phase 2 follow-ups; OpenSpec hygiene. **Non-goals:** see proposal.

## Decisions

### D1. Flyway is the only schema owner; V32 adopts what only the startup block created

V32 adds `default_funding_account_id` and `uq_tx_user_idempotency` with `IF NOT EXISTS` (no-op on
production, where the startup block created them) and moves the paid-debt archive into a one-time data
step. The "Penyesuaian Nilai Investasi" delete is not carried over: V9 already performs it, no code writes
such notes any more, and repeating it on every boot could delete an owner's own row with that note.

*Rejected:* keeping the startup block "for safety" — it hides migration gaps (as the two missing elements
show) and runs data deletes on every start. *Rejected:* re-creating the index with `CONCURRENTLY` — Flyway
runs each migration in a transaction and the table is small.

### D2. Retire `merchant_category_rules` only when empty

Nothing ever wrote the table (phase 3 audit). V32 counts its rows: zero drops it; any row raises
`V32: merchant_category_rules holds N row(s); …`, which fails the migration and therefore the release
before the API is replaced. The context and deterministic interpretation stop reading it; the benchmark
fixture's rule becomes an owner memory entry, its successor. The prompt line naming "rules" is updated and
the prompt version bumped to `notification-interpretation-10` so stored interpretations remain attributable.

*Rejected:* migrating rows into `merchant_memory` automatically — the substring pattern does not map to an
exact match key and would teach wrong memory.

### D3. Mirror tests become repeatability tests

Tests that asserted mirror = versioned file now run the versioned file twice on populated data; the
data-preservation assertions are unchanged. `test_schema_ownership.py` asserts no SQL file remains under
`backend/app/db`, V32's adoption, debt archive, idempotency uniqueness, repeatability and the guarded drop
(a populated table raises and keeps its row). The scratch-schema pattern of earlier migration tests is
reused, so nothing outside a rolled-back transaction changes.

### D4. Seeded movement categories and icons

`seed_user_defaults` inserts both "Internal Movement" categories with `is_excluded_from_budget = true` and a
NULL pillar, matching what `ensure_internal_movement_categories` enforces on first movement. Missing icons are
added to the web icon set (`coffee`, `users`, plus `receipt` used by the fee category and
`arrows-right-left` seeded by V13); stored data is not rewritten.

### D5. OpenSpec hygiene

Each open change was audited against the code (backend and companion `main`); a change is archived only when
every requirement is implemented and its delta does not contradict current behaviour. Ten were archived in
creation order; before archiving, the three requirements that `add-amount-based-investment-topups` and the
still-open `enforce-financial-integrity-and-automation` both modify were merged into the latter so the
top-up rules are not lost when it is archived later. `big-refactor-v2`'s two REMOVED blocks named
requirements that never existed in a main spec; they were restated as ADDED requirements with the same
assertions, and its button label was aligned with the shipped "Kirim Notifikasi ke Tracker".

The strict split keeps each long requirement's header and every scenario and cuts its description to one
behaviour; each removed behaviour becomes its own ADDED requirement with a new scenario. Synthetic scenario
texts for notification filtering were checked against `collect_facts`. The two open changes that modify a
split requirement (`redesign-account-hierarchy-and-actions`, `add-ai-notification-processing`) got the same
shortened description so a later archive does not restore the long text.

## Data, API and failure behaviour

- API contracts are unchanged. A database not yet at V32 lacks nothing the API needs only if it was
  previously started by the old API; a fresh database must be migrated (as the release and tests do).
- V32 failure (populated rules table, duplicate idempotency keys) stops the release before the API changes.

## Migration and rollback

Forward: V32 is additive except the guarded drop. Rollback by redeploying the previous revision: its
startup block re-creates the empty `merchant_category_rules` table (V20 mirror) and Flyway ignores the
applied future V32 (`ignoreMigrationPatterns` default `*:future`). The archived-debt flag needs no rollback:
it is the invariant the code already maintains.

## Open decisions for owner

1. **Changes left open** (implementation absent or delta contradicts current code):
   - `enforce-financial-integrity-and-automation`: the "Name-Based Entity Resolution" requirement says
     ambiguous or unmatched names are rejected, but `routers/transactions.py` `_match_account_by_name` takes
     the first substring match and auto-creates a pocket under a "Jago" root. Fix the code or the spec.
   - `modern-pop-ui-and-animations`: its pop colour tokens were replaced by the current palette.
   - `redesign-account-hierarchy-and-actions`: its transfer-action clauses were removed by the unified forms.
   - `add-ai-notification-processing`, `notification-pipeline-reliability`,
     `confirm-jago-outbound-source-pocket`: deltas describe superseded behaviour (single 30-second pairing
     window, equal amounts, primary-account fallback, `type = 'transfer'`, forward-everything listener).
   Each needs its delta rewritten to current behaviour (or abandoned) before archiving.
2. Drop the retired bot tables (`bot_users`, `bot_chat_history`, `bot_pending_actions`) and `bot_storage`
   volume (phase 1 follow-up).
3. Drop the provenance column defaults `manual`/`reviewed` kept for rollback (phase 2 follow-up).
4. Delete the unreferenced legacy `backend/migrations/0035–0038` SQL files.
5. The category list API presents a NULL expense pillar as `need` (including "Internal Movement"); the stored
   value is NULL. Decide whether the API should return NULL.
