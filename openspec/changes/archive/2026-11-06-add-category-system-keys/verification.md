# Verification: add-category-system-keys

Date: 2026-11-06 (Asia/Jakarta). Branch `feat/category-system-keys` from `origin/main` `e8ec62d`; implementation
commit `7985ff4`. After acceptance, `origin/main` `f1ee604` (Markas phase 3) was merged in (`71010a9`, "chore: merge
main into category system keys"). Final verification ran on the merged tree `71010a9` ("V52 code") against `main`
`f1ee604` ("V51 code"); the first pass (on `7985ff4` against `e8ec62d`) gave the same results and is summarised in
"First pass" below.

Owner approval (2026-11-06): "ok to all" for the three-change language plan with every recommended default; this
change merges first, on its own branch from `main`. Recorded in `proposal.md` › Plan approval and in the local
`PROJECT_STATE.md`.

## Environment

- Local only, synthetic data only. A new disposable `postgres:16` container on `127.0.0.1:55471`, migrated by
  `flyway/flyway:10` with the CI flags; removed after the run. No production system was contacted.
- Backend: the project-local `backend/.venv` (`uv venv -p 3.12`, `requirements.txt` and `requirements-dev.txt`).
- Parity stacks: "before" is `f1ee604` (first pass: `e8ec62d`) exported with `git archive` into the session scratchpad (no extra worktree),
  "after" is this worktree. Each ran its own backend (uvicorn, loopback, AI calls and background jobs off), its own
  production `next build --webpack` served by `next start` behind a loopback `/api` proxy
  (`research/evidence/add-category-system-keys/tools/proxy.mjs`), and its own database (below).
- Browser for `scripts/verify/markas/parity.mjs`: headless Brave over CDP.

## What changed

- `db/migrations/V52__category_system_keys.sql`: `categories.system_key VARCHAR(32) NULL`, CHECK
  (`investment`, `internal_movement`, `balance_adjustment`), backfill by exact, case-sensitive name over every kind
  and archived rows, a duplicate check that raises with the owner count, and the partial unique index
  `uq_category_user_system_key_kind (user_id, system_key, kind) WHERE system_key IS NOT NULL`.
- `backend/app/services/system_categories.py` owns system-category identity: the keys, `NON_REPORTABLE_KEYS`, the
  seed names, the parser-hint keys, the SQL fragments `is_system_sql`, `non_reportable_sql`, `reportable_key_sql`,
  and `ensure_system_category`.
- Every backend name check moved to the key (inventory below). Seeds carry the key; `ensure_internal_movement_categories`
  and `ensure_adjustment_category` call `ensure_system_category`.
- API (additive): `GET /api/categories` returns `system_key`; `GET /api/transactions` rows and
  `GET /api/transactions/{id}` return `category_system_key`. `POST`/`PATCH /api/categories` ignore `system_key`.
- Web: `frontend/src/lib/systemCategories.ts` and the 6 checks read the key; hand-kept types gained the fields.
- Removed: `REIMBURSEMENT_CATEGORY_NAME` (unused), `NON_REPORTABLE_CATEGORY_NAMES`, `MOVEMENT_CATEGORY_NAME`,
  `MOVEMENT_CATEGORY`, `ADJUSTMENT_CATEGORY_NAME`, `_NON_REPORTABLE_SQL` (backend) and the web's
  `NON_REPORTABLE_CATEGORY_NAMES`.
- Guards: `backend/tests/test_system_category_guard.py` and `frontend/src/systemCategoryGuard.test.ts`.
- `openspec/config.yaml`: the pull-request rule now reads that, after acceptance, the agent creates the pull request
  with `gh pr create` when the owner says "open", and never approves or merges it.

### Reference inventory (measured on `e8ec62d` with `rg`, literal and constant-based)

73 lines in 26 backend modules named a system category. 64 lines in 23 modules were moved to the key:

| Group | Modules (lines) |
| --- | --- |
| Routers (19) | `pulse` 8, `transactions` 6 (type filters, investment-trade default), `dashboard` 2, `accounts` 2, `review` 1 |
| Reports (9) | `reimbursements` 5, `report_figures` 3, `observed_insights` 1 |
| `NON_REPORTABLE_CATEGORY_NAMES` importers (14) | `history_suggestions` 3, `capture_resolution` 3, `purpose_questions` 2, `habits` 2, `habit_list` 2, `capture_suggestions` 2 |
| Movement (16) | `movement_linkage` 4, `movement_pairing` 4, `merchant_memory` 4, `balance_check` 2, `topup_learning` 1, `waiting_rows` 1 |
| Notifications (3) | `notification_application` 2, `notification_resolution` 1 (plus `notification_context`, which now loads the key) |
| Seeds (3) | `db/init_db` 3 |

Kept by design (D6, allowed by the guard): the 6 parser `category_hint` values in `notification_parser.py`, now
resolved by key (D7); copy that equals a seed name but identifies nothing: `cycle_summary.py`'s net-worth label
"Investasi" and `movements.py`'s default note "Internal Movement" (2). After the change `rg` finds the names only in
those places and in `system_categories.SEED_NAMES`.

Web: 6 checks in 5 files (`activity.tsx`, `ledgerMovements.ts` ×2, `provenance.ts`, `BudgetPanel.tsx`,
`MovementReviewCards.tsx`). Run against `e8ec62d`'s `frontend/src`, the web guard reports exactly these 6 lines.
Main's phase 3 (`f1ee604`) changed no backend or migration file and added no name check; the merged
`activity.tsx` keeps the key check and the web guard passes on the merged tree.

## Task evidence

### 2.1 Migration V52

`backend/tests/test_category_system_keys.py`:

- `test_v52_backfill_reproduces_the_name_predicate_for_every_owner_and_row`: a throwaway schema migrated V1 to V51,
  three synthetic owners and 13 categories (the parity seed's shape plus D10's edges: income "Investasi", archived
  income "Internal Movement", lower-case "investasi", lower-case "internal movement", pre-renamed "Invest", income
  "Penyesuaian Saldo"). After V52, for every row the key equals the old name predicate, and the old SQL predicates
  (reportable, movement, investment) equal the new key predicates row by row. The lower-case and renamed rows carry
  no key; the index exists with its `WHERE (system_key IS NOT NULL)`; the CHECK rejects `salary`; a second
  `investment` expense for one owner is a unique violation. Passed.
- `test_v52_fails_without_changes_when_an_owner_has_a_duplicate`: with `UNIQUE (user_id, name, kind)` dropped in
  the throwaway schema, two "Investasi" expenses for one owner make V52 raise "1 owner(s) have more than one
  category per key and kind", and afterwards the column does not exist. Passed.
- Flyway: `flyway migrate` V1 to V52 on a fresh database: "Successfully applied 52 migrations … now at version
  v52"; `flyway info`: "Schema version: 52". On the parity copy (V51 plus seed), V52 applied as one migration;
  keys per owner afterwards: `dimas_contoh` 5 keyed rows (both "Internal Movement", both "Investasi",
  "Penyesuaian Saldo"), `edge_contoh` 3 (including the archived income "Internal Movement"); lower-case
  "investasi" and "Invest" carry none; 0 of 28 categories differ from the name predicate.

### 2.2 Owning module

`test_ensure_creates_finds_and_adopts` (movement and adjustment × expense and income, 4 cases): create with the
seed name and key, find by key after a rename (and repair the excluded flag), adopt a same-name user category after
the keyed one was deleted. `test_ensure_keeps_movement_archived_and_restores_adjustment` keeps today's archive
behaviour. `test_ensure_refuses_the_seeded_investment_key_and_unknown_input`: the investment category is only seeded
and found, so `ensure` refuses it, an unknown key and an unknown kind before any statement. All passed.

### 2.3 Seeds and ensure helpers

`test_new_owner_seeds_keys_only_on_the_system_categories`: "Investasi" carries `investment`, both "Internal
Movement" `internal_movement`, the other 11 seeded categories none. `test_reconcile_files_under_the_renamed_adjustment_category`:
an upward reconcile creates "Penyesuaian Saldo" (income, key `balance_adjustment`, excluded). After renaming it
"Koreksi saldo", the next upward reconcile is filed under "Koreksi saldo" (2 rows, no new income category), and a
downward one creates the expense "Penyesuaian Saldo" (1 row), the kind that did not exist yet. Passed.

### 3.1 to 3.3 Name checks moved, renames keep figures

- `test_renaming_system_categories_changes_only_their_names`: salary, a meal, an investment buy of Rp 1.000.000, a
  linked movement of Rp 500.000 and an unlinked movement income of Rp 300.000. Snapshots of `/pulse`, `/insights`,
  overview (cycle and month), cycle summary, analytics, cash-flow summary, the current monthly report and its
  periods, the review queue and summary, the ledger with type counts and daily totals and each of the 5 type tabs
  are identical before and after renaming "Investasi" to "Investments" and both movement categories to
  "Transfer" (only read-time stamps differ). The unlinked leg is still a pairing candidate. A new expense category
  named "Investasi" afterwards is ordinary: its Rp 100.000 raises total outflow from Rp 200.000 to Rp 300.000, and
  "Investments" and "Transfer" stay out of the breakdown. Passed.
- `test_renamed_investment_is_never_offered_as_a_spending_choice` (3.2): after renames, capture resolution and
  history suggestions never offer the investment or movement category, and both are refused as an offset
  category (`invalid_offset_category`). Passed.
- 3.3: `test_jago_self_transfer_files_under_the_renamed_movement_category` (a Jago "Pindah saldo masuk" is filed in
  the renamed "Transfer" income category and counts as neither spending nor income) and
  `test_stockbit_buy_books_its_legs_in_the_renamed_movement_categories`. `test_system_hints_resolve_by_key_and_other_hints_by_name`
  shows the parser hint "Investasi" resolving to the renamed "Investments" (not a later ordinary "Investasi"),
  "Internal Movement" to "Transfer", and myBCA's "shopping" still by name. `test_model_input_keeps_system_keys_out`.
  All passed, together with the pairing, review queue, merchant memory, balance check and notification suites.

### 3.4 Backend guard

`test_backend_logic_reads_system_keys_not_names` passes on the branch. Planted once in a router and removed:
appending `PLANTED_CHECK = "SELECT 1 FROM categories c WHERE c.name = 'Investasi'"` to `routers/pulse.py` made it
fail with `routers/pulse.py:363: Investasi`; restored, it passes. Two permanent negative tests plant name checks in
a temporary tree (file and line reported; docstrings, comments and `category_hint=` values allowed).

### 4.1 API

`test_categories_and_transactions_return_the_read_only_key`: `system_key` on categories (`investment`,
`internal_movement`, `null` for "Makanan & Minuman"), `category_system_key` on ledger and drawer rows, and
`system_key: "investment"` sent on create and on update of "Hobi" leaves it `null`, with its Rp 70.000 counted as
spending. `test_kind_change_into_an_existing_system_category_is_refused`: changing the expense movement category to
income while the income one exists answers 409 instead of a unique-index error. The web has no generated API types
(`npm run generate-types` targets a file the project does not keep); the hand-kept interfaces gained the fields.

### 4.2 Web

`activity.test.ts`, `ledgerMovements.test.ts` and `provenance.test.ts` use fixtures whose system categories are
renamed ("Transfer", "Investments") and carry keys; a same-name ordinary "Internal Movement" or "Investasi" is
treated as ordinary. The movement review test (`app/review/page.test.tsx`) names the movement category "Transfer"
and asserts it is not an expense choice. New `components/report/BudgetPanel.test.tsx`: the budget editor offers
"Makanan & Minuman" and a later ordinary "Investasi", not "Investments" or "Transfer". The web guard passes; planted
once in `components/beranda/activity.tsx` (`row.category_name === "Internal Movement"`) it failed with
`components/beranda/activity.tsx:221: …`; restored, it passes.

## 1.2 and 5.1 Figure parity

Seed: `scripts/verify/markas/seed_parity.py` with `cash-flow-tracker-prototypes/design-options/shared/data.js`
(SHA-256 `c1d9a98f743416cfaf8d40106819cbdfd1781e8e2b33b29103e514437832f089`, read only) into `cft_markas_before`
at V51 (owner `dimas_contoh`, 188 transactions), then the synthetic D10 edge rows of
`research/evidence/add-category-system-keys/edge_rows.sql` (the parity seed has no "Investasi": an expense
"Investasi" with a Rp 1.000.000 buy, an income "Investasi" Rp 20.000, lower-case "investasi" Rp 75.000, pre-renamed
"Invest" Rp 50.000, "Penyesuaian Saldo" Rp 10.000, and a second owner with an archived income "Internal
Movement"). `cft_markas_after` = `CREATE DATABASE … TEMPLATE cft_markas_before`, then Flyway V52. The V51 code read
`cft_markas_before`, the V52 code `cft_markas_after`.

API snapshots (`tools/snapshot.py`, 19 responses: `/pulse`, `/insights`, cycle summary, overview cycle and month,
analytics, cash-flow summary, net worth, the latest closed and the current monthly report, report periods, review
summary and queue, the ledger with type counts and daily totals, and each type tab), compared by
`tools/compare.py` (every value; read-time stamps `generated_at`, `as_of` and a running window's "now" end skipped;
the new `category_system_key`/`system_key` fields counted, not compared, where the V51 code lacks them):

| Comparison (2026-11-06, `f1ee604` vs `71010a9`) | Values compared | Differences |
| --- | --- | --- |
| Migration: V51 code on `before` vs V52 code on `after` (10:43–10:44) | 23,793 | **0** (452 additive fields) |
| Rename on V52 code: V51 code unrenamed vs V52 code after renaming "Investasi" → "Investments" and both "Internal Movement" → "Transfer" through `PATCH /api/categories` (10:45) | 23,793 | **0** |
| The old bug, for contrast: the same renames on the V51 code (10:46) | 14,548 | **173** (dashboard 82, reports 50, transactions 30, review 5, insights 4, pulse 2) |

`parity.mjs` (every money figure, count and date in `main`, 18 routes):

| Comparison | Routes identical |
| --- | --- |
| Migration, 10:43–10:44 | **18 of 18** (Beranda 44 figures after phase 3, Tinjau 15, Transaksi 125, Analisis 74, Laporan 105, Rekening 62, Target 34, Pengaturan sections, shell Transaksi 126) |
| V52 code renamed vs V51 code unrenamed, 10:45 | **18 of 18** |
| Both renamed (V51 vs V52 code), 10:46 | 12 of 18: Beranda, Tinjau, Transaksi, Analisis, Laporan and shell Transaksi differ, which is the bug this change fixes |

Under the V51 code the rename turned the Rp 1.000.000 investment buy into spending (total outflow Rp 3.575.490 →
Rp 4.575.490 on analytics, overview and cycle summary) and moved the unlinked movement leg out of "awaiting" in
the waiting rows. Evidence: `research/evidence/add-category-system-keys/` (`before/` with `base-commit.txt`,
`after/`, `renamed/`, `rename-on-v51-code/`, the comparison outputs and the tools).

First pass (`e8ec62d` vs `7985ff4`, 10:24–10:32): the same counts (0, 0 and 173 API differences; 18, 18 and 12 of
18 routes identical; Beranda then had 68 figures). Its first renamed `parity.mjs` run hung in the browser (its CDP
calls have no timeout) and was stopped and rerun unchanged. The evidence folder holds the final pass; its
`before/base-commit.txt` names `f1ee604`.

## 5.2 Repository checks

On the merged tree `71010a9`:

- Backend: a new disposable `postgres:16`, Flyway V1 to V52 ("Successfully applied 52 migrations … v52"), then
  `TEST_DATABASE_URL=… SKIP_TEST_MIGRATIONS=1 REQUIRE_TEST_DATABASE=1 .venv/bin/python -m pytest tests/ -ra`:
  **1935 passed, 0 failed, 0 skipped** in 163 s (1 third-party warning: `passlib` uses the deprecated `crypt`). This
  includes both backend guard tests and the rename test.
- Frontend: `npm run type-check` passed; `npm run lint` passed (0 problems); `npm test` **84 files, 808 tests
  passed** (including the web guard); `npm run build` passed.
- First pass on `7985ff4`: backend 1935 passed, 0 skipped; frontend 84 files, 766 tests passed; build passed.
- `openspec validate --all --strict`: **84 passed, 0 failed** (also after the verification and task updates).
- `git diff --check` and `git diff --cached --check` before each commit: clean.

## Deviations and decisions made during implementation

- Branch: the work is on `feat/category-system-keys` (created by the caller from `origin/main` `e8ec62d`), not
  `fix/category-system-keys`; tasks 1.1 and 5.4 name the actual branch.
- Task order: the "before" figures (1.2) were captured from the base revision exported at verification time, in
  the same minutes as the "after" figures, instead of before the code was written, so both sides read the same
  seed at the same clock.
- Model input: the system keys reach the notification code through the backend-only `_system_category_keys`
  context entry; the category list the model sees is unchanged (no new prompt field, benchmark inputs unchanged).
  `evaluation/notification_cases.py` gained the same backend-only entry for its movement categories.
- Stockbit trades book their legs in the movement categories (`apply_broker_trade`), never in "Investasi"; the
  parser's "Investasi" hint is resolved by key for any deterministic path that reads it, shown by a unit test, and
  the broker buy is shown under renamed movement categories.
- `ensure_system_category` serves the movement and adjustment categories only; the investment category is seeded
  once and only found (as today), so `ensure` refuses its key.
- New 409 on `PATCH /api/categories/{id}` when a kind change would give an owner two system categories of the same
  key and kind (the new unique index would otherwise surface as a server error).
- AGENTS.md was not changed: the bullets to update (the baseline "Never open, approve, or merge a merge request…"
  and §8 "Merge Requests") do not exist in this repository's `AGENTS.md` on `main`; its only merge-request line is
  inside the managed `personal` profile block and concerns client repositories. Only `openspec/config.yaml` carries
  the new rule. Owner decision (2026-11-06, via the coordinator): no AGENTS.md edit is needed; the
  `openspec/config.yaml` rule is enough.

## Not verified

- No production check (by design); the post-merge check (Flyway 52 applied, non-zero key counts per owner) is listed
  in `PROJECT_STATE.md`.
- `scripts/verify/find_duplicate_arrivals.py` (a read-only diagnostic meant to run on older releases too) still
  finds the movement category by name; after a rename it would miss movement memory. Not changed.
- The Android companion was not exercised; it reads `/api/categories` and ignores unknown fields.

## Rollback note

Redeploy the previous release; V52 stays, unused by the old code. Before that, rename any renamed system category
back to its seed name ("Investasi", "Internal Movement", "Penyesuaian Saldo"), or its figures change under the old
code, as the V51-code comparison above shows.

## Merge of main

`git merge origin/main` (`f1ee604`) conflicted in `frontend/src/app/page.test.tsx` only: phase 3 removed two Beranda
activity tests from that place (it rewrote them elsewhere in the file), while this branch had added
`category_system_key` to them. Resolved by taking main's removal and giving every "Internal Movement" fixture in the
file its key. `activity.tsx` merged cleanly with the key check intact. Phase 3's new `activity.test.ts` cases build
ordinary rows from this branch's movement fixture, so they now set `category_system_key: null`.

## Owner acceptance (5.3)

Accepted by the owner on 2026-11-06: "complete all until finished", after reviewing the verification results.
