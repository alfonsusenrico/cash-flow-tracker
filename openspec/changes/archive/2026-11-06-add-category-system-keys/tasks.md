## 1. Approval and baseline

- [x] 1.1 Record the owner's approval with its date in `proposal.md` (replacing "Plan approval pending") and in `PROJECT_STATE.md`. Create the implementation branch `feat/category-system-keys` from `main` and move this change folder to it. Evidence: both files name the date, and `openspec validate add-category-system-keys --strict` passes on the new branch.
- [x] 1.2 Capture the "before" figures on the base revision: start a disposable PostgreSQL, migrate it to V51, seed `scripts/verify/markas/seed_parity.py` plus the edge rows of design D10, and store API snapshots of `/dashboard/cycle-summary`, `/dashboard/overview` (cycle and month), `/dashboard/analytics`, `/reports` (latest closed period), `/pulse` and the ledger type counts, plus `parity.mjs` output. Evidence: `research/evidence/add-category-system-keys/before/` with the snapshots and the base commit hash.

Expected Result: the exact figures the migration must preserve are recorded on a known revision.

## 2. Schema and identity module

- [x] 2.1 Write `db/migrations/V52__category_system_keys.sql` (D1, D3, D4): the column, the CHECK, the backfill, the duplicate check that fails with an owner count, and the partial unique index. Evidence: a pytest migration test on a copy of the synthetic V51 database asserts, per owner, that the key equals the old name predicate for every row, that the lower-case "investasi" and the pre-renamed "Invest" carry no key, that the index exists, and that a planted duplicate makes the migration fail without changes.
- [x] 2.2 Add `app/services/system_categories.py` (D5) with the key constants, `NON_REPORTABLE_KEYS`, the SQL fragments and `ensure_system_category`. Evidence: unit tests cover the find, create and adopt paths for each key and kind, including the adopt case of a same-name user category created after the migration.
- [x] 2.3 Seed system categories with their keys (`db/init_db.py`) and route `movement_linkage` and `accounts.ensure_adjustment_category` through `ensure_system_category`. Evidence: the registration test asserts the keys of "Investasi" and both "Internal Movement" categories, and that every other seeded category has none. The reconcile test asserts that a renamed adjustment category is reused.

Expected Result: every owner's system categories carry a key, and one module answers "is this a system category".

## 3. Move every name check to the key

- [x] 3.1 Replace the name checks in `routers/pulse.py`, `routers/dashboard.py`, `routers/transactions.py` (type filters and the investment-trade default), `routers/review.py`, `services/report_figures.py` and `services/observed_insights.py`. Evidence: the existing pulse, overview, analytics, ledger filter and report tests pass unchanged, and a new rename test (2 of the "Renaming a System Category Keeps Figures" scenarios) passes for each endpoint.
- [x] 3.2 Replace `NON_REPORTABLE_CATEGORY_NAMES` in `reimbursements.py` and its importers (habits, habit list, capture resolution, capture suggestions, history suggestions, purpose questions), and remove the unused `REIMBURSEMENT_CATEGORY_NAME`. Evidence: the reimbursement, habit, capture and history-suggestion suites pass. A test proves that a renamed "Investasi" row is still never proposed as a spending choice.
- [x] 3.3 Replace the movement-name checks in `movement_pairing`, `waiting_rows`, `topup_learning`, `merchant_memory`, `balance_check`, `notification_application` and `notification_resolution`, and resolve the parser hints "Internal Movement" and "Investasi" by key (D7). Evidence: the pairing, review queue, merchant memory, balance check and notification processing suites pass. New tests file a Jago "Pindah saldo masuk" under a renamed movement category, and a Stockbit buy under a renamed investment category.
- [x] 3.4 Add the backend guard test (D6). Evidence: it passes on the branch, and it fails, naming the file and line, when a planted `c.name = 'Investasi'` is added to a router (checked once and removed).

Expected Result: no backend logic reads a system category's name, and renames change nothing but the name.

## 4. API and web

- [x] 4.1 Return `system_key` from `GET /api/categories` and `category_system_key` from transaction rows. Keep both read-only (D8). Evidence: API tests for both fields and for the "Key cannot be set by a client" scenario. `npm run generate-types` (or the hand-kept types in `lib/api.ts`) is updated.
- [x] 4.2 Switch the web's 6 name checks to the key and add the frontend guard. Evidence: `activity.test.ts`, `ledgerMovements.test.ts`, `provenance.test.ts` and the budget and movement review tests pass with fixtures whose system categories are renamed. The guard fails on a planted `category_name === "Internal Movement"`.

Expected Result: the web shows the same rows, kinds and choices whatever the system categories are named.

## 5. Verification

- [x] 5.1 Figure parity (D10): repeat task 1.2's snapshots and `parity.mjs` on this branch, with the same seed, within the same minutes. Then rename "Investasi" and both movement categories, and snapshot again. Evidence: `verification.md` lists zero differing values for both comparisons, with both commit hashes.
- [x] 5.2 Repository checks on the exact revision: the full backend suite on freshly migrated disposable PostgreSQL (V1 to V52), with skipped tests listed and explained; `npm test`, `npm run type-check`, `npm run lint` and `npm run build`; `git diff --check`; `openspec validate --all --strict`. Evidence: counts and the commit hash in `verification.md`.
- [x] 5.3 Present the verification to the owner: what changed, the parity result, the rollback note (rename system categories back before rolling back), and what was not verified. Evidence: the owner's acceptance, quoted with its date in `verification.md`.
- [ ] 5.4 After the owner's acceptance: run `openspec archive add-category-system-keys`, then `openspec validate --all --strict`, commit with a subject-only Conventional Commit, push `feat/category-system-keys`, and when the owner says "open", create the pull request against `main` with `gh pr create` and report its URL (the agent never approves or merges it). Record the post-merge production check (Flyway 52 applied, key counts per owner non-zero) in `PROJECT_STATE.md`. Evidence: the archive path, the passing validation, the pushed revision and the pull request URL.

Expected Result: the change is verified on its exact revision, accepted by the owner, archived and offered for merge before `add-language-system` starts its seed work.
