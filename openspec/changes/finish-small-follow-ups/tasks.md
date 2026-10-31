# Tasks

## 1. Top categories for the phone (D1–D4)

- [x] 1.1 Add `top_categories` (limit 3, net spending > 0, outflow descending then name, uncategorised left out) to `services/report_figures.py` and return it from `GET /api/dashboard/cash-flow-summary`, built from `category_breakdown` with the summary's window and offsets. Evidence: `test_cash_flow_top_categories.py` covers the sum equal to `outflow`, reimbursement apportionment, a fully reimbursed category left out, movements, a movement fee, investment trades and the "Investasi" and "Internal Movement" categories excluded as in `outflow`, `today` vs `month` on a fixed clock (including a previous-month and a future row), an empty list, another owner's same-named category, the Bearer key (equal to the session view; unknown key 401), the limit of 3 and tie order by name; every summary in the file also asserts at most 3 entries, positive amounts, sorted order and a sum at most `outflow`. A mutation (gross amounts, zero rows kept) fails 7 of the DB tests.
- [x] 1.2 Write the `financial-reporting-consistency` ADDED requirement with the contract and its scenarios. Evidence: `openspec validate finish-small-follow-ups --strict`.

Expected Result: the phone can show the three largest spending categories, consistent with the summary's own outflow, and older clients see no change.

## 2. Habit basis on Tinjau (D5)

- [x] 2.1 Show `habitBasisText(basis)` in the `habit` suggestion button and type `kind: "habit"` and `basis` in `lib/review.ts`. Evidence: `review/page.test.tsx` "names the habit guess with its basis" (accessible name contains the label and "3 dari 4 jawaban sebelumnya", only the habit chip shows a basis, axe passes, Enter answers with `suggestion_id`); it fails without the component change.
- [x] 2.2 Pin the API: `/api/review/queue` returns the stored guess with its basis. Evidence: assertion added to `test_habit_learning.py::test_two_answers_only_put_the_guess_first`.
- [x] 2.3 Browser check in headless Brave at 390 and 1280 px, light and dark. Evidence: verification.md (accessible name, contrast, keyboard focus, overflow, axe, console).

Expected Result: the owner sees, and hears, why the app guesses a category.

## 3. Plan auto-link switch (D6)

- [x] 3.1 Pass `PLAN_AUTO_LINK_ENABLED` (default `true`) in `docker-compose.yml`. Evidence: `docker compose -f docker-compose.yml config --quiet` exit 0; rendered config shows `"true"` by default and `"false"` when the variable is set to false. `.env.example` already documents it.

Expected Result: a Compose deployment can turn plan auto-linking off.

## 4. Unused web code (D7)

- [x] 4.1 Delete `Badge`, `MetricTile`, `ProgressBar` and `Card`, the `.workbench-page`, `.card-green` and `.kakeibo-chip-*` rules, and the 12 tokens orphaned by them; update `tokens.test.ts`. Evidence: `rg` finds no import or class use; the token scan before and after lists only the six tokens that were already unused; `npm test`, lint, type-check and build pass.

Expected Result: no unused component, rule or token from this list remains, and no used one was removed.

## 5. Stale spec text (D8, D9)

- [x] 5.1 `idempotent-public-transactions`: types `expense|income`, HTTP 200 for new and replayed rows, 400 for a non-trade target account. Evidence: new `test_public_transaction_contract.py`; existing `test_transactions.py` validation and replay tests.
- [x] 5.2 `default-pocket`: scenarios describe the notification flow and name callers. Evidence: new `test_ingest.py::test_a_notification_without_a_pocket_lands_in_the_default_pocket`; existing `test_notification_sender.py` and `test_entity_name_resolution.py` default-pocket cases.
- [x] 5.3 `transaction-provenance` and `observed-insights`: split the three long requirements (MODIFIED keeps header and every scenario; each removed behaviour becomes an ADDED requirement with a scenario). Evidence: `openspec validate --all --strict` exit 0; requirement and scenario names before and after the archive in verification.md.
- [x] 5.4 `tactile-pulse-ui` items 2–4 checked against `tokens.css`, the built CSS and component classes; items 2 and 3 corrected, item 4 unchanged, scenario names kept. Evidence: design.md D8 and the open decisions.

Expected Result: main specs describe the code, and strict validation is clean.

## 6. Verification and delivery

- [x] 6.1 Backend on a fresh PostgreSQL 16 database migrated V1→V39 with flyway/flyway:10, plus the CI-style run without a database, against the origin/main baseline. Evidence: counts in verification.md.
- [x] 6.2 Frontend `npm ci`, test, lint, type-check, build. Evidence: counts in verification.md.
- [x] 6.3 `openspec validate --all --strict`, `git diff --check` and a final diff review. Evidence: verification.md.
- [x] 6.4 Commit, archive on the same branch and push `chore/small-follow-ups` without opening a pull request. Evidence: `git log` and the pushed branch.

Expected Result: verified, archived and pushed, ready for owner review.
